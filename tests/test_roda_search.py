"""
Unit tests for roda-search/handler.py.

Uses MagicMock to patch the module-level DynamoDB resource. No real AWS
calls are made.
"""

import importlib
import importlib.util
import os
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _load():
    """Load roda-search/handler.py as a unique module to avoid sys.modules collisions."""
    path = os.path.join(REPO_ROOT, "lambdas", "roda-search", "handler.py")
    spec = importlib.util.spec_from_file_location("_roda_search_handler", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_roda_search_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


roda_search = _load()


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

CLIMATE_ITEM = {
    "slug": "noaa-climate",
    "name": "NOAA Global Climate Data",
    "primaryTag": "climate",
    "tags": ["climate", "weather"],
    "description": "Historical climate measurements from NOAA stations worldwide.",
    "searchText": "climate weather temperature precipitation atmospheric",
    "formats": ["csv", "parquet"],
    "regions": ["us-east-1"],
    "s3Resources": [{"arn": "arn:aws:s3:::noaa-climate-data", "region": "us-east-1"}],
    "s3ResourceCount": 1,
    "registryUrl": "https://registry.opendata.aws/noaa-climate/",
    "license": "Open Data",
    "managedBy": "NOAA",
    "updateFrequency": "Daily",
    "documentation": "",
}

GENOME_ITEM = {
    "slug": "ncbi-genome",
    "name": "NCBI 1000 Genomes",
    "primaryTag": "genomics",
    "tags": ["genomics", "bioinformatics"],
    "description": "Whole genome sequencing data.",
    "searchText": "genomics dna rna sequencing variant genome",
    "formats": ["vcf", "bam"],
    "regions": ["us-east-1"],
    "s3Resources": [{"arn": "arn:aws:s3:::ncbi-genome-data", "region": "us-east-1"}],
    "s3ResourceCount": 1,
    "registryUrl": "",
    "license": "Open Data",
    "managedBy": "NCBI",
    "updateFrequency": "Periodic",
    "documentation": "",
}

ALL_ITEMS = [CLIMATE_ITEM, GENOME_ITEM]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(query_items=None, scan_items=None):
    table = MagicMock()
    table.query.return_value = {"Items": query_items or [], "Count": len(query_items or [])}
    table.scan.return_value = {"Items": scan_items or [], "Count": len(scan_items or [])}
    return table


def _patch_table(table):
    mock_ddb = MagicMock()
    mock_ddb.Table.return_value = table
    return patch.object(roda_search, "dynamodb", mock_ddb)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEmptyQuery:
    def test_no_query_returns_all_via_scan(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["count"] == 2
        assert len(result["datasets"]) == 2
        table.scan.assert_called_once()

    def test_response_has_required_keys(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert "count" in result
        assert "datasets" in result
        assert "query" in result
        assert "appliedTags" in result
        assert "appliedFormat" in result


class TestTagBasedSearch:
    def test_single_tag_uses_gsi_query(self):
        table = _make_table(query_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"tags": ["climate"]}, None)
        table.query.assert_called_once()
        assert result["count"] == 1
        assert result["datasets"][0]["slug"] == "noaa-climate"

    def test_multiple_tags_uses_scan(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            roda_search.handler({"tags": ["climate", "genomics"]}, None)
        table.scan.assert_called_once()

    def test_natural_language_infers_climate_tag(self):
        table = _make_table(query_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"query": "find weather and temperature datasets"}, None)
        assert "climate" in result["appliedTags"]

    def test_natural_language_infers_genomics_tag(self):
        table = _make_table(query_items=[GENOME_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"query": "genome sequencing data"}, None)
        assert "genomics" in result["appliedTags"]


class TestKeywordRanking:
    def test_keyword_match_returns_relevant_items(self):
        table = _make_table(query_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"query": "climate temperature", "tags": ["climate"]}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-climate" in slugs

    def test_no_keyword_match_preserves_tag_filtered_results(self):
        # When query keywords find no matches, keyword_rank falls back to returning
        # all tag-filtered results rather than silently discarding the filter output.
        table = _make_table(query_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"query": "xyz-nonexistent-keyword", "tags": ["climate"]}, None)
        assert result["count"] > 0


class TestFilters:
    def test_format_filter(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"format": "csv"}, None)
        assert all("csv" in d["formats"] for d in result["datasets"])
        assert result["count"] == 1

    def test_quicksight_compatible_filter(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({"quicksight_compatible": True}, None)
        assert all(d["quicksightCompatible"] for d in result["datasets"])
        assert result["count"] == 1

    def test_max_results_enforced(self):
        many_items = [dict(CLIMATE_ITEM, slug=f"ds-{i}") for i in range(20)]
        table = _make_table(scan_items=many_items)
        with _patch_table(table):
            result = roda_search.handler({"max_results": 3}, None)
        assert result["count"] <= 3

    def test_max_results_capped_at_50(self):
        many_items = [dict(CLIMATE_ITEM, slug=f"ds-{i}") for i in range(60)]
        table = _make_table(scan_items=many_items)
        with _patch_table(table):
            result = roda_search.handler({"max_results": 100}, None)
        assert result["count"] <= 50


class TestProjectResult:
    def test_projected_fields_present(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        ds = result["datasets"][0]
        expected = {
            "slug", "name", "description", "tags", "formats",
            "license", "managedBy", "updateFrequency",
            "primaryBucket", "primaryRegion", "s3ResourceCount",
            "registryUrl", "quicksightCompatible", "documentation",
        }
        assert expected.issubset(ds.keys())

    def test_bucket_extracted_from_arn(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["datasets"][0]["primaryBucket"] == "noaa-climate-data"

    def test_description_truncated_at_400(self):
        long_item = dict(CLIMATE_ITEM, description="x" * 500)
        table = _make_table(scan_items=[long_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert len(result["datasets"][0]["description"]) <= 400


class TestDeprecatedDatasets:
    """OD-15: deprecated flag in project_result + exclude_deprecated filter."""

    DEPRECATED_ITEM = {
        **CLIMATE_ITEM,
        "slug": "old-climate",
        "deprecated": True,
    }

    def test_deprecated_flag_in_result(self):
        table = _make_table(scan_items=[self.DEPRECATED_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["datasets"][0]["deprecated"] is True

    def test_non_deprecated_flag_false_in_result(self):
        table = _make_table(scan_items=[CLIMATE_ITEM])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["datasets"][0]["deprecated"] is False

    def test_exclude_deprecated_removes_items(self):
        table = _make_table(scan_items=[CLIMATE_ITEM, self.DEPRECATED_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"exclude_deprecated": True}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "old-climate" not in slugs
        assert "noaa-climate" in slugs

    def test_exclude_deprecated_false_keeps_deprecated_items(self):
        table = _make_table(scan_items=[CLIMATE_ITEM, self.DEPRECATED_ITEM])
        with _patch_table(table):
            result = roda_search.handler({"exclude_deprecated": False}, None)
        slugs = [d["slug"] for d in result["datasets"]]
        assert "old-climate" in slugs


class TestDynamoDBErrors:
    def test_scan_exception_returns_empty(self):
        table = MagicMock()
        table.scan.side_effect = Exception("DynamoDB timeout")
        with _patch_table(table):
            result = roda_search.handler({}, None)
        assert result["count"] == 0
        assert result["datasets"] == []

    def test_gsi_exception_falls_back_to_empty(self):
        table = MagicMock()
        table.query.side_effect = Exception("GSI not found")
        with _patch_table(table):
            result = roda_search.handler({"tags": ["climate"]}, None)
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# Integration tests — real DynamoDB via Substrate
# ---------------------------------------------------------------------------

_CATALOG_TABLE = "qs-catalog-test"

# Seed items in explicit DynamoDB wire format (avoids Substrate deserialization
# issues with nested map-in-list types that boto3's resource interface produces).
# SS (StringSet) is used for formats/tags so that `"csv" in item["formats"]` works
# after boto3 deserializes SS → Python set.
# NOTE: Substrate does not support empty-string DynamoDB values ("S": "").
# Omit optional string fields that may be empty in real catalog items.
_INTEG_ITEMS_DDB = [
    {
        "slug": {"S": "noaa-climate"},
        "name": {"S": "NOAA Climate"},
        "primaryTag": {"S": "climate"},
        "tags": {"SS": ["climate", "weather"]},
        "description": {"S": "Climate measurements from NOAA stations."},
        "searchText": {"S": "climate weather temperature precipitation atmospheric"},
        "formats": {"SS": ["csv", "parquet"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NOAA"},
        "updateFrequency": {"S": "Daily"},
        "registryUrl": {"S": "https://registry.opendata.aws/noaa-climate/"},
        "s3ResourceCount": {"N": "1"},
    },
    {
        "slug": {"S": "ncbi-genome"},
        "name": {"S": "NCBI 1000 Genomes"},
        "primaryTag": {"S": "genomics"},
        "tags": {"SS": ["genomics", "bioinformatics"]},
        "description": {"S": "Whole genome sequencing data."},
        "searchText": {"S": "genomics dna rna sequencing variant genome"},
        "formats": {"SS": ["vcf", "bam"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NCBI"},
        "updateFrequency": {"S": "Periodic"},
        "registryUrl": {"S": "https://registry.opendata.aws/ncbi-genome/"},
        "s3ResourceCount": {"N": "1"},
    },
    {
        "slug": {"S": "noaa-ocean"},
        "name": {"S": "NOAA Ocean Data"},
        "primaryTag": {"S": "oceans"},
        "tags": {"SS": ["oceans", "marine"]},
        "description": {"S": "Ocean temperature and salinity measurements."},
        "searchText": {"S": "ocean temperature salinity marine coastal bathymetry"},
        "formats": {"SS": ["csv"]},
        "license": {"S": "Open Data"},
        "managedBy": {"S": "NOAA"},
        "updateFrequency": {"S": "Daily"},
        "registryUrl": {"S": "https://registry.opendata.aws/noaa-ocean/"},
        "s3ResourceCount": {"N": "1"},
    },
]


@pytest.mark.integration
class TestRodaSearchIntegration:

    def _reload_search(self, substrate_url, monkeypatch):
        """Reload roda-search handler targeting Substrate."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {"TABLE_NAME": _CATALOG_TABLE, "SEARCH_CACHE_TABLE": ""}
        with patch.dict(os.environ, env):
            alias = "_roda_search_integ"
            path = os.path.join(REPO_ROOT, "lambdas", "roda-search", "handler.py")
            spec = importlib.util.spec_from_file_location(alias, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[alias] = mod
            spec.loader.exec_module(mod)
            return mod

    def _seed_catalog(self, substrate_url, items_ddb):
        """Create table and seed items in Substrate using low-level client.

        Items are in DynamoDB wire format to avoid boto3 resource-interface
        serialization of nested structures that Substrate doesn't round-trip.
        """
        import boto3

        ddb = boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "slug", "AttributeType": "S"},
                {"AttributeName": "primaryTag", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-primary-tag",
                    "KeySchema": [{"AttributeName": "primaryTag", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=_CATALOG_TABLE)
        for item in items_ddb:
            ddb.put_item(TableName=_CATALOG_TABLE, Item=item)

    def test_empty_query_returns_all_seeded_items(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url, _INTEG_ITEMS_DDB)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({}, None)
        assert result["count"] >= 3

    def test_single_tag_filter_via_gsi(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url, _INTEG_ITEMS_DDB)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"tags": ["climate"]}, None)
        assert result["count"] >= 1
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-climate" in slugs
        assert "ncbi-genome" not in slugs

    def test_keyword_search_returns_relevant(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url, _INTEG_ITEMS_DDB)
        h = self._reload_search(substrate_url, monkeypatch)
        # "marine coastal" infers the "oceans" tag → single-tag GSI query → returns noaa-ocean
        result = h.handler({"query": "marine coastal salinity"}, None)
        assert result["count"] >= 1
        slugs = [d["slug"] for d in result["datasets"]]
        assert "noaa-ocean" in slugs

    def test_max_results_one(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url, _INTEG_ITEMS_DDB)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"max_results": 1}, None)
        assert result["count"] == 1

    def test_format_filter_csv_only(self, substrate_url, reset_substrate, monkeypatch):
        self._seed_catalog(substrate_url, _INTEG_ITEMS_DDB)
        h = self._reload_search(substrate_url, monkeypatch)
        result = h.handler({"format": "csv"}, None)
        # noaa-climate (csv,parquet) and noaa-ocean (csv) match; ncbi-genome (vcf,bam) does not
        assert result["count"] == 2
        assert all("csv" in d["formats"] for d in result["datasets"])


# ---------------------------------------------------------------------------
# Tests for Feature 13: quality_score in roda_search results
# ---------------------------------------------------------------------------

_SIX_MONTHS = 180 * 24 * 3600
_TWO_YEARS = 2 * 365 * 24 * 3600


class TestQualityScore:
    """quality_score dict is present in every result and computed correctly."""

    def test_quality_score_present_in_every_result(self):
        table = _make_table(scan_items=ALL_ITEMS)
        with _patch_table(table):
            result = roda_search.handler({}, None)
        for ds in result["datasets"]:
            assert "quality_score" in ds
            qs = ds["quality_score"]
            assert "freshness" in qs
            assert "schema_completeness" in qs
            assert "last_verified" in qs

    def test_freshness_stale_when_last_updated_three_years_ago(self):
        three_years_ago = int(time.time()) - (3 * 365 * 24 * 3600)
        stale_item = dict(CLIMATE_ITEM, last_updated=three_years_ago)
        table = _make_table(scan_items=[stale_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["freshness"] == "stale"

    def test_freshness_current_when_last_updated_recent(self):
        two_months_ago = int(time.time()) - (60 * 24 * 3600)
        recent_item = dict(CLIMATE_ITEM, last_updated=two_months_ago)
        table = _make_table(scan_items=[recent_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["freshness"] == "current"

    def test_freshness_stale_when_last_updated_missing(self):
        item_no_updated = {k: v for k, v in CLIMATE_ITEM.items() if k != "last_updated"}
        table = _make_table(scan_items=[item_no_updated])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["freshness"] == "stale"

    def test_freshness_aging_between_6_and_24_months(self):
        twelve_months_ago = int(time.time()) - (365 * 24 * 3600)
        aging_item = dict(CLIMATE_ITEM, last_updated=twelve_months_ago)
        table = _make_table(scan_items=[aging_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["freshness"] == "aging"

    def test_schema_completeness_1_0_when_all_six_fields_present(self):
        complete_item = {
            "slug": "complete-ds",
            "name": "Complete Dataset",
            "description": "A dataset with all fields present",
            "tags": ["tag1"],
            "formats": ["csv"],
            "s3Resources": [{"arn": "arn:aws:s3:::bucket", "region": "us-east-1"}],
            "registryUrl": "https://registry.opendata.aws/complete/",
            "primaryTag": "climate",
            "searchText": "complete dataset",
            "license": "Open",
            "managedBy": "Test",
            "updateFrequency": "Monthly",
        }
        table = _make_table(scan_items=[complete_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["schema_completeness"] == 1.0

    def test_schema_completeness_less_than_1_when_some_fields_missing(self):
        partial_item = {
            "slug": "partial-ds",
            "name": "Partial Dataset",
            # missing: description, tags, formats, s3Resources, registryUrl
            "primaryTag": "climate",
            "searchText": "partial dataset",
        }
        table = _make_table(scan_items=[partial_item])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["schema_completeness"] < 1.0
        assert qs["schema_completeness"] > 0.0

    def test_last_verified_none_when_field_absent(self):
        item_no_verified = dict(CLIMATE_ITEM)
        item_no_verified.pop("last_verified", None)
        table = _make_table(scan_items=[item_no_verified])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["last_verified"] is None

    def test_last_verified_returned_when_field_present(self):
        verified_ts = "2026-01-15T10:00:00+00:00"
        item_with_verified = dict(CLIMATE_ITEM, last_verified=verified_ts)
        table = _make_table(scan_items=[item_with_verified])
        with _patch_table(table):
            result = roda_search.handler({}, None)
        qs = result["datasets"][0]["quality_score"]
        assert qs["last_verified"] == verified_ts
