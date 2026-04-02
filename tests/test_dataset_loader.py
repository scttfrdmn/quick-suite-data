"""
Tests for dataset-loader/handler.py (roda_load tool).

Unit tests mock all AWS clients at the module level.
Integration tests (marked) use real Substrate QuickSight while mocking
DynamoDB and S3 — isolating the QS API path through real HTTP.
"""

import importlib
import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _load(module_alias="_dataset_loader_handler"):
    """Load dataset-loader/handler.py under a unique module name."""
    path = os.path.join(REPO_ROOT, "lambdas", "dataset-loader", "handler.py")
    spec = importlib.util.spec_from_file_location(module_alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_alias] = mod
    spec.loader.exec_module(mod)
    return mod


dataset_loader = _load()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CATALOG_ITEM = {
    "slug": "noaa-climate",
    "name": "NOAA Global Climate Data",
    "formats": ["csv", "parquet"],
    "s3Resources": [
        {
            "arn": "arn:aws:s3:::noaa-climate-data",
            "region": "us-east-1",
            "requesterPays": False,
        }
    ],
    "registryUrl": "https://registry.opendata.aws/noaa-climate/",
    "documentation": "https://docs.noaa.gov/climate",
}

CSV_FILES = ["2020/data.csv", "2021/data.csv", "2022/data.csv"]


def _mock_s3_with_files(keys=None):
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": k} for k in (keys if keys is not None else CSV_FILES)]}
    ]
    s3.get_paginator.return_value = paginator
    s3.put_object.return_value = {}
    return s3


def _mock_ddb_with_item(item=None):
    ddb = MagicMock()
    ddb.Table.return_value.get_item.return_value = {"Item": item or CATALOG_ITEM}
    return ddb


def _mock_qs_success():
    qs = MagicMock()
    qs.create_data_source.return_value = {
        "DataSourceId": "test-source",
        "Arn": "arn:aws:quicksight:us-east-1:123456789012:datasource/test-source",
        "CreationStatus": "CREATION_SUCCESSFUL",
        "RequestId": "req-001",
    }
    return qs


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

class TestMissingSlug:
    def test_empty_slug_returns_error(self):
        result = dataset_loader.handler({}, None)
        assert "error" in result
        assert "slug" in result["error"].lower()

    def test_whitespace_slug_returns_error(self):
        result = dataset_loader.handler({"slug": "   "}, None)
        assert "error" in result


class TestCatalogLookup:
    def test_missing_dataset_returns_error(self):
        ddb = MagicMock()
        ddb.Table.return_value.get_item.return_value = {"Item": None}
        with patch.object(dataset_loader, "dynamodb", ddb):
            result = dataset_loader.handler({"slug": "does-not-exist"}, None)
        assert "error" in result
        assert "not found" in result["error"].lower()

    def test_dynamodb_exception_returns_error(self):
        ddb = MagicMock()
        ddb.Table.return_value.get_item.side_effect = Exception("Connection timeout")
        with patch.object(dataset_loader, "dynamodb", ddb):
            result = dataset_loader.handler({"slug": "noaa-climate"}, None)
        assert "error" in result

    def test_no_s3_resources_returns_suggestion(self):
        item = dict(CATALOG_ITEM, s3Resources=[])
        with patch.object(dataset_loader, "dynamodb", _mock_ddb_with_item(item)):
            result = dataset_loader.handler({"slug": "noaa-climate"}, None)
        assert "error" in result
        assert "suggestion" in result

    def test_resource_index_out_of_range(self):
        with patch.object(dataset_loader, "dynamodb", _mock_ddb_with_item(CATALOG_ITEM)):
            result = dataset_loader.handler({"slug": "noaa-climate", "resource_index": 99}, None)
        assert "error" in result
        assert "out of range" in result["error"]


class TestFormatHandling:
    def test_unsupported_format_returns_requires_transform(self):
        item = dict(CATALOG_ITEM, formats=["hdf5"])
        with patch.object(dataset_loader, "dynamodb", _mock_ddb_with_item(item)):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "hdf5"}, None)
        assert result.get("status") == "requires_transform"

    def test_format_auto_detected_from_catalog(self):
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(["data/part-0.parquet"])
        qs = _mock_qs_success()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs):
            result = dataset_loader.handler({"slug": "noaa-climate"}, None)
        assert result.get("format") == "parquet"


class TestBucketProbe:
    def test_no_matching_files_returns_status(self):
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files([])
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert result.get("status") == "no_matching_files"

    def test_s3_exception_returns_error(self):
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.side_effect = Exception("Access Denied")
        s3.get_paginator.return_value = paginator
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert "error" in result

    def test_sample_only_limits_files(self):
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files([f"2020/{i}.csv" for i in range(50)])
        qs = _mock_qs_success()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs):
            result = dataset_loader.handler(
                {"slug": "noaa-climate", "format": "csv", "sample_only": True}, None
            )
        assert result.get("fileCount", 0) <= 10


class TestSuccessfulLoad:
    def test_quicksight_exception_returns_manifest_ready(self):
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = MagicMock()
        qs.create_data_source.side_effect = Exception("QuickSight throttled")
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert result["status"] == "manifest_ready"
        assert "manifestUri" in result


# ---------------------------------------------------------------------------
# Integration tests — real QuickSight via Substrate
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestQuickSightIntegration:
    def _load_fresh(self, substrate_url, monkeypatch):
        """Reload handler with AWS_ENDPOINT_URL pointing at Substrate."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "TABLE_NAME": "qs-open-data-catalog",
            "MANIFEST_BUCKET": "qs-manifests-test",
            "QUICKSIGHT_ACCOUNT_ID": "123456789012",
            "QUICKSIGHT_REGION": "us-east-1",
        }
        with patch.dict(os.environ, env):
            return _load("_dataset_loader_integ")

    def test_create_data_source_reaches_substrate(self, substrate_url, reset_substrate, monkeypatch):
        h = self._load_fresh(substrate_url, monkeypatch)
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)

        with patch.object(h, "dynamodb", ddb), \
             patch.object(h, "s3", s3):
            result = h.handler({"slug": "noaa-climate", "format": "csv"}, None)

        assert result["status"] == "loaded", f"Unexpected result: {result}"
        qs_result = result.get("quicksightResult", {})
        assert qs_result.get("status") == "created"

    def test_qs_datasource_id_in_response(self, substrate_url, reset_substrate, monkeypatch):
        h = self._load_fresh(substrate_url, monkeypatch)
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)

        with patch.object(h, "dynamodb", ddb), \
             patch.object(h, "s3", s3):
            result = h.handler({"slug": "noaa-climate", "format": "csv"}, None)

        ds_id = result.get("quicksightResult", {}).get("dataSourceId", "")
        assert ds_id != ""
        assert "source" in ds_id

    def test_custom_dataset_name_preserved(self, substrate_url, reset_substrate, monkeypatch):
        h = self._load_fresh(substrate_url, monkeypatch)
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)

        with patch.object(h, "dynamodb", ddb), \
             patch.object(h, "s3", s3):
            result = h.handler(
                {"slug": "noaa-climate", "format": "csv", "dataset_name": "My Climate DS"}, None
            )

        assert result["status"] == "loaded", f"Unexpected result: {result}"
        assert result["datasetName"] == "My Climate DS"
        assert "datasetId" in result
        assert result["fileCount"] == len(CSV_FILES)

    def test_second_load_same_slug_succeeds(self, substrate_url, reset_substrate, monkeypatch):
        h = self._load_fresh(substrate_url, monkeypatch)
        ddb = _mock_ddb_with_item(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)

        with patch.object(h, "dynamodb", ddb), \
             patch.object(h, "s3", s3):
            r1 = h.handler({"slug": "noaa-climate", "format": "csv"}, None)
            r2 = h.handler({"slug": "noaa-climate", "format": "csv"}, None)

        assert r1["status"] == "loaded"
        assert r2["status"] == "loaded"
        assert r1["datasetId"] != r2["datasetId"]


# ---------------------------------------------------------------------------
# Helpers for join + suggestions tests
# ---------------------------------------------------------------------------

JOIN_ITEM = {
    "slug": "noaa-ocean",
    "name": "NOAA Ocean Data",
    "formats": ["csv"],
    "s3Resources": [{"arn": "arn:aws:s3:::noaa-ocean-data", "region": "us-east-1", "requesterPays": False}],
    "registryUrl": "https://registry.opendata.aws/noaa-ocean/",
}

CATALOG_ITEM_WITH_TAG = dict(CATALOG_ITEM, primaryTag="climate")

SUGG_ITEMS = [
    {"slug": "noaa-ocean", "name": "NOAA Ocean Data"},
    {"slug": "nasa-climate", "name": "NASA Climate"},
    {"slug": "ecmwf-forecast", "name": "ECMWF Forecast"},
    {"slug": "era5-reanalysis", "name": "ERA5 Reanalysis"},
    {"slug": "copernicus-land", "name": "Copernicus Land"},
    {"slug": "extra-sixth", "name": "Extra Sixth"},
]


def _mock_qs_full():
    """QS mock that succeeds through the full create_data_source polling loop."""
    qs = MagicMock()
    qs.create_data_source.return_value = {
        "DataSourceId": "test-source",
        "Arn": "arn:aws:quicksight:us-east-1:123456789012:datasource/test-source",
        "CreationStatus": "CREATION_SUCCESSFUL",
        "RequestId": "req-001",
    }
    qs.describe_data_source.return_value = {"DataSource": {"Status": "CREATION_SUCCESSFUL"}}
    qs.create_data_set.return_value = {"DataSetId": "test-dataset"}
    return qs


def _mock_ddb_sequence(*items):
    """DDB mock returning successive items on each get_item call."""
    ddb = MagicMock()
    responses = [{"Item": item} if item is not None else {} for item in items]
    ddb.Table.return_value.get_item.side_effect = responses
    ddb.Table.return_value.query.return_value = {"Items": []}
    return ddb


def _mock_ddb_with_suggestions(item, suggestions=None):
    """DDB mock with get_item and query (for suggestions) both configured."""
    ddb = MagicMock()
    ddb.Table.return_value.get_item.return_value = {"Item": item}
    ddb.Table.return_value.query.return_value = {"Items": suggestions or []}
    return ddb


def _mock_s3_two_probes(primary_keys, join_keys):
    """S3 paginator returning different keys for successive probe calls."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.side_effect = [
        [{"Contents": [{"Key": k} for k in primary_keys]}],
        [{"Contents": [{"Key": k} for k in join_keys]}],
    ]
    s3.get_paginator.return_value = paginator
    s3.put_object.return_value = {}
    return s3


# ---------------------------------------------------------------------------
# TestDatasetLoaderJoin
# ---------------------------------------------------------------------------

class TestDatasetLoaderJoin:
    """Tests for the join_slug + join_key path in roda_load."""

    _JOIN_EVENT = {
        "slug": "noaa-climate",
        "format": "csv",
        "join_slug": "noaa-ocean",
        "join_key": "station_id",
    }

    def test_join_happy_path(self):
        ddb = _mock_ddb_sequence(CATALOG_ITEM, JOIN_ITEM)
        s3 = _mock_s3_two_probes(CSV_FILES, ["ocean/2020.csv", "ocean/2021.csv"])
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(self._JOIN_EVENT, None)
        assert result["status"] == "loaded"
        assert result.get("join_applied") is True
        assert result.get("join_slug") == "noaa-ocean"
        qs.create_data_set.assert_called_once()
        call_kwargs = qs.create_data_set.call_args[1]
        assert "LogicalTableMap" in call_kwargs

    def test_join_slug_not_found_skips_join(self):
        # Second get_item returns {} (no Item) — join silently skipped
        ddb = _mock_ddb_sequence(CATALOG_ITEM, None)
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(self._JOIN_EVENT, None)
        assert result["status"] == "loaded"
        assert not result.get("join_applied")

    def test_join_slug_no_s3_files_skips_join(self):
        ddb = _mock_ddb_sequence(CATALOG_ITEM, JOIN_ITEM)
        # Primary probe returns files; join probe returns nothing
        s3 = _mock_s3_two_probes(CSV_FILES, [])
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(self._JOIN_EVENT, None)
        assert result["status"] == "loaded"
        assert not result.get("join_applied")

    def test_join_create_datasource_fails_returns_manifest_ready(self):
        ddb = _mock_ddb_sequence(CATALOG_ITEM, JOIN_ITEM)
        s3 = _mock_s3_two_probes(CSV_FILES, ["ocean/2020.csv"])
        qs = MagicMock()
        primary_resp = {
            "DataSourceId": "src-primary",
            "Arn": "arn:aws:quicksight:us-east-1:123456789012:datasource/src-primary",
        }
        qs.create_data_source.side_effect = [primary_resp, Exception("QS error on join source")]
        qs.describe_data_source.return_value = {"DataSource": {"Status": "CREATION_SUCCESSFUL"}}
        qs.create_data_set.return_value = {}
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(self._JOIN_EVENT, None)
        assert result["status"] == "manifest_ready"

    def test_join_slug_without_join_key_is_ignored(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(
                {"slug": "noaa-climate", "format": "csv", "join_slug": "noaa-ocean"}, None
            )
        assert result["status"] == "loaded"
        assert not result.get("join_applied")

    def test_join_key_without_join_slug_is_ignored(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM)
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler(
                {"slug": "noaa-climate", "format": "csv", "join_key": "station_id"}, None
            )
        assert result["status"] == "loaded"
        assert not result.get("join_applied")


# ---------------------------------------------------------------------------
# TestDatasetLoaderSuggestions
# ---------------------------------------------------------------------------

class TestDatasetLoaderSuggestions:
    """Tests for the related-dataset suggestions path in roda_load."""

    def test_suggestions_returned_for_primary_tag(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM_WITH_TAG, SUGG_ITEMS[:3])
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert result["status"] == "loaded"
        sugg = result.get("suggestions", [])
        assert len(sugg) == 3
        assert all("slug" in s and "name" in s for s in sugg)

    def test_suggestions_capped_at_5(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM_WITH_TAG, SUGG_ITEMS)  # 6 items
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert len(result.get("suggestions", [])) == 5

    def test_no_primary_tag_returns_empty_suggestions(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM)  # no primaryTag
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert result.get("suggestions") == []
        # query should not have been called for suggestions GSI
        for call in ddb.Table.return_value.query.call_args_list:
            assert call[1].get("IndexName") != "by-primary-tag"

    def test_suggestions_gsi_error_is_nonfatal(self):
        ddb = _mock_ddb_with_suggestions(CATALOG_ITEM_WITH_TAG)
        ddb.Table.return_value.query.side_effect = Exception("GSI not found")
        s3 = _mock_s3_with_files(CSV_FILES)
        qs = _mock_qs_full()
        with patch.object(dataset_loader, "dynamodb", ddb), \
             patch.object(dataset_loader, "s3", s3), \
             patch.object(dataset_loader, "quicksight", qs), \
             patch("time.sleep"):
            result = dataset_loader.handler({"slug": "noaa-climate", "format": "csv"}, None)
        assert result.get("suggestions") == []
        assert result.get("status") == "loaded"
        assert "datasetId" in result
