"""
Integration tests for federated-search/handler.py.

Uses Substrate (real AWS emulator) for DynamoDB — never moto.
Snowflake and Redshift search paths are not exercised here (no credentials
configured in test env); those connector tests live in test_snowflake_tools.py
and test_redshift_tools.py.
"""

import importlib
import importlib.util
import json
import os
import sys

import boto3
import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

_REGISTRY_TABLE = "qs-data-source-registry-test"
_CATALOG_TABLE = "qs-roda-catalog-test"


class TestFederatedSearch:
    """Substrate-backed integration tests for the federated-search Lambda."""

    def _create_tables(self, substrate_url):
        ddb = boto3.client("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")
        ddb.create_table(
            TableName=_REGISTRY_TABLE,
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=_REGISTRY_TABLE)
        waiter.wait(TableName=_CATALOG_TABLE)
        return boto3.resource("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")

    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        monkeypatch.setenv("REGISTRY_TABLE", _REGISTRY_TABLE)
        monkeypatch.setenv("CATALOG_TABLE", _CATALOG_TABLE)
        monkeypatch.setenv("SNOWFLAKE_SECRET_ARN", "")
        monkeypatch.setenv("REDSHIFT_SECRET_ARN", "")
        path = os.path.join(REPO_ROOT, "lambdas", "federated-search", "handler.py")
        alias = "_fed_search_integ"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod

    def _put_registry(self, resource, **kwargs):
        resource.Table(_REGISTRY_TABLE).put_item(Item=kwargs)

    def _put_catalog(self, resource, **kwargs):
        resource.Table(_CATALOG_TABLE).put_item(Item=kwargs)

    @pytest.mark.integration
    def test_empty_registry_returns_empty_results(self, substrate_url, reset_substrate, monkeypatch):
        self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        result = mod.handler({"query": "climate data"}, None)
        assert result["results"] == []
        assert result["total"] == 0
        assert result["skipped_sources"] == []

    @pytest.mark.integration
    def test_roda_source_matches_by_search_text(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="roda-noaa",
            type="roda",
            display_name="NOAA Climate",
            description="Climate data from NOAA",
            data_classification="public",
            connection_config="",
        )
        self._put_catalog(resource,
            slug="noaa-climate",
            name="NOAA Climate Dataset",
            description="Historical climate data",
            searchText="climate weather temperature precipitation",
        )

        result = mod.handler({"query": "climate temperature"}, None)
        assert result["total"] >= 1
        assert any(r["source_type"] == "roda" for r in result["results"])

    @pytest.mark.integration
    def test_s3_source_matches_by_display_name(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-genomics",
            type="s3",
            display_name="Genomics Research Bucket",
            description="Whole genome sequencing data for research",
            data_classification="internal",
            connection_config=json.dumps({"bucket": "genomics-data", "prefix": "raw/"}),
        )

        result = mod.handler({"query": "genomics sequencing", "caller_clearance": "internal"}, None)
        assert result["total"] >= 1
        assert any(r["source_id"] == "s3-genomics" for r in result["results"])

    @pytest.mark.integration
    def test_data_classification_filter_excludes_non_matching(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-public",
            type="s3",
            display_name="Public Climate Data",
            description="Open climate dataset",
            data_classification="public",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-restricted",
            type="s3",
            display_name="Restricted Climate Data",
            description="Internal climate data with restrictions",
            data_classification="restricted",
            connection_config="{}",
        )

        result = mod.handler({"query": "climate", "data_classification_filter": "public"}, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public" in source_ids
        assert "s3-restricted" not in source_ids

    @pytest.mark.integration
    def test_unreachable_source_goes_to_skipped_sources(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="roda-broken",
            type="roda",
            display_name="Broken RODA Source",
            description="This source throws on scan",
            data_classification="public",
            connection_config="",
        )

        original = mod._search_roda

        def _bad_search(query_words, source):
            raise RuntimeError("DynamoDB unavailable")

        mod._search_roda = _bad_search
        try:
            result = mod.handler({"query": "roda data"}, None)
        finally:
            mod._search_roda = original

        assert "roda-broken" in result["skipped_sources"]

    @pytest.mark.integration
    def test_max_results_caps_output(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        for i in range(20):
            self._put_registry(resource,
                source_id=f"s3-source-{i}",
                type="s3",
                display_name=f"Climate Data Source {i}",
                description="Climate and weather data",
                data_classification="public",
                connection_config="{}",
            )

        result = mod.handler({"query": "climate", "max_results": 5}, None)
        assert result["total"] <= 5
        assert len(result["results"]) <= 5

    @pytest.mark.integration
    def test_results_sorted_by_match_score_descending(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-good-match",
            type="s3",
            display_name="Climate Weather Analysis",
            description="Climate and weather data analysis",
            data_classification="public",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-weak-match",
            type="s3",
            display_name="Climate Station Data",
            description="Generic data",
            data_classification="public",
            connection_config="{}",
        )

        result = mod.handler({"query": "climate weather"}, None)
        scores = [r["match_score"] for r in result["results"]]
        assert scores == sorted(scores, reverse=True)

    @pytest.mark.integration
    def test_query_required(self, substrate_url, reset_substrate, monkeypatch):
        self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handler({}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_max_results_default_ten(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        for i in range(15):
            self._put_registry(resource,
                source_id=f"s3-src-{i}",
                type="s3",
                display_name=f"Dataset {i} climate",
                description="climate data",
                data_classification="public",
                connection_config="{}",
            )

        result = mod.handler({"query": "climate"}, None)
        assert result["total"] <= 10

    # ------------------------------------------------------------------
    # caller_clearance tests (Issue #16)
    # ------------------------------------------------------------------

    @pytest.mark.integration
    def test_caller_clearance_public_excludes_restricted(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """Public clearance should not return restricted or phi sources."""
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-public-data",
            type="s3",
            display_name="Public Climate Dataset",
            description="Open climate records",
            data_classification="public",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-restricted-data",
            type="s3",
            display_name="Restricted Climate Records",
            description="Internal restricted climate data",
            data_classification="restricted",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-phi-data",
            type="s3",
            display_name="PHI Clinical Climate Data",
            description="Patient health information climate exposure",
            data_classification="phi",
            connection_config="{}",
        )

        result = mod.handler({"query": "climate", "caller_clearance": "public"}, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public-data" in source_ids
        assert "s3-restricted-data" not in source_ids
        assert "s3-phi-data" not in source_ids

    @pytest.mark.integration
    def test_caller_clearance_restricted_excludes_phi(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """Restricted clearance should return public, internal, restricted but not phi."""
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        for classification in ["public", "internal", "restricted", "phi"]:
            self._put_registry(resource,
                source_id=f"s3-{classification}",
                type="s3",
                display_name=f"Climate {classification} data",
                description="climate data records",
                data_classification=classification,
                connection_config="{}",
            )

        result = mod.handler({"query": "climate", "caller_clearance": "restricted"}, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public" in source_ids
        assert "s3-internal" in source_ids
        assert "s3-restricted" in source_ids
        assert "s3-phi" not in source_ids

    @pytest.mark.integration
    def test_caller_clearance_phi_returns_all_levels(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """PHI clearance should return sources at every classification level."""
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        for classification in ["public", "internal", "restricted", "phi"]:
            self._put_registry(resource,
                source_id=f"s3-{classification}",
                type="s3",
                display_name=f"Health {classification} records",
                description="health data records",
                data_classification=classification,
                connection_config="{}",
            )

        result = mod.handler({"query": "health", "caller_clearance": "phi"}, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public" in source_ids
        assert "s3-internal" in source_ids
        assert "s3-restricted" in source_ids
        assert "s3-phi" in source_ids

    @pytest.mark.integration
    def test_caller_clearance_defaults_to_public(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """Omitting caller_clearance defaults to public — most restrictive."""
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-internal-only",
            type="s3",
            display_name="Internal genome data",
            description="internal genome sequencing",
            data_classification="internal",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-open-genome",
            type="s3",
            display_name="Open genome data",
            description="public genome sequencing",
            data_classification="public",
            connection_config="{}",
        )

        # No caller_clearance provided
        result = mod.handler({"query": "genome"}, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-open-genome" in source_ids
        assert "s3-internal-only" not in source_ids

    @pytest.mark.integration
    def test_caller_clearance_and_classification_filter_combined(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """caller_clearance and data_classification_filter can be used together."""
        resource = self._create_tables(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        self._put_registry(resource,
            source_id="s3-internal-a",
            type="s3",
            display_name="Internal dataset A",
            description="internal dataset A records",
            data_classification="internal",
            connection_config="{}",
        )
        self._put_registry(resource,
            source_id="s3-public-b",
            type="s3",
            display_name="Public dataset B",
            description="public dataset B records",
            data_classification="public",
            connection_config="{}",
        )

        # internal clearance + filter for public only → only public returned
        result = mod.handler({
            "query": "dataset",
            "caller_clearance": "internal",
            "data_classification_filter": "public",
        }, None)
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public-b" in source_ids
        assert "s3-internal-a" not in source_ids
