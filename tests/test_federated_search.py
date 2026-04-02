"""
Unit tests for federated-search/handler.py.

Uses moto to mock DynamoDB.
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

_REGISTRY_TABLE = "qs-data-source-registry-test"
_CATALOG_TABLE = "qs-roda-catalog-test"


def _load(registry_table=_REGISTRY_TABLE, catalog_table=_CATALOG_TABLE):
    env_patch = {
        "REGISTRY_TABLE": registry_table,
        "CATALOG_TABLE": catalog_table,
        "SNOWFLAKE_SECRET_ARN": "",
        "REDSHIFT_SECRET_ARN": "",
    }
    with patch.dict(os.environ, env_patch):
        path = os.path.join(REPO_ROOT, "lambdas", "federated-search", "handler.py")
        alias = f"_fed_search_{registry_table}"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod


class TestFederatedSearch:
    @pytest.fixture(autouse=True)
    def setup_moto(self):
        with mock_aws():
            self.ddb = boto3.client("dynamodb", region_name="us-east-1")
            self.resource = boto3.resource("dynamodb", region_name="us-east-1")
            self._create_tables()
            self.mod = _load(_REGISTRY_TABLE, _CATALOG_TABLE)
            self.mod.dynamodb = self.resource
            yield

    def _create_tables(self):
        self.ddb.create_table(
            TableName=_REGISTRY_TABLE,
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self.ddb.create_table(
            TableName=_CATALOG_TABLE,
            KeySchema=[{"AttributeName": "slug", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "slug", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = self.ddb.get_waiter("table_exists")
        waiter.wait(TableName=_REGISTRY_TABLE)
        waiter.wait(TableName=_CATALOG_TABLE)

    def _put_registry_item(self, **kwargs):
        self.resource.Table(_REGISTRY_TABLE).put_item(Item=kwargs)

    def _put_catalog_item(self, **kwargs):
        self.resource.Table(_CATALOG_TABLE).put_item(Item=kwargs)

    def _call(self, event):
        return self.mod.handler(event, None)

    def test_empty_registry_returns_empty_results(self):
        result = self._call({"query": "climate data"})
        assert result["results"] == []
        assert result["total"] == 0
        assert result["skipped_sources"] == []

    def test_roda_source_matches_by_search_text(self):
        self._put_registry_item(
            source_id="roda-noaa",
            type="roda",
            display_name="NOAA Climate",
            description="Climate data from NOAA",
            data_classification="public",
            connection_config="",
        )
        self._put_catalog_item(
            slug="noaa-climate",
            name="NOAA Climate Dataset",
            description="Historical climate data",
            searchText="climate weather temperature precipitation",
        )
        result = self._call({"query": "climate temperature"})
        assert result["total"] >= 1
        assert any(r["source_type"] == "roda" for r in result["results"])

    def test_s3_source_matches_by_display_name(self):
        self._put_registry_item(
            source_id="s3-genomics",
            type="s3",
            display_name="Genomics Research Bucket",
            description="Whole genome sequencing data for research",
            data_classification="internal",
            connection_config=json.dumps({"bucket": "genomics-data", "prefix": "raw/"}),
        )
        result = self._call({"query": "genomics sequencing"})
        assert result["total"] >= 1
        assert any(r["source_id"] == "s3-genomics" for r in result["results"])

    def test_data_classification_filter_excludes_non_matching(self):
        self._put_registry_item(
            source_id="s3-public",
            type="s3",
            display_name="Public Climate Data",
            description="Open climate dataset",
            data_classification="public",
            connection_config="{}",
        )
        self._put_registry_item(
            source_id="s3-restricted",
            type="s3",
            display_name="Restricted Climate Data",
            description="Internal climate data with restrictions",
            data_classification="restricted",
            connection_config="{}",
        )
        result = self._call({"query": "climate", "data_classification_filter": "public"})
        source_ids = [r["source_id"] for r in result["results"]]
        assert "s3-public" in source_ids
        assert "s3-restricted" not in source_ids

    def test_unreachable_source_goes_to_skipped_sources(self):
        self._put_registry_item(
            source_id="roda-broken",
            type="roda",
            display_name="Broken RODA Source",
            description="This source throws on scan",
            data_classification="public",
            connection_config="",
        )

        def _bad_search(query_words, source):
            raise RuntimeError("DynamoDB unavailable")

        original = self.mod._search_roda
        self.mod._search_roda = _bad_search
        try:
            result = self._call({"query": "roda data"})
        finally:
            self.mod._search_roda = original

        assert "roda-broken" in result["skipped_sources"]

    def test_max_results_caps_output(self):
        for i in range(20):
            self._put_registry_item(
                source_id=f"s3-source-{i}",
                type="s3",
                display_name=f"Climate Data Source {i}",
                description="Climate and weather data",
                data_classification="public",
                connection_config="{}",
            )
        result = self._call({"query": "climate", "max_results": 5})
        assert result["total"] <= 5
        assert len(result["results"]) <= 5

    def test_results_sorted_by_match_score_descending(self):
        # Source 1: matches query well (both words in both fields)
        self._put_registry_item(
            source_id="s3-good-match",
            type="s3",
            display_name="Climate Weather Analysis",
            description="Climate and weather data analysis",
            data_classification="public",
            connection_config="{}",
        )
        # Source 2: weaker match (only one word matches)
        self._put_registry_item(
            source_id="s3-weak-match",
            type="s3",
            display_name="Climate Station Data",
            description="Generic data",
            data_classification="public",
            connection_config="{}",
        )
        result = self._call({"query": "climate weather"})
        scores = [r["match_score"] for r in result["results"]]
        assert scores == sorted(scores, reverse=True)

    def test_query_required(self):
        result = self._call({})
        assert "error" in result

    def test_max_results_default_ten(self):
        for i in range(15):
            self._put_registry_item(
                source_id=f"s3-src-{i}",
                type="s3",
                display_name=f"Dataset {i} climate",
                description="climate data",
                data_classification="public",
                connection_config="{}",
            )
        result = self._call({"query": "climate"})
        assert result["total"] <= 10
