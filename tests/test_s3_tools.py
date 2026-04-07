"""
Tests for s3-browse, s3-preview, and s3-load handlers.

Three lambda directories each have a handler.py with the same module name.
Use importlib.util.spec_from_file_location to load them as distinct modules.

Unit tests use MagicMock for all AWS clients.
Integration tests (marked) route QuickSight calls through Substrate.
"""

import importlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest

# Ensure data_utils (common layer) is importable for s3-preview
REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(REPO_ROOT, "lambdas", "common", "python"))


def _load_handler(lambda_dir: str, module_alias: str):
    """
    Load lambdas/{lambda_dir}/handler.py as a module named module_alias.

    Needed because all three lambda directories contain a file named handler.py
    and they cannot share the same module name in sys.modules.
    """
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(module_alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_alias] = mod
    spec.loader.exec_module(mod)
    return mod


_s3_browse = _load_handler("s3-browse", "_s3_browse_handler")
_s3_preview = _load_handler("s3-preview", "_s3_preview_handler")
_s3_load = _load_handler("s3-load", "_s3_load_handler")

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SOURCES = [
    {
        "label": "Research Data",
        "bucket": "uni-research-data",
        "prefix": "datasets/",
        "description": "Institutional research datasets",
    },
    {
        "label": "Enrollment",
        "bucket": "uni-enrollment",
        "prefix": "",
        "description": "Student enrollment records",
    },
]

CSV_CONTENT = b"id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n"


def _make_s3_list(keys=None, prefixes=None):
    mock_s3 = MagicMock()
    contents = [
        {"Key": k, "Size": 1024, "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc)}
        for k in (keys or [])
    ]
    common_prefixes = [{"Prefix": p} for p in (prefixes or [])]
    mock_s3.list_objects_v2.return_value = {
        "Contents": contents,
        "CommonPrefixes": common_prefixes,
        "IsTruncated": False,
    }
    return mock_s3


def _make_s3_paginator(keys=None):
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": k} for k in (keys or [])]}
    ]
    mock_s3.get_paginator.return_value = paginator
    mock_s3.put_object.return_value = {}
    return mock_s3


# ===========================================================================
# s3_browse
# ===========================================================================

class TestS3Browse:
    def test_no_args_returns_source_catalog(self):
        with patch.object(_s3_browse, "_sources", SOURCES):
            result = _s3_browse.handler({}, None)
        assert "sources" in result
        assert result["count"] == 2

    def test_list_sources_flag_returns_catalog(self):
        with patch.object(_s3_browse, "_sources", SOURCES):
            result = _s3_browse.handler({"list_sources": True}, None)
        assert "sources" in result

    def test_catalog_includes_label_and_description(self):
        with patch.object(_s3_browse, "_sources", SOURCES):
            result = _s3_browse.handler({}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "Research Data" in labels
        assert "Enrollment" in labels

    def test_unknown_source_returns_error_with_available(self):
        with patch.object(_s3_browse, "_sources", SOURCES):
            result = _s3_browse.handler({"source": "Nonexistent"}, None)
        assert "error" in result
        assert "not found" in result["error"].lower()
        assert "availableSources" not in result

    def test_source_lookup_case_insensitive(self):
        mock_s3 = _make_s3_list(keys=["datasets/sample.csv"])
        with patch.object(_s3_browse, "_sources", SOURCES), \
             patch.object(_s3_browse, "s3", mock_s3):
            result = _s3_browse.handler({"source": "RESEARCH DATA"}, None)
        assert "error" not in result

    def test_browse_returns_objects_and_metadata(self):
        mock_s3 = _make_s3_list(keys=["datasets/2023.csv", "datasets/2024.csv"])
        with patch.object(_s3_browse, "_sources", SOURCES), \
             patch.object(_s3_browse, "s3", mock_s3):
            result = _s3_browse.handler({"source": "Research Data"}, None)
        assert result["count"] == 2
        assert result["bucket"] == "uni-research-data"
        assert result["source"] == "Research Data"

    def test_browse_returns_subdirectories(self):
        mock_s3 = _make_s3_list(prefixes=["datasets/2023/", "datasets/2024/"])
        with patch.object(_s3_browse, "_sources", SOURCES), \
             patch.object(_s3_browse, "s3", mock_s3):
            result = _s3_browse.handler({"source": "Research Data"}, None)
        assert len(result["subdirectories"]) == 2

    def test_max_keys_capped_at_500(self):
        mock_s3 = _make_s3_list()
        with patch.object(_s3_browse, "_sources", SOURCES), \
             patch.object(_s3_browse, "s3", mock_s3):
            _s3_browse.handler({"source": "Research Data", "max_keys": 9999}, None)
        call_kwargs = mock_s3.list_objects_v2.call_args[1]
        assert call_kwargs["MaxKeys"] <= 500

    def test_s3_exception_returns_error(self):
        mock_s3 = MagicMock()
        # NoSuchBucket must be a real exception class so the handler can catch it
        mock_s3.exceptions.NoSuchBucket = type("NoSuchBucket", (Exception,), {})
        mock_s3.list_objects_v2.side_effect = Exception("Access Denied")
        with patch.object(_s3_browse, "_sources", SOURCES), \
             patch.object(_s3_browse, "s3", mock_s3):
            result = _s3_browse.handler({"source": "Research Data"}, None)
        assert "error" in result

    def test_no_sources_configured(self):
        with patch.object(_s3_browse, "_sources", []):
            result = _s3_browse.handler({}, None)
        assert result["count"] == 0

    def test_source_request_with_no_sources_returns_error_not_s3_access(self):
        """When no sources are configured, requesting a source must return an error
        without making any S3 API call (fail-closed — issue #50)."""
        mock_s3 = MagicMock()
        with patch.object(_s3_browse, "_sources", []), \
             patch.object(_s3_browse, "s3", mock_s3):
            result = _s3_browse.handler({"source": "any-source"}, None)
        assert "error" in result
        assert "not found" in result["error"]
        mock_s3.list_objects_v2.assert_not_called()


# ===========================================================================
# s3_preview
# ===========================================================================

class TestS3Preview:
    def test_missing_both_args_returns_error(self):
        with patch.object(_s3_preview, "_sources", SOURCES):
            result = _s3_preview.handler({}, None)
        assert "error" in result

    def test_missing_key_returns_error(self):
        with patch.object(_s3_preview, "_sources", SOURCES):
            result = _s3_preview.handler({"source": "Research Data"}, None)
        assert "error" in result

    def test_missing_source_returns_error(self):
        with patch.object(_s3_preview, "_sources", SOURCES):
            result = _s3_preview.handler({"key": "data.csv"}, None)
        assert "error" in result

    def test_unknown_source_returns_error_with_available(self):
        with patch.object(_s3_preview, "_sources", SOURCES):
            result = _s3_preview.handler({"source": "Bogus", "key": "file.csv"}, None)
        assert "error" in result
        assert "not found" in result["error"].lower()
        assert "availableSources" not in result

    def test_preview_csv_returns_schema_and_rows(self):
        mock_s3 = MagicMock()
        # NoSuchKey must be a real exception class for the handler's except clause
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.head_object.return_value = {
            "ContentLength": len(CSV_CONTENT),
            "ContentType": "text/csv",
        }
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=CSV_CONTENT)),
        }
        with patch.object(_s3_preview, "_sources", SOURCES), \
             patch.object(_s3_preview, "s3", mock_s3):
            result = _s3_preview.handler(
                {"source": "Research Data", "key": "2023/data.csv"}, None
            )
        # infer_schema_from_bytes merges columns/sample_rows/row_count directly into result
        assert "columns" in result
        assert "sample_rows" in result
        assert result.get("format") == "csv"

    def test_s3_get_object_exception_returns_error(self):
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.head_object.side_effect = Exception("access denied")
        with patch.object(_s3_preview, "_sources", SOURCES), \
             patch.object(_s3_preview, "s3", mock_s3):
            result = _s3_preview.handler(
                {"source": "Research Data", "key": "missing.csv"}, None
            )
        assert "error" in result


# ===========================================================================
# s3_load
# ===========================================================================

class TestS3Load:
    def test_missing_source_returns_error(self):
        with patch.object(_s3_load, "_sources", SOURCES):
            result = _s3_load.handler({}, None)
        assert "error" in result
        assert '"source"' in result["error"]

    def test_unknown_source_returns_error(self):
        with patch.object(_s3_load, "_sources", SOURCES):
            result = _s3_load.handler({"source": "Nonexistent"}, None)
        assert "error" in result
        assert "not found" in result["error"].lower()
        assert "availableSources" not in result

    def test_no_matching_files_returns_status(self):
        mock_s3 = _make_s3_paginator(keys=[])
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3):
            result = _s3_load.handler({"source": "Research Data", "format": "csv"}, None)
        assert result["status"] == "no_matching_files"

    def test_unsupported_format_returns_requires_transform(self):
        # _list_files for unknown format falls back to default extensions (csv/tsv/parquet/json).
        # Provide a csv file so listing succeeds; then the explicit unsupported format triggers
        # requires_transform AFTER the file list passes.
        mock_s3 = _make_s3_paginator(keys=["data/sample.csv"])
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3):
            result = _s3_load.handler({"source": "Research Data", "format": "hdf5"}, None)
        assert result["status"] == "requires_transform"

    def test_qs_failure_returns_manifest_ready(self):
        mock_s3 = _make_s3_paginator(keys=["datasets/2023.csv"])
        mock_qs = MagicMock()
        mock_qs.create_data_source.side_effect = Exception("QS throttled")
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs):
            result = _s3_load.handler({"source": "Research Data", "format": "csv"}, None)
        assert result["status"] == "manifest_ready"
        assert "manifestUri" in result

    def test_sample_only_limits_to_ten_files(self):
        mock_s3 = _make_s3_paginator(keys=[f"data/{i}.csv" for i in range(50)])
        mock_qs = MagicMock()
        mock_qs.create_data_source.return_value = {}
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs):
            result = _s3_load.handler(
                {"source": "Research Data", "format": "csv", "sample_only": True}, None
            )
        assert result.get("fileCount", 0) <= 10


# ===========================================================================
# s3_load integration — real QuickSight via Substrate
# ===========================================================================

@pytest.mark.integration
class TestS3LoadIntegration:
    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "MANIFEST_BUCKET": "qs-manifests-test",
            "QUICKSIGHT_ACCOUNT_ID": "123456789012",
            "QUICKSIGHT_REGION": "us-east-1",
            "SOURCES_CONFIG": json.dumps(SOURCES),
        }
        with patch.dict(os.environ, env):
            spec = importlib.util.spec_from_file_location(
                "_s3_load_integ",
                os.path.join(REPO_ROOT, "lambdas", "s3-load", "handler.py"),
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        return mod

    def test_load_creates_qs_datasource(self, substrate_url, reset_substrate, monkeypatch):
        h = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/2023.csv", "datasets/2024.csv"])
        with patch.object(h, "s3", mock_s3):
            result = h.handler({"source": "Research Data", "format": "csv"}, None)

        assert result["status"] == "loaded", f"Unexpected result: {result}"
        qs_result = result.get("quicksightResult", {})
        assert qs_result.get("status") == "created"

    def test_format_inferred_from_parquet_extension(self, substrate_url, reset_substrate, monkeypatch):
        h = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/report.parquet"])
        with patch.object(h, "s3", mock_s3):
            result = h.handler({"source": "Research Data"}, None)
        assert result["status"] == "loaded", f"Unexpected result: {result}"
        assert result.get("format") == "parquet"

    def test_load_qs_datasource_id_ends_with_source(self, substrate_url, reset_substrate, monkeypatch):
        h = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/2023.csv"])
        with patch.object(h, "s3", mock_s3):
            result = h.handler({"source": "Enrollment", "format": "csv"}, None)

        assert result["status"] == "loaded"
        ds_id = result.get("quicksightResult", {}).get("dataSourceId", "")
        assert ds_id.endswith("-source"), f"Expected '-source' suffix, got: {ds_id}"


# ===========================================================================
# s3_load multi-prefix
# ===========================================================================


@pytest.mark.integration
class TestS3LoadMultiPrefixIntegration:
    """Tests for the prefixes list parameter — real QuickSight via Substrate."""

    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "MANIFEST_BUCKET": "qs-manifests-test",
            "QUICKSIGHT_ACCOUNT_ID": "123456789012",
            "QUICKSIGHT_REGION": "us-east-1",
            "SOURCES_CONFIG": json.dumps(SOURCES),
        }
        with patch.dict(os.environ, env):
            spec = importlib.util.spec_from_file_location(
                "_s3_load_mp_integ",
                os.path.join(REPO_ROOT, "lambdas", "s3-load", "handler.py"),
            )
            mod = importlib.util.module_from_spec(spec)
            sys.modules["_s3_load_mp_integ"] = mod
            spec.loader.exec_module(mod)
        return mod

    def test_two_prefixes_combined(self, substrate_url, reset_substrate, monkeypatch):
        # Each prefix probe returns 2 files → 4 total
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["f1.csv", "f2.csv"])
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler(
                {"source": "Research Data", "format": "csv", "prefixes": ["2023/", "2024/"]}, None
            )
        assert result["status"] == "loaded"
        assert result["fileCount"] == 4
        assert result["prefixCount"] == 2

    def test_single_prefix_no_prefixes_param(self, substrate_url, reset_substrate, monkeypatch):
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/f1.csv", "datasets/f2.csv", "datasets/f3.csv"])
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler(
                {"source": "Research Data", "format": "csv", "prefix": "datasets/"}, None
            )
        assert result["status"] == "loaded"
        assert result["fileCount"] == 3
        assert result["prefixCount"] == 1

    def test_dotdot_in_prefix_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        mod = self._reload(substrate_url, monkeypatch)
        result = mod.handler(
            {"source": "Research Data", "format": "csv", "prefixes": ["../etc/passwd"]}, None
        )
        assert "error" in result
        assert "access denied" in result["error"].lower()

    def test_prefixes_takes_priority_over_prefix(self, substrate_url, reset_substrate, monkeypatch):
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["f1.csv", "f2.csv"])
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler(
                {
                    "source": "Research Data",
                    "format": "csv",
                    "prefix": "old/",
                    "prefixes": ["2023/", "2024/"],
                },
                None,
            )
        assert result["status"] == "loaded"
        assert result["prefixCount"] == 2

    def test_sample_only_caps_total_files(self, substrate_url, reset_substrate, monkeypatch):
        # 20 files available per prefix; with 3 prefixes and sample_only,
        # max_per_prefix = 10 // 3 = 3 → total ≤ 9
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=[f"data/f{i}.csv" for i in range(20)])
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler(
                {
                    "source": "Research Data",
                    "format": "csv",
                    "prefixes": ["a/", "b/", "c/"],
                    "sample_only": True,
                },
                None,
            )
        assert result.get("fileCount", 0) <= 10

    def test_empty_prefixes_list_falls_back_to_no_prefix(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["f1.csv"])
        with patch.object(mod, "s3", mock_s3):
            result = mod.handler(
                {"source": "Research Data", "format": "csv", "prefixes": []}, None
            )
        assert result["status"] == "loaded"
        assert result["prefixCount"] == 1


# ===========================================================================
# s3_load — ClawsLookupTable persistence (OD-17)
# ===========================================================================

_CLAWS_LOOKUP_TABLE = "qs-claws-lookup-test"


@pytest.mark.integration
class TestS3LoadClawsLookupIntegration:
    """After successful QS creation, claws_source_id is written to ClawsLookupTable."""

    def _setup_ddb(self, substrate_url):
        ddb = boto3.client(
            "dynamodb",
            endpoint_url=substrate_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        ddb.create_table(
            TableName=_CLAWS_LOOKUP_TABLE,
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_CLAWS_LOOKUP_TABLE)
        return ddb

    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        env = {
            "MANIFEST_BUCKET": "qs-manifests-test",
            "QUICKSIGHT_ACCOUNT_ID": "123456789012",
            "QUICKSIGHT_REGION": "us-east-1",
            "SOURCES_CONFIG": json.dumps(SOURCES),
            "CLAWS_LOOKUP_TABLE": _CLAWS_LOOKUP_TABLE,
        }
        with patch.dict(os.environ, env):
            spec = importlib.util.spec_from_file_location(
                "_s3_load_claws_integ",
                os.path.join(REPO_ROOT, "lambdas", "s3-load", "handler.py"),
            )
            mod = importlib.util.module_from_spec(spec)
            sys.modules["_s3_load_claws_integ"] = mod
            spec.loader.exec_module(mod)
        return mod

    def test_claws_lookup_written_on_success(self, substrate_url, reset_substrate, monkeypatch):
        ddb = self._setup_ddb(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/2023.csv"])

        with patch.object(mod, "s3", mock_s3):
            result = mod.handler({"source": "Research Data", "format": "csv"}, None)

        assert result["status"] == "loaded"
        claws_id = result["claws_source_id"]
        resp = ddb.get_item(
            TableName=_CLAWS_LOOKUP_TABLE,
            Key={"source_id": {"S": claws_id}},
        )
        assert "Item" in resp
        assert "dataset_id" in resp["Item"]

    def test_claws_lookup_write_failure_is_nonfatal(
        self, substrate_url, reset_substrate, monkeypatch, fault_inject
    ):
        """DDB PutItem failure must not fail the load — Substrate fault injection."""
        self._setup_ddb(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)
        mock_s3 = _make_s3_paginator(keys=["datasets/2023.csv"])
        fault_inject("dynamodb", "PutItem", "InternalServerError", 500)

        with patch.object(mod, "s3", mock_s3):
            result = mod.handler({"source": "Research Data", "format": "csv"}, None)

        assert result["status"] == "loaded"


# ===========================================================================
# s3_browse + s3_preview integration tests (OD-14)
# ===========================================================================

S3_BROWSE_SOURCES = [
    {"label": "Research Data", "bucket": "uni-research-data",
     "prefix": "datasets/", "description": "test"},
]
CSV_CONTENT = b"id,name,score\n1,Alice,95\n2,Bob,87\n3,Carol,92\n"


def _reload_s3_handler(lambda_dir: str, alias: str, substrate_url: str, monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def _seed_s3_bucket(substrate_url: str):
    s3_client = boto3.client(
        "s3",
        endpoint_url=substrate_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    s3_client.create_bucket(Bucket="uni-research-data")
    s3_client.put_object(
        Bucket="uni-research-data",
        Key="datasets/2024/enrollment.csv",
        Body=CSV_CONTENT,
    )
    s3_client.put_object(
        Bucket="uni-research-data",
        Key="datasets/sample.csv",
        Body=CSV_CONTENT,
    )
    return s3_client


@pytest.mark.integration
class TestS3BrowseIntegration:
    """s3_browse handler integration tests against Substrate S3."""

    def test_no_args_returns_source_list(self, substrate_url, reset_substrate, monkeypatch):
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-browse", "_s3_browse_integ_a", substrate_url, monkeypatch)
        result = h.handler({}, None)
        assert result["count"] == 1
        assert result["sources"][0]["label"] == "Research Data"

    def test_browse_valid_source_returns_keys(self, substrate_url, reset_substrate, monkeypatch):
        _seed_s3_bucket(substrate_url)
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-browse", "_s3_browse_integ_b", substrate_url, monkeypatch)
        result = h.handler({"source": "Research Data"}, None)
        assert "error" not in result, f"Unexpected error: {result.get('error')}"
        assert result["count"] >= 1

    def test_unknown_source_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-browse", "_s3_browse_integ_c", substrate_url, monkeypatch)
        result = h.handler({"source": "Nonexistent Source"}, None)
        assert "error" in result

    def test_list_s3_error_returns_error(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        # Don't create the bucket — Substrate returns NoSuchBucket naturally,
        # which s3-browse handler catches and returns as an error dict.
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-browse", "_s3_browse_integ_d", substrate_url, monkeypatch)
        result = h.handler({"source": "Research Data"}, None)
        assert "error" in result


@pytest.mark.integration
class TestS3PreviewIntegration:
    """s3_preview handler integration tests against Substrate S3."""

    def test_csv_preview_returns_schema_and_rows(self, substrate_url, reset_substrate, monkeypatch):
        _seed_s3_bucket(substrate_url)
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-preview", "_s3_preview_integ_a", substrate_url, monkeypatch)
        result = h.handler({"source": "Research Data", "key": "datasets/sample.csv"}, None)
        assert result.get("format") == "csv", f"Got: {result}"
        assert len(result.get("columns", [])) == 3
        assert len(result.get("sample_rows", [])) >= 1

    def test_missing_key_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        _seed_s3_bucket(substrate_url)
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-preview", "_s3_preview_integ_b", substrate_url, monkeypatch)
        result = h.handler({"source": "Research Data", "key": "datasets/missing.csv"}, None)
        assert "error" in result

    def test_unknown_source_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-preview", "_s3_preview_integ_c", substrate_url, monkeypatch)
        result = h.handler({"source": "Unknown", "key": "x.csv"}, None)
        assert "error" in result

    def test_s3_error_on_head_returns_error(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        # Create the bucket but not the file — Substrate returns NoSuchKey naturally,
        # which s3-preview handler catches and returns as an error dict.
        boto3.client(
            "s3", endpoint_url=substrate_url, region_name="us-east-1",
            aws_access_key_id="test", aws_secret_access_key="test",
        ).create_bucket(Bucket="uni-research-data")
        monkeypatch.setenv("SOURCES_CONFIG", json.dumps(S3_BROWSE_SOURCES))
        h = _reload_s3_handler("s3-preview", "_s3_preview_integ_d", substrate_url, monkeypatch)
        result = h.handler({"source": "Research Data", "key": "datasets/missing-entirely.csv"}, None)
        assert "error" in result


# ===========================================================================
# s3_browse clearance filtering via source registry (Issue #16)
# ===========================================================================

_S3_BROWSE_REGISTRY_TABLE = "qs-data-source-registry-s3browse-test"


def _create_s3browse_registry(substrate_url):
    ddb = boto3.client("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")
    ddb.create_table(
        TableName=_S3_BROWSE_REGISTRY_TABLE,
        KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=_S3_BROWSE_REGISTRY_TABLE)
    return boto3.resource("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")


def _put_s3browse_source(resource, source_id, classification, bucket="test-bucket"):
    resource.Table(_S3_BROWSE_REGISTRY_TABLE).put_item(Item={
        "source_id": source_id,
        "type": "s3",
        "display_name": f"Source {source_id}",
        "description": f"A {classification} S3 source",
        "data_classification": classification,
        "connection_config": json.dumps({"bucket": bucket, "prefix": ""}),
    })


def _reload_s3browse_registry(substrate_url, monkeypatch, alias):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    monkeypatch.setenv("USE_SOURCE_REGISTRY", "true")
    monkeypatch.setenv("SOURCE_REGISTRY_TABLE", _S3_BROWSE_REGISTRY_TABLE)
    path = os.path.join(REPO_ROOT, "lambdas", "s3-browse", "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.integration
class TestS3BrowseClearanceFiltering:
    """Issue #16: s3_browse clearance filtering when USE_SOURCE_REGISTRY=true."""

    def test_public_clearance_hides_restricted_sources(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = _create_s3browse_registry(substrate_url)
        _put_s3browse_source(resource, "public-src", "public")
        _put_s3browse_source(resource, "restricted-src", "restricted")
        h = _reload_s3browse_registry(substrate_url, monkeypatch, "_s3b_clearance_a")

        result = h.handler({"caller_clearance": "public"}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "public-src" in labels
        assert "restricted-src" not in labels

    def test_public_clearance_hides_phi_sources(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = _create_s3browse_registry(substrate_url)
        _put_s3browse_source(resource, "public-src", "public")
        _put_s3browse_source(resource, "phi-src", "phi")
        h = _reload_s3browse_registry(substrate_url, monkeypatch, "_s3b_clearance_b")

        result = h.handler({"caller_clearance": "public"}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "public-src" in labels
        assert "phi-src" not in labels

    def test_phi_clearance_returns_all_levels(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = _create_s3browse_registry(substrate_url)
        for cls in ["public", "internal", "restricted", "phi"]:
            _put_s3browse_source(resource, f"{cls}-src", cls)
        h = _reload_s3browse_registry(substrate_url, monkeypatch, "_s3b_clearance_c")

        result = h.handler({"caller_clearance": "phi"}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "public-src" in labels
        assert "internal-src" in labels
        assert "restricted-src" in labels
        assert "phi-src" in labels

    def test_default_clearance_is_public(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        """Omitting caller_clearance defaults to public — most restrictive."""
        resource = _create_s3browse_registry(substrate_url)
        _put_s3browse_source(resource, "pub-src", "public")
        _put_s3browse_source(resource, "int-src", "internal")
        h = _reload_s3browse_registry(substrate_url, monkeypatch, "_s3b_clearance_d")

        result = h.handler({}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "pub-src" in labels
        assert "int-src" not in labels

    def test_restricted_clearance_includes_public_and_internal(
        self, substrate_url, reset_substrate, monkeypatch
    ):
        resource = _create_s3browse_registry(substrate_url)
        for cls in ["public", "internal", "restricted", "phi"]:
            _put_s3browse_source(resource, f"src-{cls}", cls)
        h = _reload_s3browse_registry(substrate_url, monkeypatch, "_s3b_clearance_e")

        result = h.handler({"caller_clearance": "restricted"}, None)
        labels = [s["label"] for s in result["sources"]]
        assert "src-public" in labels
        assert "src-internal" in labels
        assert "src-restricted" in labels
        assert "src-phi" not in labels
