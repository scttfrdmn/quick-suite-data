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

def _mock_qs_s3load():
    """QS mock that completes create_data_source polling on first iteration."""
    qs = MagicMock()
    qs.create_data_source.return_value = {
        "DataSourceId": "test-source",
        "Arn": "arn:aws:quicksight:us-east-1:123456789012:datasource/test-source",
    }
    qs.describe_data_source.return_value = {"DataSource": {"Status": "CREATION_SUCCESSFUL"}}
    qs.create_data_set.return_value = {}
    return qs


class TestS3LoadMultiPrefix:
    """Tests for the prefixes list parameter added in v0.4.0."""

    def test_two_prefixes_combined(self):
        # Each prefix probe returns 2 files → 4 total
        mock_s3 = _make_s3_paginator(keys=["f1.csv", "f2.csv"])
        mock_qs = _mock_qs_s3load()
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs), \
             patch("time.sleep"):
            result = _s3_load.handler(
                {"source": "Research Data", "format": "csv", "prefixes": ["2023/", "2024/"]}, None
            )
        assert result["status"] == "loaded"
        assert result["fileCount"] == 4
        assert result["prefixCount"] == 2

    def test_single_prefix_no_prefixes_param(self):
        mock_s3 = _make_s3_paginator(keys=["datasets/f1.csv", "datasets/f2.csv", "datasets/f3.csv"])
        mock_qs = _mock_qs_s3load()
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs), \
             patch("time.sleep"):
            result = _s3_load.handler(
                {"source": "Research Data", "format": "csv", "prefix": "datasets/"}, None
            )
        assert result["status"] == "loaded"
        assert result["fileCount"] == 3
        assert result["prefixCount"] == 1

    def test_dotdot_in_prefix_returns_error(self):
        with patch.object(_s3_load, "_sources", SOURCES):
            result = _s3_load.handler(
                {"source": "Research Data", "format": "csv", "prefixes": ["../etc/passwd"]}, None
            )
        assert "error" in result
        assert "access denied" in result["error"].lower()

    def test_prefixes_takes_priority_over_prefix(self):
        mock_s3 = _make_s3_paginator(keys=["f1.csv", "f2.csv"])
        mock_qs = _mock_qs_s3load()
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs), \
             patch("time.sleep"):
            result = _s3_load.handler(
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

    def test_sample_only_caps_total_files(self):
        # 20 files available per prefix; with 3 prefixes and sample_only,
        # max_per_prefix = 10 // 3 = 3 → total ≤ 9
        mock_s3 = _make_s3_paginator(keys=[f"data/f{i}.csv" for i in range(20)])
        mock_qs = _mock_qs_s3load()
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs), \
             patch("time.sleep"):
            result = _s3_load.handler(
                {
                    "source": "Research Data",
                    "format": "csv",
                    "prefixes": ["a/", "b/", "c/"],
                    "sample_only": True,
                },
                None,
            )
        assert result.get("fileCount", 0) <= 10

    def test_empty_prefixes_list_falls_back_to_no_prefix(self):
        # Empty list → extra_prefixes = [] → handler uses extra_prefixes = [single_prefix]
        mock_s3 = _make_s3_paginator(keys=["f1.csv"])
        mock_qs = _mock_qs_s3load()
        with patch.object(_s3_load, "_sources", SOURCES), \
             patch.object(_s3_load, "s3", mock_s3), \
             patch.object(_s3_load, "quicksight", mock_qs), \
             patch("time.sleep"):
            result = _s3_load.handler(
                {"source": "Research Data", "format": "csv", "prefixes": []}, None
            )
        assert result["status"] == "loaded"
        assert result["prefixCount"] == 1
