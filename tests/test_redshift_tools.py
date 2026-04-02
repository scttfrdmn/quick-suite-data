"""
Unit tests for redshift-browse/handler.py and redshift-preview/handler.py.

Uses unittest.mock.MagicMock to mock boto3 redshift-data client and Secrets Manager.
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

_REDSHIFT_SECRET = {
    "workgroup": "my-workgroup",
    "database": "mydb",
    "secret_arn": "arn:aws:secretsmanager:us-east-1:123:secret:redshift-creds",
}

_STATEMENT_ID = "abc-123-stmt"

_TABLES_RECORDS = [
    [{"stringValue": "public"}, {"stringValue": "orders"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "public"}, {"stringValue": "customers"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "analytics"}, {"stringValue": "summary"}, {"stringValue": "BASE TABLE"}],
]

_PREVIEW_COLUMN_METADATA = [
    {"name": "id"},
    {"name": "name"},
    {"name": "amount"},
]

_PREVIEW_RECORDS = [
    [{"stringValue": "1"}, {"stringValue": "Alice"}, {"stringValue": "99.99"}],
    [{"stringValue": "2"}, {"stringValue": "Bob"}, {"stringValue": "49.50"}],
]


def _load_browse():
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
    alias = "_rs_browse_handler"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:rs"}):
        spec.loader.exec_module(mod)
    return mod


def _load_preview():
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-preview", "handler.py")
    alias = "_rs_preview_handler"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:rs"}):
        spec.loader.exec_module(mod)
    return mod


rs_browse = _load_browse()
rs_preview = _load_preview()


def _mock_clients(mod, records, column_metadata=None, final_status="FINISHED"):
    """
    Patch secrets_client and redshift_data on the module with mock responses.

    Returns (mock_secrets, mock_redshift).
    """
    mock_sc = MagicMock()
    mock_sc.get_secret_value.return_value = {
        "SecretString": json.dumps(_REDSHIFT_SECRET)
    }

    mock_rd = MagicMock()
    mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
    mock_rd.describe_statement.return_value = {"Status": final_status, "Error": ""}
    result_resp = {"Records": records}
    if column_metadata is not None:
        result_resp["ColumnMetadata"] = column_metadata
    mock_rd.get_statement_result.return_value = result_resp

    return (
        patch.object(mod, "secrets_client", mock_sc),
        patch.object(mod, "redshift_data", mock_rd),
    )


# ---------------------------------------------------------------------------
# redshift-browse tests
# ---------------------------------------------------------------------------

class TestRedshiftBrowse:
    def test_happy_path_returns_table_list(self):
        p_sc, p_rd = _mock_clients(rs_browse, _TABLES_RECORDS)
        with p_sc, p_rd:
            result = rs_browse.handler({"source_id": "rs-prod"}, None)
        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["count"] == 3
        assert result["tables"][0] == {"schema": "public", "name": "orders", "type": "BASE TABLE"}
        assert result["workgroup"] == "my-workgroup"
        assert result["database"] == "mydb"

    def test_query_times_out_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {
            "SecretString": json.dumps(_REDSHIFT_SECRET)
        }
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        # Always return STARTED (never terminal)
        mock_rd.describe_statement.return_value = {"Status": "STARTED"}

        with patch.object(rs_browse, "secrets_client", mock_sc):
            with patch.object(rs_browse, "redshift_data", mock_rd):
                with patch.object(rs_browse, "_POLL_MAX", 2):
                    with patch("time.sleep"):
                        result = rs_browse.handler({"source_id": "rs-slow"}, None)
        assert "error" in result
        assert "timed out" in result["error"]

    def test_failed_status_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {
            "SecretString": json.dumps(_REDSHIFT_SECRET)
        }
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {
            "Status": "FAILED",
            "Error": "Permission denied on table information_schema.tables",
        }

        with patch.object(rs_browse, "secrets_client", mock_sc):
            with patch.object(rs_browse, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = rs_browse.handler({"source_id": "rs-fail"}, None)
        assert "error" in result
        assert "Redshift query failed" in result["error"]
        assert "Permission denied" in result["error"]

    def test_source_id_required(self):
        result = rs_browse.handler({}, None)
        assert "error" in result

    def test_not_configured_when_no_secret_arn(self):
        path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
        alias = "_rs_browse_no_secret"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": ""}):
            spec.loader.exec_module(mod)
        result = mod.handler({"source_id": "rs-no-config"}, None)
        assert "error" in result
        assert "not configured" in result["error"]


# ---------------------------------------------------------------------------
# redshift-preview tests
# ---------------------------------------------------------------------------

class TestRedshiftPreview:
    def test_happy_path_returns_sample_rows_and_columns(self):
        p_sc, p_rd = _mock_clients(rs_preview, _PREVIEW_RECORDS, _PREVIEW_COLUMN_METADATA)
        with p_sc, p_rd:
            with patch("time.sleep"):
                result = rs_preview.handler(
                    {"source_id": "rs-prod", "schema": "public", "table": "orders"},
                    None,
                )
        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["schema"] == "public"
        assert result["table"] == "orders"
        assert result["columns"] == ["id", "name", "amount"]
        assert len(result["sample_rows"]) == 2
        assert result["sample_rows"][0]["name"] == "Alice"
        assert result["format"] == "redshift"

    def test_invalid_table_name_returns_error(self):
        result = rs_preview.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders; DROP TABLE foo--"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    def test_invalid_schema_name_returns_error(self):
        result = rs_preview.handler(
            {"source_id": "rs-prod", "schema": "public'; DELETE FROM x--", "table": "orders"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    def test_max_rows_capped_at_25(self):
        rows = [[{"stringValue": str(i)}, {"stringValue": f"n{i}"}] for i in range(25)]
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {
            "SecretString": json.dumps(_REDSHIFT_SECRET)
        }
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {"Status": "FINISHED"}
        mock_rd.get_statement_result.return_value = {
            "ColumnMetadata": [{"name": "id"}, {"name": "name"}],
            "Records": rows,
        }
        with patch.object(rs_preview, "secrets_client", mock_sc):
            with patch.object(rs_preview, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = rs_preview.handler(
                        {
                            "source_id": "rs-prod",
                            "schema": "public",
                            "table": "orders",
                            "max_rows": 100,
                        },
                        None,
                    )
        # Verify the SQL sent used LIMIT 25
        call_kwargs = mock_rd.execute_statement.call_args[1]
        assert "LIMIT 25" in call_kwargs["Sql"]
        assert result["row_count"] <= 25

    def test_source_id_required(self):
        result = rs_preview.handler({"schema": "public", "table": "orders"}, None)
        assert "error" in result

    def test_schema_required(self):
        result = rs_preview.handler({"source_id": "rs-prod", "table": "orders"}, None)
        assert "error" in result

    def test_table_required(self):
        result = rs_preview.handler({"source_id": "rs-prod", "schema": "public"}, None)
        assert "error" in result

    def test_aborted_status_returns_error(self):
        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {
            "SecretString": json.dumps(_REDSHIFT_SECRET)
        }
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": _STATEMENT_ID}
        mock_rd.describe_statement.return_value = {"Status": "ABORTED", "Error": "Query aborted"}

        with patch.object(rs_preview, "secrets_client", mock_sc):
            with patch.object(rs_preview, "redshift_data", mock_rd):
                with patch("time.sleep"):
                    result = rs_preview.handler(
                        {"source_id": "rs-prod", "schema": "public", "table": "orders"},
                        None,
                    )
        assert "error" in result
        assert "Redshift query failed" in result["error"]
