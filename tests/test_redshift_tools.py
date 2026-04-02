"""
Integration tests for redshift-browse/handler.py and redshift-preview/handler.py.

Uses Substrate (real AWS emulator) for Secrets Manager and the Redshift Data API.
Result seeding and status override use the Substrate v0.51.0 control plane:
  POST /v1/redshift-data/results  — seed GetStatementResult rows (wildcard or SQL-specific)
  POST /v1/redshift-data/status   — set statement status (FINISHED/FAILED/ABORTED/STARTED)

One test (test_max_rows_capped_at_25) remains MagicMock because it verifies that the
handler constructs SQL with the correct LIMIT value — SQL string inspection is not
possible via Substrate (the emulator does not apply SQL semantics to results).
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
import requests

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

_SECRET_NAME = "test/redshift-creds"
_REDSHIFT_SECRET = {
    "workgroup": "my-workgroup",
    "database": "mydb",
    "secret_arn": "arn:aws:secretsmanager:us-east-1:123:secret:redshift-inner-creds",
}

_TABLES_RECORDS = [
    [{"stringValue": "public"}, {"stringValue": "orders"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "public"}, {"stringValue": "customers"}, {"stringValue": "BASE TABLE"}],
    [{"stringValue": "analytics"}, {"stringValue": "summary"}, {"stringValue": "BASE TABLE"}],
]

_PREVIEW_COLUMN_METADATA = [{"name": "id"}, {"name": "name"}, {"name": "amount"}]
_PREVIEW_RECORDS = [
    [{"stringValue": "1"}, {"stringValue": "Alice"}, {"stringValue": "99.99"}],
    [{"stringValue": "2"}, {"stringValue": "Bob"}, {"stringValue": "49.50"}],
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_secret(substrate_url):
    sm = boto3.client("secretsmanager", endpoint_url=substrate_url, region_name="us-east-1")
    sm.create_secret(Name=_SECRET_NAME, SecretString=json.dumps(_REDSHIFT_SECRET))
    return f"arn:aws:secretsmanager:us-east-1:123456789012:secret:{_SECRET_NAME}"


def _seed_redshift_result(substrate_url, records, column_metadata=None):
    """Seed a wildcard GetStatementResult response via Substrate v0.51.0 control plane.

    records: list of rows, each row a list of {"stringValue": "..."} dicts.
    column_metadata: list of {"name": "col_name"} dicts (optional).
    """
    result_body = {
        "ColumnMetadata": [
            {"name": col["name"], "typeName": "varchar"} for col in (column_metadata or [])
        ],
        "Records": records,
    }
    resp = requests.post(
        f"{substrate_url}/v1/redshift-data/results",
        json={"result": result_body},  # omit "sql" → defaults to "*" wildcard
    )
    assert resp.status_code == 200, f"seed result failed: {resp.text}"


def _set_redshift_status(substrate_url, status, error_message=""):
    """Override the default statement status via Substrate v0.51.0 control plane."""
    resp = requests.post(
        f"{substrate_url}/v1/redshift-data/status",
        json={"status": status, "errorMessage": error_message},
    )
    assert resp.status_code == 200, f"set status failed: {resp.text}"


def _reload_browse(substrate_url, monkeypatch, secret_arn):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    monkeypatch.setenv("REDSHIFT_SECRET_ARN", secret_arn)
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
    alias = "_rs_browse_integ"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def _reload_preview(substrate_url, monkeypatch, secret_arn):
    monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
    monkeypatch.setenv("REDSHIFT_SECRET_ARN", secret_arn)
    path = os.path.join(REPO_ROOT, "lambdas", "redshift-preview", "handler.py")
    alias = "_rs_preview_integ"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# redshift-browse — Substrate integration tests
# ---------------------------------------------------------------------------

class TestRedshiftBrowse:
    """Integration tests using real Substrate Secrets Manager + Redshift Data API."""

    @pytest.mark.integration
    def test_happy_path_returns_valid_structure(self, substrate_url, reset_substrate, monkeypatch):
        """Handler completes the full SM → execute → describe → get_result round-trip."""
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)

        result = mod.handler({"source_id": "rs-prod"}, None)

        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["workgroup"] == "my-workgroup"
        assert result["database"] == "mydb"
        assert isinstance(result["tables"], list)
        assert isinstance(result["count"], int)

    @pytest.mark.integration
    def test_source_id_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_not_configured_when_no_secret_arn(self, substrate_url, reset_substrate, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        monkeypatch.setenv("REDSHIFT_SECRET_ARN", "")
        path = os.path.join(REPO_ROOT, "lambdas", "redshift-browse", "handler.py")
        alias = "_rs_browse_no_secret"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        result = mod.handler({"source_id": "rs-no-config"}, None)
        assert "error" in result
        assert "not configured" in result["error"]


# ---------------------------------------------------------------------------
# redshift-browse — result-parsing and error-state tests (Substrate v0.51.0)
# ---------------------------------------------------------------------------

class TestRedshiftBrowseResultParsing:
    """Tests requiring specific result rows or non-FINISHED status.

    All use Substrate v0.51.0 control plane for result seeding and status override.
    """

    @pytest.mark.integration
    def test_happy_path_parses_rows_correctly(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        _seed_redshift_result(substrate_url, _TABLES_RECORDS)
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)

        result = mod.handler({"source_id": "rs-prod"}, None)

        assert result["count"] == 3
        assert result["tables"][0] == {"schema": "public", "name": "orders", "type": "BASE TABLE"}

    @pytest.mark.integration
    def test_query_times_out_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        _set_redshift_status(substrate_url, "STARTED")
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)
        mod._POLL_MAX = 2  # cap at 2 iterations so the test completes quickly

        with patch("time.sleep"):
            result = mod.handler({"source_id": "rs-slow"}, None)

        assert "error" in result
        assert "timed out" in result["error"]

    @pytest.mark.integration
    def test_failed_status_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        _set_redshift_status(
            substrate_url, "FAILED",
            error_message="Permission denied on table information_schema.tables",
        )
        mod = _reload_browse(substrate_url, monkeypatch, secret_arn)

        result = mod.handler({"source_id": "rs-fail"}, None)

        assert "error" in result
        assert "Redshift query failed" in result["error"]
        assert "Permission denied" in result["error"]


# ---------------------------------------------------------------------------
# redshift-preview — Substrate integration tests
# ---------------------------------------------------------------------------

class TestRedshiftPreview:
    """Integration tests using real Substrate Secrets Manager + Redshift Data API."""

    @pytest.mark.integration
    def test_happy_path_returns_valid_structure(self, substrate_url, reset_substrate, monkeypatch):
        """Full SM → execute → describe → get_result round-trip."""
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)

        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
        )

        assert "error" not in result
        assert result["source_id"] == "rs-prod"
        assert result["schema"] == "public"
        assert result["table"] == "orders"
        assert isinstance(result["columns"], list)
        assert isinstance(result["sample_rows"], list)
        assert result["format"] == "redshift"

    @pytest.mark.integration
    def test_invalid_table_name_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders; DROP TABLE foo--"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    @pytest.mark.integration
    def test_invalid_schema_name_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public'; DELETE FROM x--", "table": "orders"},
            None,
        )
        assert "error" in result
        assert "invalid table name" in result["error"]

    @pytest.mark.integration
    def test_source_id_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"schema": "public", "table": "orders"}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_schema_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"source_id": "rs-prod", "table": "orders"}, None)
        assert "error" in result

    @pytest.mark.integration
    def test_table_required(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)
        result = mod.handler({"source_id": "rs-prod", "schema": "public"}, None)
        assert "error" in result


# ---------------------------------------------------------------------------
# redshift-preview — result-parsing and error-state tests (Substrate v0.51.0)
# ---------------------------------------------------------------------------

class TestRedshiftPreviewResultParsing:
    """Tests requiring specific row data or non-FINISHED status.

    Uses Substrate v0.51.0 control plane for result seeding and status override,
    except test_max_rows_capped_at_25 which uses MagicMock to inspect the SQL
    argument — Substrate does not enforce SQL semantics and cannot verify LIMIT.
    """

    @pytest.mark.integration
    def test_happy_path_parses_rows_and_columns(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        _seed_redshift_result(substrate_url, _PREVIEW_RECORDS, _PREVIEW_COLUMN_METADATA)
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)

        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
        )

        assert result["columns"] == ["id", "name", "amount"]
        assert len(result["sample_rows"]) == 2
        assert result["sample_rows"][0]["name"] == "Alice"
        assert result["format"] == "redshift"

    def test_max_rows_capped_at_25(self):
        """SQL construction test: max_rows=100 must produce LIMIT 25, not LIMIT 100.

        Uses MagicMock to inspect the Sql kwarg passed to execute_statement because
        Substrate does not apply SQL semantics — it returns all seeded rows regardless
        of the LIMIT clause, so the cap cannot be verified via Substrate round-trip.
        """
        path = os.path.join(REPO_ROOT, "lambdas", "redshift-preview", "handler.py")
        alias = "_rs_preview_sql_cap"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        with patch.dict(os.environ, {"REDSHIFT_SECRET_ARN": "arn:test"}):
            spec.loader.exec_module(mod)

        mock_sc = MagicMock()
        mock_sc.get_secret_value.return_value = {"SecretString": json.dumps(_REDSHIFT_SECRET)}
        mock_rd = MagicMock()
        mock_rd.execute_statement.return_value = {"Id": "stmt-cap-test"}
        mock_rd.describe_statement.return_value = {"Status": "FINISHED"}
        mock_rd.get_statement_result.return_value = {
            "ColumnMetadata": [{"name": "id"}, {"name": "name"}],
            "Records": [[{"stringValue": str(i)}, {"stringValue": f"n{i}"}] for i in range(25)],
        }
        with patch.object(mod, "secrets_client", mock_sc):
            with patch.object(mod, "redshift_data", mock_rd):
                result = mod.handler(
                    {"source_id": "rs-prod", "schema": "public", "table": "orders",
                     "max_rows": 100},
                    None,
                )
        call_kwargs = mock_rd.execute_statement.call_args[1]
        assert "LIMIT 25" in call_kwargs["Sql"]
        assert result["row_count"] <= 25

    @pytest.mark.integration
    def test_aborted_status_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        secret_arn = _seed_secret(substrate_url)
        _set_redshift_status(substrate_url, "ABORTED", error_message="Query aborted")
        mod = _reload_preview(substrate_url, monkeypatch, secret_arn)

        result = mod.handler(
            {"source_id": "rs-prod", "schema": "public", "table": "orders"}, None
        )

        assert "error" in result
        assert "Redshift query failed" in result["error"]
