"""
Unit tests for snowflake-browse/handler.py and snowflake-preview/handler.py.

Uses unittest.mock.patch to mock urllib.request.urlopen and Secrets Manager.
"""

import importlib
import importlib.util
import json
import os
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

_SNOWFLAKE_SECRET = {
    "account": "myorg-myaccount",
    "user": "testuser",
    "password": "testpass",
    "warehouse": "COMPUTE_WH",
    "role": "SYSADMIN",
    "database": "MY_DB",
}

_BROWSE_RESULT = {
    "data": [
        ["PUBLIC", "ORDERS", "BASE TABLE", "10000"],
        ["PUBLIC", "CUSTOMERS", "BASE TABLE", "5000"],
        ["ANALYTICS", "SUMMARY", "BASE TABLE", "200"],
    ]
}

_PREVIEW_RESULT = {
    "resultSetMetaData": {
        "rowType": [
            {"name": "id"},
            {"name": "name"},
            {"name": "amount"},
        ]
    },
    "data": [
        ["1", "Alice", "99.99"],
        ["2", "Bob", "49.50"],
    ],
}


def _make_urlopen_response(body: dict):
    """Create a mock urllib response context manager."""
    encoded = json.dumps(body).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = encoded
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _load_browse():
    path = os.path.join(REPO_ROOT, "lambdas", "snowflake-browse", "handler.py")
    alias = "_sf_browse_handler"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"SNOWFLAKE_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:sf"}):
        spec.loader.exec_module(mod)
    return mod


def _load_preview():
    path = os.path.join(REPO_ROOT, "lambdas", "snowflake-preview", "handler.py")
    alias = "_sf_preview_handler"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"SNOWFLAKE_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:sf"}):
        spec.loader.exec_module(mod)
    return mod


sf_browse = _load_browse()
sf_preview = _load_preview()


def _mock_secret(mod):
    """Patch secrets_client.get_secret_value on the module to return test config."""
    mock_sc = MagicMock()
    mock_sc.get_secret_value.return_value = {
        "SecretString": json.dumps(_SNOWFLAKE_SECRET)
    }
    return patch.object(mod, "secrets_client", mock_sc)


# ---------------------------------------------------------------------------
# snowflake-browse tests
# ---------------------------------------------------------------------------

class TestSnowflakeBrowse:
    def test_happy_path_returns_table_list(self):
        url_resp = _make_urlopen_response(_BROWSE_RESULT)
        with _mock_secret(sf_browse):
            with patch("urllib.request.urlopen", return_value=url_resp):
                result = sf_browse.handler({"source_id": "sf-prod"}, None)
        assert "error" not in result
        assert result["source_id"] == "sf-prod"
        assert result["count"] == 3
        assert result["tables"][0]["schema"] == "PUBLIC"
        assert result["tables"][0]["name"] == "ORDERS"
        assert result["tables"][0]["row_count"] == 10000

    def test_api_error_returns_error_dict(self):
        import urllib.error
        http_err = urllib.error.HTTPError(
            url="https://test.snowflakecomputing.com/api/v2/statements",
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=BytesIO(b'{"message": "Invalid credentials"}'),
        )
        with _mock_secret(sf_browse):
            with patch("urllib.request.urlopen", side_effect=http_err):
                result = sf_browse.handler({"source_id": "sf-prod"}, None)
        assert "error" in result
        assert "Snowflake API error" in result["error"]

    def test_missing_secret_returns_error(self):
        """When SNOWFLAKE_SECRET_ARN is empty, should return error immediately."""
        path = os.path.join(REPO_ROOT, "lambdas", "snowflake-browse", "handler.py")
        alias = "_sf_browse_no_secret"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        with patch.dict(os.environ, {"SNOWFLAKE_SECRET_ARN": ""}):
            spec.loader.exec_module(mod)
        result = mod.handler({"source_id": "sf-empty"}, None)
        assert "error" in result
        assert "not configured" in result["error"]

    def test_source_id_required(self):
        with _mock_secret(sf_browse):
            result = sf_browse.handler({}, None)
        assert "error" in result
        assert "source_id" in result["error"]

    def test_empty_table_list_on_no_data(self):
        url_resp = _make_urlopen_response({"data": []})
        with _mock_secret(sf_browse):
            with patch("urllib.request.urlopen", return_value=url_resp):
                result = sf_browse.handler({"source_id": "sf-empty-db"}, None)
        assert result["count"] == 0
        assert result["tables"] == []


# ---------------------------------------------------------------------------
# snowflake-preview tests
# ---------------------------------------------------------------------------

class TestSnowflakePreview:
    def test_happy_path_returns_sample_rows_and_columns(self):
        url_resp = _make_urlopen_response(_PREVIEW_RESULT)
        with _mock_secret(sf_preview):
            with patch("urllib.request.urlopen", return_value=url_resp):
                result = sf_preview.handler(
                    {"source_id": "sf-prod", "schema": "PUBLIC", "table": "ORDERS"},
                    None,
                )
        assert "error" not in result
        assert result["source_id"] == "sf-prod"
        assert result["schema"] == "PUBLIC"
        assert result["table"] == "ORDERS"
        assert result["columns"] == ["id", "name", "amount"]
        assert len(result["sample_rows"]) == 2
        assert result["sample_rows"][0]["name"] == "Alice"
        assert result["format"] == "snowflake"

    def test_invalid_table_name_sql_injection_returns_error(self):
        with _mock_secret(sf_preview):
            result = sf_preview.handler(
                {"source_id": "sf-prod", "schema": "PUBLIC", "table": "ORDERS; DROP TABLE users--"},
                None,
            )
        assert "error" in result
        assert "invalid table name" in result["error"]

    def test_invalid_schema_name_returns_error(self):
        with _mock_secret(sf_preview):
            result = sf_preview.handler(
                {"source_id": "sf-prod", "schema": "PUBLIC'; DELETE FROM x--", "table": "ORDERS"},
                None,
            )
        assert "error" in result
        assert "invalid table name" in result["error"]

    def test_max_rows_capped_at_25(self):
        # Build result with 25 rows (the API call should use LIMIT 25)
        rows = [[str(i), f"name_{i}", str(i * 10.0)] for i in range(25)]
        url_resp = _make_urlopen_response({
            "resultSetMetaData": {"rowType": [{"name": "id"}, {"name": "name"}, {"name": "val"}]},
            "data": rows,
        })
        with _mock_secret(sf_preview):
            with patch("urllib.request.urlopen", return_value=url_resp) as mock_url:
                result = sf_preview.handler(
                    {"source_id": "sf-prod", "schema": "PUBLIC", "table": "ORDERS", "max_rows": 100},
                    None,
                )
        # Verify the SQL used LIMIT 25
        call_args = mock_url.call_args
        request_obj = call_args[0][0]
        body = json.loads(request_obj.data.decode("utf-8"))
        assert "LIMIT 25" in body["statement"]
        assert result["row_count"] == 25

    def test_source_id_required(self):
        with _mock_secret(sf_preview):
            result = sf_preview.handler({"schema": "PUBLIC", "table": "ORDERS"}, None)
        assert "error" in result

    def test_schema_required(self):
        with _mock_secret(sf_preview):
            result = sf_preview.handler({"source_id": "sf-prod", "table": "ORDERS"}, None)
        assert "error" in result

    def test_table_required(self):
        with _mock_secret(sf_preview):
            result = sf_preview.handler({"source_id": "sf-prod", "schema": "PUBLIC"}, None)
        assert "error" in result

    def test_missing_secret_returns_error(self):
        path = os.path.join(REPO_ROOT, "lambdas", "snowflake-preview", "handler.py")
        alias = "_sf_preview_no_secret"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        with patch.dict(os.environ, {"SNOWFLAKE_SECRET_ARN": ""}):
            spec.loader.exec_module(mod)
        result = mod.handler(
            {"source_id": "sf-empty", "schema": "PUBLIC", "table": "T"},
            None,
        )
        assert "error" in result
        assert "not configured" in result["error"]
