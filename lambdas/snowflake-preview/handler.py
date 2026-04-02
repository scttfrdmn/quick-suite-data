"""
snowflake_preview: Sample rows from a Snowflake table.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses urllib.request and base64 from stdlib only. No snowflake-connector-python.
"""

import base64
import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")

SNOWFLAKE_SECRET_ARN = os.environ.get("SNOWFLAKE_SECRET_ARN", "")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


def _get_snowflake_config() -> dict | None:
    """Fetch Snowflake connection config from Secrets Manager. Returns None if not configured."""
    if not SNOWFLAKE_SECRET_ARN:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=SNOWFLAKE_SECRET_ARN)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.error(json.dumps({"error": "secrets_manager_error", "detail": str(e)}))
        return None


def _snowflake_execute(config: dict, statement: str) -> dict:
    """
    Execute a SQL statement via the Snowflake SQL API v2.

    Returns the parsed JSON response body or raises an exception.
    """
    account = config["account"]
    user = config["user"]
    password = config["password"]
    warehouse = config.get("warehouse", "")
    role = config.get("role", "")
    database = config.get("database", "")

    url = f"https://{account}.snowflakecomputing.com/api/v2/statements"
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()

    body = {
        "statement": statement,
        "warehouse": warehouse,
        "role": role,
        "database": database,
    }
    body_bytes = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Basic {credentials}",
        },
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def handler(event: dict, context: Any) -> dict:
    """
    Sample rows from a Snowflake table.

    Tool arguments:
    - source_id: str (required) — identifier for the Snowflake source
    - schema: str (required) — table schema
    - table: str (required) — table name
    - max_rows: int (optional, default 5, max 25)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    source_id = (event.get("source_id") or "").strip()
    if not source_id:
        return {"error": "source_id is required"}

    schema = (event.get("schema") or "").strip()
    if not schema:
        return {"error": "schema is required"}

    table = (event.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}

    # Sanitize schema and table names — alphanumeric + underscore only
    if not _SAFE_IDENTIFIER.match(schema):
        return {"error": "invalid table name"}
    if not _SAFE_IDENTIFIER.match(table):
        return {"error": "invalid table name"}

    try:
        max_rows = int(event.get("max_rows", 5))
    except (TypeError, ValueError):
        max_rows = 5
    max_rows = min(max(1, max_rows), 25)

    config = _get_snowflake_config()
    if config is None:
        return {"error": "Snowflake source not configured"}

    sql = f"SELECT * FROM {schema}.{table} LIMIT {max_rows}"

    try:
        result = _snowflake_execute(config, sql)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            pass
        logger.error(json.dumps({"snowflake_http_error": e.code, "body": body}))
        return {"error": f"Snowflake API error: {body or str(e)}"}
    except Exception as e:
        logger.error(json.dumps({"snowflake_error": str(e)}))
        return {"error": f"Snowflake API error: {e}"}

    # Parse result — columns from resultSetMetaData, rows from data
    metadata = result.get("resultSetMetaData", {})
    row_type = metadata.get("rowType", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(row_type)]

    rows = result.get("data", [])

    # Infer columns from first row if metadata not available
    if not columns and rows:
        columns = [f"col{i}" for i in range(len(rows[0]))]

    sample_rows = []
    for row in rows:
        sample_rows.append(dict(zip(columns, row)))

    return {
        "source_id": source_id,
        "schema": schema,
        "table": table,
        "columns": columns,
        "sample_rows": sample_rows,
        "row_count": len(sample_rows),
        "format": "snowflake",
    }
