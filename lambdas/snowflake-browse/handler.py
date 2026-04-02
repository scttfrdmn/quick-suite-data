"""
snowflake_browse: Browse tables in a Snowflake data source.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses urllib.request and base64 from stdlib only. No snowflake-connector-python.
"""

import base64
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")

SNOWFLAKE_SECRET_ARN = os.environ.get("SNOWFLAKE_SECRET_ARN", "")


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
    Browse tables in a Snowflake data source.

    Tool arguments:
    - source_id: str (required) — identifier for the Snowflake source
    - schema: str (optional, default "PUBLIC") — schema to filter
    - database: str (optional) — database override
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

    config = _get_snowflake_config()
    if config is None:
        return {"error": "Snowflake source not configured"}

    database = (event.get("database") or config.get("database", "")).strip()

    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT "
        "FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE = 'BASE TABLE' "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )

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

    # Parse result rows — Snowflake SQL API v2 returns {"data": [[col, ...], ...]}
    rows = result.get("data", [])
    tables = []
    for row in rows:
        if len(row) >= 3:
            tables.append({
                "schema": row[0] if row[0] is not None else "",
                "name": row[1] if row[1] is not None else "",
                "row_count": int(row[3]) if len(row) >= 4 and row[3] is not None else 0,
            })

    return {
        "source_id": source_id,
        "account": config.get("account", ""),
        "database": database,
        "schema": event.get("schema", "PUBLIC"),
        "tables": tables,
        "count": len(tables),
    }
