"""
redshift_browse: Browse tables in a Redshift Serverless data source.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses boto3 redshift-data client.
"""

import json
import logging
import os
import time
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
redshift_data = boto3.client("redshift-data")

REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")

_POLL_MAX = 30
_POLL_INTERVAL = 1


def _get_redshift_config() -> dict | None:
    """Fetch Redshift connection config from Secrets Manager. Returns None if not configured."""
    if not REDSHIFT_SECRET_ARN:
        return None
    try:
        resp = secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.error(json.dumps({"error": "secrets_manager_error", "detail": str(e)}))
        return None


def _poll_statement(statement_id: str) -> dict:
    """
    Poll describe_statement until terminal status or timeout.

    Returns the final describe_statement response or raises on timeout.
    """
    for _ in range(_POLL_MAX):
        resp = redshift_data.describe_statement(Id=statement_id)
        status = resp.get("Status", "")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return resp
        time.sleep(_POLL_INTERVAL)
    raise TimeoutError(f"Redshift statement {statement_id} did not complete in {_POLL_MAX}s")


def handler(event: dict, context: Any) -> dict:
    """
    Browse tables in a Redshift Serverless data source.

    Tool arguments:
    - source_id: str (required) — identifier for the Redshift source
    - schema: str (optional) — schema to filter; list all schemas if omitted
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

    config = _get_redshift_config()
    if config is None:
        return {"error": "Redshift source not configured"}

    workgroup = config.get("workgroup", "")
    database = config.get("database", "")
    secret_arn = config.get("secret_arn", "")

    sql = (
        "SELECT table_schema, table_name, table_type "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY table_schema, table_name"
    )

    try:
        exec_resp = redshift_data.execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            SecretArn=secret_arn,
            Sql=sql,
        )
        statement_id = exec_resp["Id"]
    except Exception as e:
        logger.error(json.dumps({"redshift_error": str(e)}))
        return {"error": "Redshift query could not be submitted"}  # sanitized (#59)

    try:
        final = _poll_statement(statement_id)
    except TimeoutError:
        return {"error": "Redshift query timed out"}
    except Exception as e:
        logger.error(json.dumps({"poll_error": str(e)}))
        return {"error": "Redshift query status check failed"}  # sanitized (#59)

    status = final.get("Status", "")
    if status in ("FAILED", "ABORTED"):
        logger.error(json.dumps({"redshift_query_status": status, "detail": final.get("Error", "")}))
        return {"error": "Redshift query failed"}  # sanitized (#59)

    try:
        result = redshift_data.get_statement_result(Id=statement_id)
    except Exception as e:
        logger.error(json.dumps({"get_result_error": str(e)}))
        return {"error": "Failed to retrieve Redshift query results"}  # sanitized (#59)

    records = result.get("Records", [])
    tables = []
    for row in records:
        schema_val = (row[0].get("stringValue") or "") if row else ""
        name_val = (row[1].get("stringValue") or "") if len(row) > 1 else ""
        type_val = (row[2].get("stringValue") or "") if len(row) > 2 else ""
        tables.append({"schema": schema_val, "name": name_val, "type": type_val})

    return {
        "source_id": source_id,
        # workgroup omitted — do not expose infrastructure identifiers (#56, #59)
        "tables": tables,
        "count": len(tables),
    }
