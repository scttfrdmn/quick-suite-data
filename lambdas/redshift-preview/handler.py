"""
redshift_preview: Sample rows from a Redshift Serverless table.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Uses boto3 redshift-data client.
"""

import json
import logging
import os
import re
import time
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
redshift_data = boto3.client("redshift-data")

REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")

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
    Sample rows from a Redshift Serverless table.

    Tool arguments:
    - source_id: str (required) — identifier for the Redshift source
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

    config = _get_redshift_config()
    if config is None:
        return {"error": "Redshift source not configured"}

    workgroup = config.get("workgroup", "")
    database = config.get("database", "")
    secret_arn = config.get("secret_arn", "")

    sql = f'SELECT * FROM "{schema}"."{table}" LIMIT {max_rows}'

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
        return {"error": f"Redshift execute failed: {e}"}

    try:
        final = _poll_statement(statement_id)
    except TimeoutError:
        return {"error": "Redshift query timed out"}
    except Exception as e:
        logger.error(json.dumps({"poll_error": str(e)}))
        return {"error": f"Redshift poll failed: {e}"}

    status = final.get("Status", "")
    if status in ("FAILED", "ABORTED"):
        err = final.get("Error", "unknown error")
        return {"error": f"Redshift query failed: {err}"}

    try:
        result = redshift_data.get_statement_result(Id=statement_id)
    except Exception as e:
        logger.error(json.dumps({"get_result_error": str(e)}))
        return {"error": f"Redshift get result failed: {e}"}

    # Extract column names from ColumnMetadata
    column_metadata = result.get("ColumnMetadata", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(column_metadata)]

    records = result.get("Records", [])

    # Infer columns from first row if metadata not available
    if not columns and records:
        columns = [f"col{i}" for i in range(len(records[0]))]

    sample_rows = []
    for row in records:
        row_dict = {}
        for i, cell in enumerate(row):
            col_name = columns[i] if i < len(columns) else f"col{i}"
            # Redshift data API returns cells as {type: value} dicts
            val = None
            for cell_val in cell.values():
                val = cell_val
                break
            row_dict[col_name] = val
        sample_rows.append(row_dict)

    return {
        "source_id": source_id,
        "schema": schema,
        "table": table,
        "columns": columns,
        "sample_rows": sample_rows,
        "row_count": len(sample_rows),
        "format": "redshift",
    }
