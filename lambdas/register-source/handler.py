"""
register_source: Internal Lambda — register a new data source in the source registry.

Not an AgentCore Gateway tool. Called by ops/admin tooling to add data sources
to the qs-data-source-registry DynamoDB table.

Returns a plain dict.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["SOURCE_REGISTRY_TABLE"]

ALLOWED_TYPES = {"s3", "snowflake", "redshift", "roda"}
ALLOWED_CLASSIFICATIONS = {"public", "internal", "restricted", "phi"}
REQUIRED_FIELDS = {"source_id", "type", "connection_config", "display_name", "description",
                   "data_classification"}


def handler(event: dict, context: Any) -> dict:
    """
    Register a data source in the source registry.

    Input fields:
    - source_id: str (required) — unique identifier for the source
    - type: str (required) — one of: s3, snowflake, redshift, roda
    - connection_config: str or dict (required) — Secret ARN for DB types,
        or JSON with keys bucket/prefix/description for s3 type
    - display_name: str (required)
    - description: str (required)
    - tags: list[str] (optional)
    - data_classification: str (required) — public | internal | restricted | phi
    """
    logger.info(json.dumps({"action": "register_source", "source_id": event.get("source_id")}))

    # Validate required fields
    missing = [f for f in REQUIRED_FIELDS if not event.get(f)]
    if missing:
        return {"error": f"Missing required fields: {', '.join(sorted(missing))}"}

    source_id = str(event["source_id"]).strip()
    source_type = str(event["type"]).strip()
    connection_config = event["connection_config"]
    display_name = str(event["display_name"]).strip()
    description = str(event["description"]).strip()
    tags = event.get("tags", [])
    data_classification = str(event["data_classification"]).strip()

    if not source_id:
        return {"error": "source_id must not be empty"}

    if source_type not in ALLOWED_TYPES:
        return {"error": f"Invalid type '{source_type}'. Must be one of: {', '.join(sorted(ALLOWED_TYPES))}"}

    if data_classification not in ALLOWED_CLASSIFICATIONS:
        return {
            "error": (
                f"Invalid data_classification '{data_classification}'. "
                f"Must be one of: {', '.join(sorted(ALLOWED_CLASSIFICATIONS))}"
            )
        }

    # Normalize connection_config to string for storage
    if isinstance(connection_config, dict):
        connection_config_str = json.dumps(connection_config)
    else:
        connection_config_str = str(connection_config)

    registered_at = datetime.now(timezone.utc).isoformat()

    item = {
        "source_id": source_id,
        "type": source_type,
        "connection_config": connection_config_str,
        "display_name": display_name,
        "description": description,
        "tags": tags if isinstance(tags, list) else [],
        "data_classification": data_classification,
        "registered_at": registered_at,
    }

    try:
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=item)
    except Exception as e:
        logger.error(json.dumps({"error": str(e), "source_id": source_id}))
        return {"error": f"Failed to register source: {e}"}

    logger.info(json.dumps({"registered": source_id, "type": source_type}))
    return {"status": "registered", "source_id": source_id}
