"""
claws_resolver — internal Lambda (not an AgentCore target).

Resolves a clAWS source_id to a Quick Sight dataset_id.
Used by the compute extract Lambda to hand off from an open-data source
to a compute job without requiring the caller to know the dataset_id.

Input:  {"source_id": "roda-noaa-ghcn"}
Output: {"source_id": "...", "dataset_id": "qs-123"}
        or {"error": "not found"}
"""

import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CLAWS_LOOKUP_TABLE = os.environ["CLAWS_LOOKUP_TABLE"]
dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    source_id = (event.get("source_id") or "").strip()
    if not source_id:
        return {"error": "source_id is required"}

    logger.info(json.dumps({"source_id": source_id}))
    table = dynamodb.Table(CLAWS_LOOKUP_TABLE)
    try:
        resp = table.get_item(Key={"source_id": source_id})
        item = resp.get("Item")
        if not item:
            return {"error": f"source_id '{source_id}' not found in lookup table"}
        return {"source_id": source_id, "dataset_id": item["dataset_id"]}
    except Exception as exc:
        logger.error(json.dumps({"lookup_error": str(exc), "source_id": source_id}))
        return {"error": f"Lookup failed: {exc}"}
