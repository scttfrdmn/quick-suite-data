"""
catalog_quality_check — EventBridge weekly trigger (not an AgentCore tool).

Scans the RODA catalog DynamoDB table for stale or unreachable items.

Stale: last_updated older than 2 years (or missing) → sets stale=True.
Unreachable: S3 bucket in s3Resources returns NoSuchBucket → sets unreachable=True.

Emits CloudWatch metrics:
  StaleDatasets      — count of stale items found
  UnreachableDatasets — count of items with inaccessible S3 buckets
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
dynamodb = boto3.resource("dynamodb")
cw = boto3.client("cloudwatch")

# Anonymous S3 client for probing public RODA dataset buckets
s3_anon = boto3.client("s3", config=Config(signature_version=UNSIGNED))

TWO_YEARS_SECONDS = 2 * 365 * 24 * 3600
SIX_MONTHS_SECONDS = 180 * 24 * 3600
SCHEMA_COMPLETENESS_FIELDS = ["name", "description", "tags", "formats", "s3Resources", "registryUrl"]


def _compute_quality_score(item: dict, now: int) -> dict:
    """Compute quality_score dict for a catalog item (same formula as roda-search)."""
    last_updated = item.get("last_updated")
    if last_updated is None:
        freshness = "stale"
    else:
        try:
            age = now - float(last_updated)
            if age < SIX_MONTHS_SECONDS:
                freshness = "current"
            elif age < TWO_YEARS_SECONDS:
                freshness = "aging"
            else:
                freshness = "stale"
        except (TypeError, ValueError):
            freshness = "stale"

    present = 0
    for field in SCHEMA_COMPLETENESS_FIELDS:
        val = item.get(field)
        if val is not None and val != "" and val != [] and val != {}:
            present += 1
    schema_completeness = present / len(SCHEMA_COMPLETENESS_FIELDS)

    return {
        "freshness": freshness,
        "schema_completeness": schema_completeness,
        "last_verified": datetime.now(timezone.utc).isoformat(),
    }


def _probe_s3_resources(s3_resources: list) -> bool:
    """
    Return True if any S3 resource bucket is unreachable (NoSuchBucket / 404).
    Uses an anonymous client — RODA datasets are publicly accessible.
    Other errors (403 AccessDenied, network) are ignored to avoid false positives.
    """
    for resource in s3_resources:
        arn = resource.get("arn", "")
        # ARN format: arn:aws:s3:::bucket-name
        if ":::" not in arn:
            continue
        bucket = arn.split(":::")[-1].strip()
        if not bucket:
            continue
        try:
            s3_anon.head_bucket(Bucket=bucket)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket"):
                return True
            # 403 AccessDenied = bucket exists but restricted — not unreachable
        except Exception:
            pass  # Network errors etc. — don't flag as unreachable
    return False


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    now = int(time.time())
    cutoff = now - TWO_YEARS_SECONDS

    stale_count = 0
    unreachable_count = 0
    scanned = 0
    last_key = None

    while True:
        scan_kwargs = {
            "ProjectionExpression": (
                "slug, last_updated, s3Resources, #n, description, tags, formats, registryUrl"
            ),
            "ExpressionAttributeNames": {"#n": "name"},
        }
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key

        try:
            resp = table.scan(**scan_kwargs)
        except Exception as exc:
            logger.error(json.dumps({"scan_error": str(exc)}))
            break

        for item in resp.get("Items", []):
            scanned += 1
            last_updated = item.get("last_updated")
            is_stale = (last_updated is None) or (int(last_updated) < cutoff)

            quality_score = _compute_quality_score(item, now)
            verified_at = quality_score["last_verified"]

            if is_stale:
                stale_count += 1
                try:
                    table.update_item(
                        Key={"slug": item["slug"]},
                        UpdateExpression=(
                            "SET stale = :stale, last_verified = :lv, quality_score = :qs"
                        ),
                        ExpressionAttributeValues={
                            ":stale": True,
                            ":lv": verified_at,
                            ":qs": quality_score,
                        },
                    )
                except Exception as exc:
                    logger.warning(json.dumps({"update_error": str(exc), "slug": item["slug"]}))
            else:
                # Always write back last_verified and quality_score even for non-stale items
                try:
                    table.update_item(
                        Key={"slug": item["slug"]},
                        UpdateExpression="SET last_verified = :lv, quality_score = :qs",
                        ExpressionAttributeValues={
                            ":lv": verified_at,
                            ":qs": quality_score,
                        },
                    )
                except Exception as exc:
                    logger.warning(json.dumps({"update_error": str(exc), "slug": item["slug"]}))

            s3_resources = item.get("s3Resources") or []
            if s3_resources and _probe_s3_resources(s3_resources):
                unreachable_count += 1
                try:
                    table.update_item(
                        Key={"slug": item["slug"]},
                        UpdateExpression="SET unreachable = :v",
                        ExpressionAttributeValues={":v": True},
                    )
                except Exception as exc:
                    logger.warning(json.dumps({"update_error": str(exc), "slug": item["slug"]}))

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    logger.info(json.dumps({
        "scanned": scanned,
        "stale_count": stale_count,
        "unreachable_count": unreachable_count,
    }))

    try:
        cw.put_metric_data(
            Namespace="QuickSuiteOpenData",
            MetricData=[
                {
                    "MetricName": "StaleDatasets",
                    "Value": stale_count,
                    "Unit": "Count",
                },
                {
                    "MetricName": "UnreachableDatasets",
                    "Value": unreachable_count,
                    "Unit": "Count",
                },
            ],
        )
    except Exception as exc:
        logger.warning(json.dumps({"cw_error": str(exc)}))

    return {
        "scanned": scanned,
        "stale_count": stale_count,
        "unreachable_count": unreachable_count,
    }
