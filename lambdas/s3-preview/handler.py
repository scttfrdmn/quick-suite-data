"""
s3_preview: Sample rows and infer schema from an S3 object.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Only accesses buckets explicitly configured in SOURCES_CONFIG.
For Parquet files, requires the pyarrow Lambda layer.
For CSV/JSON files, uses stdlib only.
"""

import json
import logging
import os
from typing import Any

import boto3

# data_utils is provided by the common Lambda layer
from data_utils import (
    detect_format_from_key,
    infer_schema_from_bytes,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

try:
    _sources: list[dict] = json.loads(os.environ.get('SOURCES_CONFIG', '[]'))
except json.JSONDecodeError as e:
    logger.error(f"Invalid SOURCES_CONFIG JSON: {e}")
    _sources = []

# Max bytes to download for schema inference (avoid downloading huge files)
MAX_PREVIEW_BYTES = 512 * 1024  # 512 KB


def handler(event: dict, context: Any) -> dict:
    """
    Sample rows and infer schema from an S3 file.

    Tool arguments:
    - source: str — institutional source label
    - key: str — S3 key (relative to source prefix)
    - format: str — hint: csv, tsv, json, parquet (optional, inferred from key)
    - max_rows: int — rows to sample (default 5)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    source_label = event.get('source', '').strip()
    key = event.get('key', '').strip()

    if not source_label or not key:
        return {'error': 'Both "source" and "key" are required.'}

    source = _find_source(source_label)
    if not source:
        return {'error': f'Source "{source_label}" not found. '
                         f'{len(_sources)} source(s) are configured.'}

    bucket = source['bucket']
    base_prefix = source.get('prefix', '')
    if '..' in key.lstrip('/').split('/'):
        return {'error': 'Access denied: key contains invalid path components.'}
    # Accept both full S3 keys (as returned by s3_browse) and prefix-relative keys.
    full_key = key.lstrip('/')
    if base_prefix and not full_key.startswith(base_prefix):
        full_key = base_prefix + full_key

    if base_prefix and not full_key.startswith(base_prefix):
        return {'error': 'Access denied: key is outside configured source prefix.'}

    fmt = str(event.get('format') or '').lower() or detect_format_from_key(full_key)
    try:
        _max_rows = int(event.get('max_rows', 5))
    except (TypeError, ValueError):
        return {'error': "'max_rows' must be an integer"}
    if not (1 <= _max_rows <= 20):
        return {'error': "'max_rows' must be between 1 and 20"}

    # Get object metadata first
    try:
        head = s3.head_object(Bucket=bucket, Key=full_key)
        size_bytes = head.get('ContentLength', 0)
        content_type = head.get('ContentType', '')
    except s3.exceptions.NoSuchKey:
        return {'error': f'Key "{key}" not found in source "{source_label}".'}
    except Exception as e:
        return {'error': f'Failed to access object: {e}'}

    # Download a prefix of the file for schema inference
    if size_bytes == 0:
        return {'error': 'File is empty', 'key': key, 'size_bytes': 0}
    try:
        range_end = min(size_bytes, MAX_PREVIEW_BYTES) - 1
        resp = s3.get_object(
            Bucket=bucket,
            Key=full_key,
            Range=f"bytes=0-{range_end}",
        )
        content = resp['Body'].read()
    except Exception as e:
        logger.error(f"s3_preview download failed: {e}")
        return {'error': f'Failed to download preview: {e}'}

    schema = infer_schema_from_bytes(content, fmt)

    return {
        'source': source_label,
        'key': key,
        'bucket': bucket,
        'fullKey': full_key,
        'sizeBytes': size_bytes,
        'sizeMB': round(size_bytes / 1024 / 1024, 2),
        'format': fmt or 'unknown',
        'contentType': content_type,
        'previewBytes': len(content),
        **schema,
    }


def _find_source(label: str) -> dict | None:
    label_lower = label.lower()
    for s in _sources:
        if s['label'].lower() == label_lower:
            return s
    return None
