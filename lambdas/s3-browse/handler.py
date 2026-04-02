"""
s3_browse: List objects in configured institutional S3 data sources.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Only accesses buckets explicitly configured in SOURCES_CONFIG.
The LLM cannot browse arbitrary S3 paths.
"""

import json
import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

_USE_SOURCE_REGISTRY = os.environ.get('USE_SOURCE_REGISTRY', '').lower() in ('true', '1', 'yes')
_SOURCE_REGISTRY_TABLE = os.environ.get('SOURCE_REGISTRY_TABLE', '')


def _load_sources_from_registry() -> list:
    """Load S3 sources from DynamoDB source registry. Falls back to empty list on error."""
    if not _SOURCE_REGISTRY_TABLE:
        return []
    try:
        table = dynamodb.Table(_SOURCE_REGISTRY_TABLE)
        resp = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('type').eq('s3')
        )
        items = resp.get('Items', [])
        sources = []
        for item in items:
            try:
                conn = json.loads(item.get('connection_config', '{}'))
            except (json.JSONDecodeError, TypeError):
                conn = {}
            sources.append({
                'label': item.get('source_id', ''),
                'bucket': conn.get('bucket', ''),
                'prefix': conn.get('prefix', ''),
                'description': conn.get('description', item.get('description', '')),
            })
        return sources
    except Exception as e:
        logger.warning(f"Failed to load sources from registry: {e}")
        return []


# Loaded at cold start
if _USE_SOURCE_REGISTRY:
    _sources: list[dict] = _load_sources_from_registry()
else:
    try:
        _sources = json.loads(os.environ.get('SOURCES_CONFIG', '[]'))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid SOURCES_CONFIG JSON: {e}")
        _sources = []


def handler(event: dict, context: Any) -> dict:
    """
    List objects in an institutional S3 data source.

    Tool arguments:
    - source: str — label of the institutional source (from list_sources),
                    or omit to list available sources
    - prefix: str — S3 key prefix within the source to browse (optional)
    - max_keys: int — max objects to return (default 100, max 500)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    if not event.get('source') and not event.get('list_sources'):
        # No source specified: return the catalog of available sources
        return _list_sources()

    source_label = event.get('source', '').strip()
    if not source_label:
        return _list_sources()

    source = _find_source(source_label)
    if not source:
        return {'error': f'Source "{source_label}" not found. '
                         f'{len(_sources)} source(s) are configured.'}

    bucket = source['bucket']
    base_prefix = source.get('prefix', '')
    extra_prefix = event.get('prefix', '').lstrip('/')
    if '..' in extra_prefix.split('/'):
        return {'error': 'Access denied: prefix contains invalid path components.'}
    full_prefix = base_prefix + extra_prefix

    if base_prefix and not full_prefix.startswith(base_prefix):
        return {'error': 'Access denied: prefix is outside configured source prefix.'}

    try:
        max_keys = min(int(event.get('max_keys', 100)), 500)
    except (TypeError, ValueError):
        return {'error': "'max_keys' must be an integer"}

    try:
        list_kwargs = {
            'Bucket': bucket,
            'MaxKeys': max_keys,
            'Delimiter': '/',
        }
        if full_prefix:
            list_kwargs['Prefix'] = full_prefix

        resp = s3.list_objects_v2(**list_kwargs)

        # Common prefixes are "directories"
        prefixes = [
            p['Prefix'][len(base_prefix):].rstrip('/')
            for p in resp.get('CommonPrefixes', [])
        ]

        # Objects at this level
        objects = []
        for obj in resp.get('Contents', []):
            key = obj['Key']
            # Skip the prefix marker itself
            if key == full_prefix:
                continue
            objects.append({
                'key': key[len(base_prefix):],  # relative to source prefix
                'size': obj['Size'],
                'lastModified': obj['LastModified'].isoformat(),
            })

        return {
            'source': source['label'],
            'bucket': bucket,
            'prefix': full_prefix[len(base_prefix):],
            'subdirectories': prefixes,
            'objects': objects,
            'truncated': resp.get('IsTruncated', False),
            'count': len(objects),
        }

    except s3.exceptions.NoSuchBucket:
        return {'error': f'Bucket {bucket} not found or not accessible.'}
    except Exception as e:
        logger.error(f"s3_browse failed for source={source_label}: {e}")
        return {'error': f'Browse failed: {e}'}


def _list_sources() -> dict:
    """Return the catalog of configured institutional sources."""
    return {
        'sources': [
            {
                'label': s['label'],
                'description': s.get('description', ''),
                'prefix': s.get('prefix', '(root)'),
            }
            for s in _sources
        ],
        'count': len(_sources),
        'hint': 'Use the "source" argument with one of these labels to browse.',
    }


def _find_source(label: str) -> dict | None:
    label_lower = label.lower()
    for s in _sources:
        if s['label'].lower() == label_lower:
            return s
    return None
