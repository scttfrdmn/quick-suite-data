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

# ---------------------------------------------------------------------------
# Data classification clearance ordering (lowest → highest)
# ---------------------------------------------------------------------------

_CLEARANCE_LEVELS = {"public": 0, "internal": 1, "restricted": 2, "phi": 3}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

_USE_SOURCE_REGISTRY = os.environ.get('USE_SOURCE_REGISTRY', '').lower() in ('true', '1', 'yes')
_SOURCE_REGISTRY_TABLE = os.environ.get('SOURCE_REGISTRY_TABLE', '')


def _load_sources_from_registry(caller_clearance_level: int = 0) -> list:
    """Load S3 sources from DynamoDB source registry. Falls back to empty list on error.

    Filters out sources whose data_classification exceeds caller_clearance_level.
    """
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
            # Clearance check: skip sources above caller's clearance level
            classification = (item.get('data_classification') or 'public').lower()
            if _CLEARANCE_LEVELS.get(classification, 0) > caller_clearance_level:
                continue
            try:
                conn = json.loads(item.get('connection_config', '{}'))
            except (json.JSONDecodeError, TypeError):
                conn = {}
            sources.append({
                'label': item.get('source_id', ''),
                'bucket': conn.get('bucket', ''),
                'prefix': conn.get('prefix', ''),
                'description': conn.get('description', item.get('description', '')),
                'data_classification': classification,
            })
        return sources
    except Exception as e:
        logger.warning(f"Failed to load sources from registry: {e}")
        return []


# Loaded at cold start (without caller_clearance — will be filtered per-request when registry mode)
if _USE_SOURCE_REGISTRY:
    _sources: list[dict] = _load_sources_from_registry(caller_clearance_level=3)  # load all at cold start
else:
    try:
        _sources = json.loads(os.environ.get('SOURCES_CONFIG', '[]'))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid SOURCES_CONFIG JSON: {e}")
        _sources = []

if not _USE_SOURCE_REGISTRY and not _sources:
    logger.warning(json.dumps({
        "level": "WARN",
        "msg": "No S3 sources configured — set SOURCES_CONFIG or USE_SOURCE_REGISTRY=true. "
               "All browse requests will return 'source not found'.",
    }))


def handler(event: dict, context: Any) -> dict:
    """
    List objects in an institutional S3 data source.

    Tool arguments:
    - source: str — label of the institutional source (from list_sources),
                    or omit to list available sources
    - prefix: str — S3 key prefix within the source to browse (optional)
    - max_keys: int — max objects to return (default 100, max 500)
    - caller_clearance: str — clearance level of the caller; sources above this level are hidden.
      Levels (lowest→highest): public < internal < restricted < phi.
      Defaults to "public" if not provided. Only applied when use_source_registry is enabled.
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    # When using source registry, apply caller_clearance filtering per-request
    raw_clearance = (event.get('caller_clearance') or 'public').strip().lower()
    caller_clearance_level = _CLEARANCE_LEVELS.get(raw_clearance, 0)
    if _USE_SOURCE_REGISTRY:
        effective_sources = _load_sources_from_registry(caller_clearance_level=caller_clearance_level)
    else:
        effective_sources = _sources

    if not event.get('source') and not event.get('list_sources'):
        # No source specified: return the catalog of available sources
        return _list_sources(effective_sources)

    source_label = event.get('source', '').strip()
    if not source_label:
        return _list_sources(effective_sources)

    source = _find_source(source_label, effective_sources)
    if not source:
        return {'error': f'Source "{source_label}" not found. '
                         f'{len(effective_sources)} source(s) are configured.'}

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
        return {'error': f'Source "{source_label}" is not accessible.'}  # bucket name sanitized (#59)
    except Exception as e:
        logger.error(f"s3_browse failed for source={source_label}: {e}")
        return {'error': 'Browse failed. Check the source name and try again.'}  # sanitized (#59)


def _list_sources(sources: list | None = None) -> dict:
    """Return the catalog of configured institutional sources."""
    src_list = sources if sources is not None else _sources
    return {
        'sources': [
            {
                'label': s['label'],
                'description': s.get('description', ''),
                'prefix': s.get('prefix', '(root)'),
            }
            for s in src_list
        ],
        'count': len(src_list),
        'hint': 'Use the "source" argument with one of these labels to browse.',
    }


def _find_source(label: str, sources: list | None = None) -> dict | None:
    src_list = sources if sources is not None else _sources
    label_lower = label.lower()
    for s in src_list:
        if s['label'].lower() == label_lower:
            return s
    return None
