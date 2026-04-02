"""
roda_search: Search the RODA catalog in DynamoDB.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Supports three search modes:
1. Tag-based (GSI query) — fast, exact match on primary tag
2. Keyword scan — scans searchText field with contains filters
3. Combined — tag filter + keyword refinement
"""

import base64
import hashlib
import json
import logging
import os
import re
import time
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['TABLE_NAME']
CACHE_TABLE = os.environ.get('SEARCH_CACHE_TABLE', '')
CACHE_TTL = 3600  # seconds

# Tags that map to common research domains
DOMAIN_TAG_MAP = {
    'genomics': ['genomics', 'genetic', 'genome', 'sequencing', 'variant', 'dna', 'rna'],
    'climate': ['climate', 'weather', 'atmospheric', 'temperature', 'precipitation'],
    'satellite': ['satellite', 'earth observation', 'remote sensing', 'imagery', 'radar'],
    'neuroscience': ['neuroscience', 'brain', 'neural', 'eeg', 'fmri'],
    'biodiversity': ['biodiversity', 'ecology', 'species', 'conservation'],
    'astronomy': ['astronomy', 'astrophysics', 'telescope', 'stellar', 'galaxy'],
    'geospatial': ['geospatial', 'geographic', 'mapping', 'gis', 'lidar'],
    'health': ['health', 'medical', 'clinical', 'disease', 'epidemiology'],
    'agriculture': ['agriculture', 'crop', 'soil', 'farming'],
    'oceans': ['oceans', 'marine', 'sea', 'coastal', 'bathymetry'],
    'machine learning': ['machine learning', 'deep learning', 'training data', 'benchmark'],
    'chemistry': ['chemistry', 'molecular', 'compound', 'drug discovery'],
    'economics': ['economics', 'financial', 'census', 'demographic'],
    'transportation': ['transportation', 'traffic', 'vehicle', 'aviation'],
    'energy': ['energy', 'solar', 'wind', 'power', 'grid'],
}


def handler(event: dict, context: Any) -> dict:
    """
    Search the RODA catalog.

    Tool arguments (from AgentCore Gateway, passed directly as event):
    - query: str — natural language search
    - tags: list[str] — filter by tags
    - format: str — filter by detected data format
    - region: str — filter by AWS region
    - max_results: int — default 10, max 50
    - quicksight_compatible: bool — only return CSV/Parquet/JSON datasets
    - pagination_token: str — opaque token from a previous response's next_token
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    query = event.get('query', '').strip()
    tags = event.get('tags', [])
    fmt = event.get('format', '')
    region = event.get('region', '')
    try:
        max_results = int(event.get('max_results', 10))
    except (TypeError, ValueError):
        return {'count': 0, 'datasets': [], 'query': '', 'appliedTags': [], 'appliedFormat': '',
                'error': "'max_results' must be an integer"}
    if not (1 <= max_results <= 50):
        return {'count': 0, 'datasets': [], 'query': '', 'appliedTags': [], 'appliedFormat': '',
                'error': "'max_results' must be between 1 and 50"}
    qs_only = event.get('quicksight_compatible', False)
    _raw_excl = event.get('exclude_deprecated', False)
    exclude_deprecated = _raw_excl.lower() in ("true", "1", "yes") if isinstance(_raw_excl, str) else bool(_raw_excl)

    # Decode pagination token
    pagination_token = str(event.get('pagination_token') or '').strip()
    exclusive_start_key = None
    if pagination_token:
        try:
            exclusive_start_key = json.loads(base64.b64decode(pagination_token).decode())
        except Exception:
            return {'count': 0, 'datasets': [], 'query': '', 'appliedTags': [], 'appliedFormat': '',
                    'error': 'Invalid pagination_token'}

    # Check search cache (only for non-paginated, non-empty-query requests)
    cache_key = None
    if CACHE_TABLE and query and not exclusive_start_key:
        cache_key = _make_cache_key(query, tags, fmt, max_results)
        cached = _cache_get(cache_key)
        if cached is not None:
            cached['cache_hit'] = True
            return cached

    table = dynamodb.Table(TABLE_NAME)
    results = []
    last_evaluated_key = None

    # Infer domain tags from natural language query
    if query and not tags:
        tags = infer_tags(query)

    # Strategy 1: If we have a single primary tag, use the GSI
    if len(tags) == 1:
        results, last_evaluated_key = query_by_tag(
            table, tags[0], max_results * 3, exclusive_start_key
        )
    # Strategy 2: Multiple tags or no tags — scan with filters
    else:
        results, last_evaluated_key = scan_with_filters(
            table, tags, max_results * 3, exclusive_start_key
        )

    # Keyword filter on results
    if query:
        keywords = extract_keywords(query)
        results = keyword_rank(results, keywords)

    # Format filter
    if fmt:
        results = [r for r in results if fmt.lower() in r.get('formats', [])]

    # Region filter
    if region:
        results = [
            r for r in results
            if any(s.get('region') == region for s in r.get('s3Resources', []))
        ]

    # QuickSight compatibility filter
    if qs_only:
        qs_formats = {'csv', 'tsv', 'parquet', 'json'}
        results = [
            r for r in results
            if qs_formats.intersection(set(r.get('formats', [])))
        ]

    # Deprecated filter
    if exclude_deprecated:
        results = [r for r in results if not r.get('deprecated', False)]

    results = results[:max_results]
    projected = [project_result(r) for r in results]
    now_ts = time.time()
    for i, item in enumerate(results):
        projected[i]["quality_score"] = compute_quality_score(item, now_ts)

    # Encode next_token for pagination
    next_token = ''
    if last_evaluated_key:
        next_token = base64.b64encode(json.dumps(last_evaluated_key).encode()).decode()

    response = {
        'count': len(projected),
        'datasets': projected,
        'query': query,
        'appliedTags': tags,
        'appliedFormat': fmt,
        'next_token': next_token,
        'cache_hit': False,
    }

    # Write to cache (only non-paginated, non-empty-query responses)
    if CACHE_TABLE and query and cache_key and not exclusive_start_key:
        _cache_put(cache_key, response)

    return response


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _make_cache_key(query: str, tags: list, fmt: str, max_results: int) -> str:
    raw = f"{query}|{sorted(tags)}|{fmt}|{max_results}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _cache_get(key: str):
    try:
        table = dynamodb.Table(CACHE_TABLE)
        resp = table.get_item(Key={'cache_key': key})
        item = resp.get('Item')
        if item and int(item.get('ttl', 0)) > int(time.time()):
            return json.loads(item['payload'])
    except Exception as e:
        logger.warning(f"Cache get failed: {e}")
    return None


def _cache_put(key: str, payload: dict):
    try:
        table = dynamodb.Table(CACHE_TABLE)
        table.put_item(Item={
            'cache_key': key,
            'payload': json.dumps(payload),
            'ttl': int(time.time()) + CACHE_TTL,
        })
    except Exception as e:
        logger.warning(f"Cache put failed: {e}")


# ---------------------------------------------------------------------------
# DynamoDB query/scan helpers
# ---------------------------------------------------------------------------

def query_by_tag(table, tag: str, limit: int, exclusive_start_key=None) -> tuple:
    kwargs = {
        'IndexName': 'by-primary-tag',
        'KeyConditionExpression': Key('primaryTag').eq(tag),
        'Limit': limit,
    }
    if exclusive_start_key:
        kwargs['ExclusiveStartKey'] = exclusive_start_key
    try:
        resp = table.query(**kwargs)
        return resp.get('Items', []), resp.get('LastEvaluatedKey')
    except Exception as e:
        logger.warning(f"GSI query failed for tag '{tag}': {e}")
        return [], None


def scan_with_filters(table, tags: list, limit: int, exclusive_start_key=None) -> tuple:
    scan_kwargs = {'Limit': limit}

    if exclusive_start_key:
        scan_kwargs['ExclusiveStartKey'] = exclusive_start_key

    if tags:
        filter_parts = []
        expr_values = {}
        for i, tag in enumerate(tags):
            key = f':tag{i}'
            filter_parts.append(f'contains(searchText, {key})')
            expr_values[key] = tag.lower()

        scan_kwargs['FilterExpression'] = ' OR '.join(filter_parts)
        scan_kwargs['ExpressionAttributeValues'] = expr_values

    MAX_SCAN_PAGES = 10

    try:
        resp = table.scan(**scan_kwargs)
        items = resp.get('Items', [])
        last_key = resp.get('LastEvaluatedKey')

        page_count = 0
        while last_key and len(items) < limit and page_count < MAX_SCAN_PAGES:
            page_count += 1
            scan_kwargs['ExclusiveStartKey'] = last_key
            resp = table.scan(**scan_kwargs)
            items.extend(resp.get('Items', []))
            last_key = resp.get('LastEvaluatedKey')

        if page_count >= MAX_SCAN_PAGES and len(items) < limit:
            logger.warning(f"Scan hit page cap ({MAX_SCAN_PAGES}) with {len(items)} results")

        return items[:limit], last_key
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        return [], None


# ---------------------------------------------------------------------------
# Ranking helpers
# ---------------------------------------------------------------------------

def infer_tags(query: str) -> list:
    query_lower = query.lower()
    matched_tags = []
    for tag, keywords in DOMAIN_TAG_MAP.items():
        for kw in keywords:
            if kw in query_lower:
                matched_tags.append(tag)
                break
    return matched_tags


def extract_keywords(query: str) -> list:
    stop_words = {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'can', 'shall', 'to', 'of', 'in', 'for',
        'on', 'with', 'at', 'by', 'from', 'as', 'into', 'about', 'between',
        'through', 'after', 'before', 'above', 'below', 'and', 'or', 'not',
        'but', 'if', 'then', 'so', 'than', 'too', 'very', 'just', 'only',
        'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those',
        'i', 'me', 'my', 'we', 'our', 'you', 'your', 'they', 'their',
        'it', 'its', 'any', 'all', 'some', 'find', 'show', 'get', 'list',
        'search', 'look', 'dataset', 'datasets', 'data', 'available',
    }
    words = re.findall(r'[a-z0-9]+', query.lower())
    return [w for w in words if w not in stop_words and len(w) > 2]


def keyword_rank(items: list, keywords: list) -> list:
    if not keywords:
        return items
    scored = []
    for item in items:
        text = item.get('searchText') or ''
        score = sum(1 for kw in keywords if kw in text)
        name = (item.get('name') or '').lower()
        # Standard name match (3x weight)
        score += sum(3 for kw in keywords if kw in name)
        # Exact full-name match bonus (5x extra per keyword)
        score += sum(5 for kw in keywords if kw == name)
        tags_text = ' '.join(item.get('tags') or [])
        score += sum(2 for kw in keywords if kw in tags_text)

        # Apply score multipliers
        if item.get('deprecated', False):
            score = score * 0.5

        update_freq = (item.get('updateFrequency') or '').lower()
        if update_freq == 'daily':
            score = score * 1.2
        elif update_freq == 'weekly':
            score = score * 1.1

        scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    # Return items with a keyword match; if nothing matched at all, return
    # all results in ranked order so tag/format filters aren't silently discarded.
    filtered = [item for score, item in scored if score > 0]
    return filtered if filtered else [item for _, item in scored]


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------

_SIX_MONTHS_SECONDS = 180 * 24 * 3600
_TWO_YEARS_SECONDS = 2 * 365 * 24 * 3600
_SCHEMA_COMPLETENESS_FIELDS = ["name", "description", "tags", "formats", "s3Resources", "registryUrl"]


def compute_quality_score(item: dict, now_ts: float | None = None) -> dict:
    """
    Compute a quality_score dict for a catalog item.

    Returns:
        {
            "freshness": "current" | "aging" | "stale",
            "schema_completeness": float,  # 0.0–1.0
            "last_verified": str | None,
        }
    """
    if now_ts is None:
        now_ts = time.time()

    last_updated = item.get("last_updated")
    if last_updated is None:
        freshness = "stale"
    else:
        try:
            age = now_ts - float(last_updated)
            if age < _SIX_MONTHS_SECONDS:
                freshness = "current"
            elif age < _TWO_YEARS_SECONDS:
                freshness = "aging"
            else:
                freshness = "stale"
        except (TypeError, ValueError):
            freshness = "stale"

    present = 0
    for field in _SCHEMA_COMPLETENESS_FIELDS:
        val = item.get(field)
        if val is not None and val != "" and val != [] and val != {}:
            present += 1
    schema_completeness = present / len(_SCHEMA_COMPLETENESS_FIELDS)

    last_verified = item.get("last_verified") or None

    return {
        "freshness": freshness,
        "schema_completeness": schema_completeness,
        "last_verified": last_verified,
    }


def project_result(item: dict) -> dict:
    s3_resources = item.get('s3Resources', [])
    primary_bucket = ''
    primary_region = ''
    if s3_resources:
        arn = s3_resources[0].get('arn', '')
        if ':::' in arn:
            primary_bucket = arn.split(':::')[-1].rstrip('/')
        primary_region = s3_resources[0].get('region', '')

    return {
        'slug': item.get('slug', ''),
        'name': item.get('name', ''),
        'description': item.get('description', '')[:400],
        'tags': item.get('tags', []),
        'formats': item.get('formats', []),
        'license': item.get('license', ''),
        'managedBy': item.get('managedBy', ''),
        'updateFrequency': item.get('updateFrequency', ''),
        'primaryBucket': primary_bucket,
        'primaryRegion': primary_region,
        's3ResourceCount': item.get('s3ResourceCount', 0),
        'registryUrl': item.get('registryUrl', ''),
        'quicksightCompatible': bool(
            {'csv', 'tsv', 'parquet', 'json'}.intersection(
                set(item.get('formats', []))
            )
        ),
        'documentation': item.get('documentation', ''),
        'deprecated': bool(item.get('deprecated', False)),
    }
