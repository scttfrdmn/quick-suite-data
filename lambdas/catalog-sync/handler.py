"""
catalog-sync: Fetches RODA NDJSON catalog from S3 and upserts into DynamoDB.

Runs on a daily schedule and optionally on SNS notifications from the
RODA object_created topic. Each dataset becomes a DynamoDB item keyed
by its slug (derived from the YAML filename).
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

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']
RODA_BUCKET = os.environ.get('RODA_BUCKET', 'registry.opendata.aws')
RODA_PREFIX = os.environ.get('RODA_PREFIX', 'roda/ndjson/')


def handler(event: dict, context: Any) -> dict:
    """
    Sync the RODA NDJSON catalog into DynamoDB.

    Handles two invocation modes:
    1. Scheduled (EventBridge) — full catalog sync
    2. SNS notification — incremental update for a single dataset
    """
    table = dynamodb.Table(TABLE_NAME)

    # Check if this is an SNS notification (incremental)
    if 'Records' in event and event['Records'][0].get('EventSource') == 'aws:sns':
        return handle_sns_update(event, table)

    # Full sync
    return handle_full_sync(table)


def handle_full_sync(table) -> dict:
    """Fetch all NDJSON files and upsert into DynamoDB."""
    logger.info(f"Starting full catalog sync from s3://{RODA_BUCKET}/{RODA_PREFIX}")

    # List all NDJSON files in the prefix
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=RODA_BUCKET, Prefix=RODA_PREFIX)

    synced = 0
    errors = 0

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.ndjson') and not key.endswith('.json'):
                continue

            try:
                response = s3.get_object(Bucket=RODA_BUCKET, Key=key)
                body = response['Body'].read().decode('utf-8')

                if not body.strip():
                    logger.warning(f"Empty NDJSON body for key={key}. Skipping to preserve catalog.")
                    errors += 1
                    continue

                # NDJSON: one JSON object per line
                for line in body.strip().split('\n'):
                    if not line.strip():
                        continue
                    try:
                        dataset = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Skipping malformed NDJSON line in {key}: {exc}")
                        errors += 1
                        continue
                    if not isinstance(dataset, dict):
                        logger.warning(f"Skipping non-dict JSON line in {key}: got {type(dataset).__name__}")
                        errors += 1
                        continue
                    item = transform_dataset(dataset, key)
                    if item:
                        table.put_item(Item=item)
                        synced += 1

            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                errors += 1

    if synced == 0 and errors == 0:
        logger.warning("Sync produced 0 datasets with 0 errors — possible empty catalog file.")
    logger.info(f"Sync complete: {synced} datasets synced, {errors} errors")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'synced': synced,
            'errors': errors,
        }),
    }


def handle_sns_update(event: dict, table) -> dict:
    """Process a single SNS notification for incremental update."""
    for record in event.get('Records', []):
        message = json.loads(record['Sns']['Message'])
        # The SNS message from RODA contains S3 event details
        for s3_record in message.get('Records', []):
            key = s3_record['s3']['object']['key']
            try:
                response = s3.get_object(Bucket=RODA_BUCKET, Key=key)
                body = response['Body'].read().decode('utf-8')
                if not body.strip():
                    logger.warning(f"Empty NDJSON body for key={key}. Skipping to preserve catalog.")
                    continue
                for line in body.strip().split('\n'):
                    if not line.strip():
                        continue
                    try:
                        dataset = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Skipping malformed NDJSON line in {key}: {exc}")
                        continue
                    if not isinstance(dataset, dict):
                        logger.warning(f"Skipping non-dict JSON line in {key}: got {type(dataset).__name__}")
                        continue
                    item = transform_dataset(dataset, key)
                    if item:
                        table.put_item(Item=item)
            except Exception as e:
                logger.error(f"Error processing SNS update for {key}: {e}")

    return {'statusCode': 200}


def transform_dataset(dataset: dict, source_key: str) -> dict | None:
    """
    Transform a RODA NDJSON dataset entry into a DynamoDB item.

    Extracts and normalizes fields for searchability. The slug is derived
    from the Name field (lowercased, special chars replaced with hyphens)
    matching how the RODA browser generates URL paths.
    """
    name = dataset.get('Name', '').strip()
    if not name:
        return None

    slug = derive_slug(name, source_key)
    tags = dataset.get('Tags', [])
    if not isinstance(tags, list):
        logger.warning(f"Tags is not a list for '{name}': {type(tags).__name__}; using []")
        tags = []
    resources = dataset.get('Resources', [])
    if not isinstance(resources, list):
        logger.warning(f"Resources is not a list for '{name}': {type(resources).__name__}; using []")
        resources = []

    # Extract S3 resources specifically
    s3_resources = []
    for r in resources:
        if r.get('Type') == 'S3 Bucket':
            s3_resources.append({
                'arn': r.get('ARN', ''),
                'region': r.get('Region', ''),
                'description': r.get('Description', ''),
                'requesterPays': r.get('RequesterPays', False),
                'accountRequired': r.get('AccountRequired', False),
                'explore': r.get('Explore', []),
            })

    # Detect data formats from descriptions and explore links
    formats = detect_formats(resources, dataset.get('Description', ''))

    # Pick the first non-generic tag as primary for the GSI
    primary_tag = next(
        (t for t in tags if t not in ('aws-pds',)),
        'uncategorized'
    )

    # Build searchable text blob (name + description + tags)
    description = dataset.get('Description', '')
    search_text = f"{name} {description} {' '.join(tags)}".lower()

    item = {
        'slug': slug,
        'name': name,
        'description': description[:2000],  # DDB item size budget
        'tags': tags,
        'primaryTag': primary_tag,
        'searchText': search_text[:4000],
        'license': dataset.get('License', ''),
        'managedBy': dataset.get('ManagedBy', ''),
        'updateFrequency': dataset.get('UpdateFrequency', ''),
        'contact': dataset.get('Contact', ''),
        'documentation': dataset.get('Documentation', ''),
        's3Resources': s3_resources,
        'formats': formats,
        'resourceCount': len(resources),
        's3ResourceCount': len(s3_resources),
        'registryUrl': f"https://registry.opendata.aws/{slug}/",
        'sourceKey': source_key,
        'last_updated': int(time.time()),
    }

    # Include DataAtWork if present (tutorials, publications)
    daw = dataset.get('DataAtWork', {})
    if isinstance(daw, dict):
        tutorials = daw.get('Tutorials', [])
        if tutorials:
            item['tutorials'] = tutorials[:5]  # Keep top 5
        publications = daw.get('Publications', [])
        if publications:
            item['publications'] = publications[:5]

    return item


def derive_slug(name: str, source_key: str) -> str:
    """
    Derive URL slug from dataset name or source key.
    Matches the convention used by the RODA browser.
    """
    # Try to extract from the source key (e.g. roda/ndjson/1000-genomes.ndjson)
    basename = source_key.rsplit('/', 1)[-1]
    if basename.endswith('.ndjson') or basename.endswith('.json'):
        slug = basename.rsplit('.', 1)[0]
        if slug and len(slug) > 2:
            return slug

    # Fall back to name-based slug
    slug = name.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')[:128]


def detect_formats(resources: list, description: str) -> list[str]:
    """Detect likely data formats from resource descriptions and metadata."""
    formats = set()
    text = description.lower()

    for r in resources:
        rdesc = (r.get('Description', '') or '').lower()
        text += ' ' + rdesc
        for explore in (r.get('Explore', []) or []):
            if isinstance(explore, str):
                text += ' ' + explore.lower()

    format_patterns = {
        'parquet': r'\bparquet\b',
        'csv': r'\bcsv\b',
        'tsv': r'\btsv\b',
        'json': r'\bjson\b',
        'ndjson': r'\bndjson\b',
        'zarr': r'\bzarr\b',
        'netcdf': r'\bnetcdf\b|\.nc\b',
        'geotiff': r'\bgeotiff\b|\.tif\b',
        'cog': r'\bcog\b|cloud.optimized.geotiff',
        'vcf': r'\bvcf\b',
        'bam': r'\bbam\b',
        'fastq': r'\bfastq\b',
        'hdf5': r'\bhdf5?\b|\.h5\b',
        'grib': r'\bgrib\b',
        'shapefile': r'\bshapefile\b|\.shp\b',
    }

    for fmt, pattern in format_patterns.items():
        if re.search(pattern, text):
            formats.add(fmt)

    return sorted(formats)
