"""
s3_load: Register an institutional S3 path as a Quick Sight data source.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Only accesses buckets explicitly configured in SOURCES_CONFIG.
Creates a Quick Sight data source + dataset from the specified S3 path.
"""

import json
import logging
import os
import re
import time
import uuid
from typing import Any

import boto3
from data_utils import detect_format_from_key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
quicksight = boto3.client(
    'quicksight',
    region_name=os.environ.get('QUICKSIGHT_REGION', 'us-east-1')
)

try:
    _sources: list[dict] = json.loads(os.environ.get('SOURCES_CONFIG', '[]'))
except json.JSONDecodeError as e:
    logger.error(f"Invalid SOURCES_CONFIG JSON: {e}")
    _sources = []
MANIFEST_BUCKET = os.environ['MANIFEST_BUCKET']
QS_ACCOUNT_ID = os.environ['QUICKSIGHT_ACCOUNT_ID']
QS_REGION = os.environ.get('QUICKSIGHT_REGION', 'us-east-1')

QS_DIRECT_FORMATS = {'csv', 'tsv', 'parquet', 'json'}
MAX_MANIFEST_FILES = 200


def handler(event: dict, context: Any) -> dict:
    """
    Register an institutional S3 path as a Quick Sight data source.

    Tool arguments:
    - source: str — institutional source label
    - prefix: str — S3 key prefix within source to load (optional)
    - format: str — data format (csv, tsv, parquet, json; inferred if omitted)
    - dataset_name: str — name for the Quick Sight dataset
    - sample_only: bool — if true, only include first 10 files
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    source_label = event.get('source', '').strip()
    if not source_label:
        return {'error': '"source" is required. Use s3_browse to discover available sources.'}

    source = _find_source(source_label)
    if not source:
        return {'error': f'Source "{source_label}" not found. '
                         f'{len(_sources)} source(s) are configured.',
        }

    bucket = source['bucket']
    base_prefix = source.get('prefix', '')

    # Support both single 'prefix' and list 'prefixes'
    single_prefix = event.get('prefix', '').lstrip('/')
    prefixes_raw = event.get('prefixes', [])
    if prefixes_raw and isinstance(prefixes_raw, list):
        extra_prefixes = [str(p).lstrip('/') for p in prefixes_raw]
    else:
        extra_prefixes = [single_prefix]

    # Validate all prefixes
    for ep in extra_prefixes:
        if '..' in ep.split('/'):
            return {'error': 'Access denied: prefix contains invalid path components.'}
        full = base_prefix + ep
        if base_prefix and not full.startswith(base_prefix):
            return {'error': 'Access denied: prefix is outside configured source prefix.'}

    fmt = str(event.get('format') or '').lower()
    display_prefix = extra_prefixes[0] if len(extra_prefixes) == 1 else f"{len(extra_prefixes)} prefixes"
    custom_name = event.get('dataset_name', '') or f"{source_label}{' / ' + display_prefix if display_prefix else ''}"
    _raw_sample = event.get('sample_only', False)
    sample_only = _raw_sample.lower() in ("true", "1", "yes") if isinstance(_raw_sample, str) else bool(_raw_sample)
    max_per_prefix = (10 if sample_only else MAX_MANIFEST_FILES) // max(len(extra_prefixes), 1)

    # List files across all prefixes to build a combined manifest
    all_files = []
    for ep in extra_prefixes:
        full_prefix = base_prefix + ep
        try:
            prefix_files = _list_files(
                bucket, full_prefix, fmt,
                max_files=max_per_prefix,
            )
            all_files.extend(prefix_files)
        except Exception as e:
            return {'error': f'Failed to list files for prefix "{ep}": {e}'}
    files = all_files

    if not files:
        return {
            'status': 'no_matching_files',
            'message': f'No {fmt or "tabular"} files found in s3://{bucket}/ for the specified prefix(es)',
            'suggestion': 'Try a more specific prefix or different format.',
        }

    extra_prefix = extra_prefixes[0] if len(extra_prefixes) == 1 else ''

    # Infer format from first file if not provided
    if not fmt:
        fmt = detect_format_from_key(files[0]) or 'csv'

    if fmt not in QS_DIRECT_FORMATS:
        return {
            'status': 'requires_transform',
            'message': f'Format "{fmt}" cannot be directly ingested by Quick Sight. '
                       f'Route through the compute layer to convert to Parquet first.',
        }

    manifest = _generate_manifest(bucket, files, fmt)
    manifest_key = f"s3-loads/{source_label.lower().replace(' ', '-')}/{uuid.uuid4().hex[:8]}.manifest.json"

    try:
        s3.put_object(
            Bucket=MANIFEST_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest),
            ContentType='application/json',
        )
    except Exception as e:
        return {'error': f'Failed to write manifest: {e}'}

    ds_id = f"s3-{source_label.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"

    try:
        qs_result = _create_quicksight_datasource(
            ds_id, custom_name, MANIFEST_BUCKET, manifest_key, fmt=fmt
        )
    except Exception as e:
        return {
            'status': 'manifest_ready',
            'message': f'Manifest created but Quick Sight dataset creation failed: {e}',
            'manifestUri': f's3://{MANIFEST_BUCKET}/{manifest_key}',
            'fileCount': len(files),
        }

    return {
        'status': 'loaded',
        'datasetId': ds_id,
        'datasetName': custom_name,
        'source': source_label,
        'prefix': extra_prefix,
        'prefixCount': len(extra_prefixes),
        'fileCount': len(files),
        'format': fmt,
        'manifestUri': f's3://{MANIFEST_BUCKET}/{manifest_key}',
        'quicksightResult': qs_result,
        'claws_source_id': f's3-{re.sub(r"[^a-z0-9-]", "-", source_label.lower())}',
    }


def _list_files(bucket: str, prefix: str, fmt: str, max_files: int) -> list[str]:
    ext_map = {
        'csv': ['.csv', '.csv.gz'],
        'tsv': ['.tsv', '.tsv.gz', '.tab', '.tab.gz'],
        'parquet': ['.parquet', '.snappy.parquet', '.parq'],
        'json': ['.json', '.json.gz', '.jsonl', '.ndjson'],
    }
    extensions = ext_map.get(fmt, ['.csv', '.tsv', '.parquet', '.json', '.jsonl', '.ndjson'])

    paginator = s3.get_paginator('list_objects_v2')
    list_kwargs = {'Bucket': bucket, 'MaxKeys': 1000}
    if prefix:
        list_kwargs['Prefix'] = prefix

    matching = []
    for page in paginator.paginate(**list_kwargs):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if any(key.lower().endswith(ext) for ext in extensions):
                matching.append(key)
                if len(matching) >= max_files:
                    return matching
    return matching


def _generate_manifest(bucket: str, keys: list[str], fmt: str) -> dict:
    if len(keys) > 20:
        common = os.path.commonprefix(keys)
        if '/' in common:
            prefix = common[:common.rindex('/') + 1]
            file_locations = [{'URIPrefixes': [f"s3://{bucket}/{prefix}"]}]
        else:
            file_locations = [{'URIs': [f"s3://{bucket}/{k}" for k in keys]}]
    else:
        file_locations = [{'URIs': [f"s3://{bucket}/{k}" for k in keys]}]

    manifest: dict = {'fileLocations': file_locations}

    if fmt == 'csv':
        manifest['globalUploadSettings'] = {'format': 'CSV', 'delimiter': ',', 'containsHeader': 'true'}
    elif fmt == 'tsv':
        manifest['globalUploadSettings'] = {'format': 'TSV', 'delimiter': '\t', 'containsHeader': 'true'}
    elif fmt == 'json':
        manifest['globalUploadSettings'] = {'format': 'JSON'}

    return manifest


def _create_quicksight_datasource(
    ds_id: str, name: str, manifest_bucket: str, manifest_key: str, fmt: str = ""
) -> dict:
    data_source_id = f"{ds_id}-source"
    qs_user = os.environ.get("QUICKSIGHT_USER", "Admin")
    admin_principal = f"arn:aws:quicksight:{QS_REGION}:{QS_ACCOUNT_ID}:user/default/{qs_user}"

    ds_response = quicksight.create_data_source(
        AwsAccountId=QS_ACCOUNT_ID,
        DataSourceId=data_source_id,
        Name=f"{name} (Source)",
        Type='S3',
        DataSourceParameters={
            'S3Parameters': {
                'ManifestFileLocation': {
                    'Bucket': manifest_bucket,
                    'Key': manifest_key,
                },
            },
        },
        Permissions=[
            {
                'Principal': admin_principal,
                'Actions': [
                    'quicksight:DescribeDataSource',
                    'quicksight:DescribeDataSourcePermissions',
                    'quicksight:PassDataSource',
                    'quicksight:UpdateDataSource',
                    'quicksight:DeleteDataSource',
                    'quicksight:UpdateDataSourcePermissions',
                ],
            },
        ],
    )

    # Wait for DataSource to become ready before creating the DataSet
    for _ in range(10):
        status_resp = quicksight.describe_data_source(
            AwsAccountId=QS_ACCOUNT_ID, DataSourceId=data_source_id
        )
        ds_status = status_resp["DataSource"]["Status"]
        if ds_status == "CREATION_SUCCESSFUL":
            break
        if ds_status == "CREATION_FAILED":
            raise RuntimeError(f"DataSource creation failed: {ds_status}")
        time.sleep(1)
    else:
        raise RuntimeError("DataSource creation timed out after 10 seconds")

    # Create the DataSet on top of the DataSource (required for QuickSight analyses)
    quicksight.create_data_set(
        AwsAccountId=QS_ACCOUNT_ID,
        DataSetId=ds_id,
        Name=name,
        PhysicalTableMap={
            "source": {
                "S3Source": {
                    "DataSourceArn": ds_response["Arn"],
                    "UploadSettings": {
                        "Format": (fmt or "CSV").upper(),
                    },
                    "InputColumns": [],  # QuickSight auto-infers columns
                }
            }
        },
        ImportMode="DIRECT_QUERY",
        Permissions=[
            {
                'Principal': admin_principal,
                'Actions': [
                    'quicksight:DescribeDataSet',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:PassDataSet',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                ],
            },
        ],
    )

    return {
        'dataSourceId': data_source_id,
        'datasetId': ds_id,
        'status': 'created',
    }


def _find_source(label: str) -> dict | None:
    label_lower = label.lower()
    for s in _sources:
        if s['label'].lower() == label_lower:
            return s
    return None
