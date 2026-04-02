"""
roda_load: Load an open dataset into Quick Sight.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Given a dataset slug and optional resource index/prefix, this Lambda:
1. Looks up the dataset in the RODA catalog (DynamoDB)
2. Probes the S3 bucket to discover structure and sample files
3. Generates a Quick Sight S3 manifest file
4. Creates a Quick Sight data source
5. Returns the dataset ID for Quick Sight consumption

Supports CSV, TSV, Parquet, and JSON formats. For other formats,
returns instructions for routing through the compute layer.
"""

import json
import logging
import os
import time
import uuid
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
quicksight = boto3.client(
    'quicksight',
    region_name=os.environ.get('QUICKSIGHT_REGION', 'us-east-1')
)

TABLE_NAME = os.environ['TABLE_NAME']
MANIFEST_BUCKET = os.environ['MANIFEST_BUCKET']
QS_ACCOUNT_ID = os.environ['QUICKSIGHT_ACCOUNT_ID']

QS_DIRECT_FORMATS = {'csv', 'tsv', 'parquet', 'json'}
MAX_MANIFEST_FILES = 200


def handler(event: dict, context: Any) -> dict:
    """
    Load an open dataset into Quick Sight.

    Tool arguments (from AgentCore Gateway, passed directly as event):
    - slug: str (required) — dataset slug from roda_search results
    - resource_index: int — which S3 resource to use (default 0)
    - prefix: str — optional S3 key prefix to narrow scope
    - format: str — data format hint (csv, tsv, parquet, json)
    - sample_only: bool — if true, only load first 10 files
    - dataset_name: str — custom name for the Quick Sight dataset
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    slug = event.get('slug', '').strip()
    if not slug:
        return {'error': 'slug is required'}

    try:
        resource_index = int(event.get('resource_index', 0))
    except (TypeError, ValueError):
        return {'error': "'resource_index' must be an integer"}
    prefix = event.get('prefix', '')
    format_hint = event.get('format', '').lower()
    _raw_sample = event.get('sample_only', False)
    sample_only = _raw_sample.lower() in ("true", "1", "yes") if isinstance(_raw_sample, str) else bool(_raw_sample)
    custom_name = event.get('dataset_name', '')
    join_slug = event.get('join_slug', '').strip()
    join_key = event.get('join_key', '').strip()

    # Look up dataset in catalog
    table = dynamodb.Table(TABLE_NAME)
    try:
        resp = table.get_item(Key={'slug': slug})
        item = resp.get('Item')
        if not item:
            return {'error': f'Dataset "{slug}" not found'}
    except Exception as e:
        return {'error': f'Catalog lookup failed: {e}'}

    # Get the S3 resource
    s3_resources = item.get('s3Resources', [])
    if not s3_resources:
        return {
            'error': 'No S3 resources for this dataset',
            'suggestion': 'This dataset may use non-S3 resources. '
                          'Check the registry page for access instructions.',
            'registryUrl': item.get('registryUrl', ''),
        }

    if resource_index < 0 or resource_index >= len(s3_resources):
        return {
            'error': f'Resource index {resource_index} out of range '
                     f'(dataset has {len(s3_resources)} S3 resources)',
        }

    resource = s3_resources[resource_index]
    bucket_name = _extract_bucket_name(resource.get('arn', ''))
    if not bucket_name:
        bad_arn = resource.get('arn', '(missing)')
        logger.error(f"Invalid S3 ARN for slug={slug}: {bad_arn!r}")
        return {'error': 'Could not parse S3 bucket from ARN in catalog. '
                         'The dataset may have an invalid resource definition.'}

    requester_pays = resource.get('requesterPays', False)

    # Detect format if not provided
    detected_formats = item.get('formats', [])
    if not format_hint:
        for preferred in ['parquet', 'csv', 'json', 'tsv']:
            if preferred in detected_formats:
                format_hint = preferred
                break

    if format_hint and format_hint not in QS_DIRECT_FORMATS:
        return {
            'status': 'requires_transform',
            'message': f'Format "{format_hint}" requires transformation before '
                       f'Quick Sight can ingest it. Route through the compute '
                       f'layer (EMR Serverless or Lambda) to convert to Parquet.',
            'dataset': item.get('name', ''),
            'bucket': bucket_name,
            'detectedFormats': detected_formats,
            'registryUrl': item.get('registryUrl', ''),
        }

    # Probe the bucket for matching files
    try:
        files = _probe_bucket(
            bucket_name, prefix, format_hint,
            requester_pays=requester_pays,
            max_files=10 if sample_only else MAX_MANIFEST_FILES,
        )
    except Exception as e:
        return {
            'error': f'Failed to probe bucket: {e}',
            'bucket': bucket_name,
            'prefix': prefix,
            'suggestion': 'The bucket may require special access. '
                          'Check the registry page.',
            'registryUrl': item.get('registryUrl', ''),
        }

    if not files:
        return {
            'status': 'no_matching_files',
            'message': f'No {format_hint or "tabular"} files found in '
                       f's3://{bucket_name}/{prefix}',
            'suggestion': 'Try specifying a different prefix or format.',
            'documentation': item.get('documentation', ''),
            'registryUrl': item.get('registryUrl', ''),
        }

    # Generate manifest
    manifest = _generate_manifest(bucket_name, files, format_hint)
    manifest_key = f"roda-manifests/{slug}/{uuid.uuid4().hex[:8]}.manifest.json"

    try:
        s3.put_object(
            Bucket=MANIFEST_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest),
            ContentType='application/json',
        )
    except Exception as e:
        return {'error': f'Failed to write manifest: {e}'}

    # Create Quick Sight data source
    dataset_name = custom_name or f"RODA: {item.get('name', slug)}"
    ds_id = f"roda-{slug}-{uuid.uuid4().hex[:8]}"

    # Optional join: load a second RODA dataset and join in QuickSight
    join_manifest_key = None
    join_item = None
    if join_slug and join_key:
        try:
            join_resp = table.get_item(Key={'slug': join_slug})
            join_item = join_resp.get('Item')
        except Exception:
            join_item = None
        if join_item:
            join_s3_resources = join_item.get('s3Resources', [])
            if join_s3_resources:
                join_bucket = _extract_bucket_name(join_s3_resources[0].get('arn', ''))
                if join_bucket:
                    try:
                        join_files = _probe_bucket(join_bucket, '', format_hint,
                                                   max_files=MAX_MANIFEST_FILES)
                        if join_files:
                            join_manifest = _generate_manifest(join_bucket, join_files, format_hint)
                            join_manifest_key = f"roda-manifests/{join_slug}/{uuid.uuid4().hex[:8]}.manifest.json"
                            s3.put_object(
                                Bucket=MANIFEST_BUCKET,
                                Key=join_manifest_key,
                                Body=json.dumps(join_manifest),
                                ContentType='application/json',
                            )
                    except Exception:
                        join_manifest_key = None

    try:
        qs_result = _create_quicksight_dataset(
            ds_id, dataset_name, MANIFEST_BUCKET, manifest_key, format_hint,
            join_manifest_key=join_manifest_key,
            join_key=join_key if join_manifest_key else None,
        )
    except Exception as e:
        return {
            'status': 'manifest_ready',
            'message': f'Manifest created but Quick Sight dataset creation '
                       f'failed: {e}. You can manually import using the manifest file.',
            'manifestUri': f's3://{MANIFEST_BUCKET}/{manifest_key}',
            'fileCount': len(files),
            'format': format_hint,
        }

    # Related dataset suggestions (same primary tag, up to 5)
    suggestions = []
    primary_tag = item.get('primaryTag', '')
    if primary_tag:
        try:
            sugg_resp = table.query(
                IndexName='by-primary-tag',
                KeyConditionExpression=Key('primaryTag').eq(primary_tag),
                FilterExpression=Attr('slug').ne(slug),
                Limit=6,
                ProjectionExpression='slug, #n',
                ExpressionAttributeNames={'#n': 'name'},
            )
            suggestions = [
                {'slug': r['slug'], 'name': r.get('name', '')}
                for r in sugg_resp.get('Items', [])
                if r['slug'] != slug
            ][:5]
        except Exception:
            pass

    result = {
        'status': 'loaded',
        'datasetId': ds_id,
        'datasetName': dataset_name,
        'fileCount': len(files),
        'format': format_hint,
        'bucket': bucket_name,
        'manifestUri': f's3://{MANIFEST_BUCKET}/{manifest_key}',
        'registryUrl': item.get('registryUrl', ''),
        'quicksightResult': qs_result,
        'claws_source_id': f'roda-{slug}',
        'suggestions': suggestions,
    }
    if join_manifest_key and join_item:
        result['join_applied'] = True
        result['join_slug'] = join_slug
    return result


def _extract_bucket_name(arn: str) -> str:
    if ':::' in arn:
        path = arn.split(':::')[-1]
        return path.split('/')[0]
    return ''


def _probe_bucket(
    bucket: str, prefix: str, format_hint: str,
    requester_pays: bool = False, max_files: int = 100
) -> list[str]:
    ext_map = {
        'csv': ['.csv', '.csv.gz'],
        'tsv': ['.tsv', '.tsv.gz', '.tab', '.tab.gz'],
        'parquet': ['.parquet', '.snappy.parquet', '.parq'],
        'json': ['.json', '.json.gz', '.jsonl', '.ndjson'],
    }
    extensions = ext_map.get(format_hint, ['.csv', '.tsv', '.parquet', '.json', '.jsonl', '.ndjson'])

    list_kwargs = {'Bucket': bucket, 'MaxKeys': 1000}
    if prefix:
        list_kwargs['Prefix'] = prefix
    if requester_pays:
        list_kwargs['RequestPayer'] = 'requester'

    matching = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(**list_kwargs):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if any(key.lower().endswith(ext) for ext in extensions):
                matching.append(key)
                if len(matching) >= max_files:
                    return matching

    return matching


def _generate_manifest(bucket: str, keys: list[str], format_hint: str) -> dict:
    uri_prefixes = []
    uri_list = []

    if len(keys) > 20:
        common = os.path.commonprefix(keys)
        if '/' in common:
            prefix = common[:common.rindex('/') + 1]
            uri_prefixes.append(f"s3://{bucket}/{prefix}")
        else:
            uri_list = [f"s3://{bucket}/{key}" for key in keys]
    else:
        uri_list = [f"s3://{bucket}/{key}" for key in keys]

    manifest: dict = {'fileLocations': []}
    if uri_prefixes:
        manifest['fileLocations'].append({'URIPrefixes': uri_prefixes})
    if uri_list:
        manifest['fileLocations'].append({'URIs': uri_list})

    if format_hint == 'csv':
        manifest['globalUploadSettings'] = {
            'format': 'CSV', 'delimiter': ',', 'containsHeader': 'true',
        }
    elif format_hint == 'tsv':
        manifest['globalUploadSettings'] = {
            'format': 'TSV', 'delimiter': '\t', 'containsHeader': 'true',
        }
    elif format_hint == 'json':
        manifest['globalUploadSettings'] = {'format': 'JSON'}

    return manifest


def _create_quicksight_dataset(
    ds_id: str, name: str, manifest_bucket: str,
    manifest_key: str, format_hint: str,
    join_manifest_key: str = None, join_key: str = None,
) -> dict:
    data_source_id = f"{ds_id}-source"
    qs_region = os.environ.get("QUICKSIGHT_REGION", "us-east-1")
    qs_user = os.environ.get("QUICKSIGHT_USER", "Admin")
    admin_principal = f"arn:aws:quicksight:{qs_region}:{QS_ACCOUNT_ID}:user/default/{qs_user}"

    permissions = [
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
    ]

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
        Permissions=permissions,
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

    physical_table_map = {
        "primary": {
            "S3Source": {
                "DataSourceArn": ds_response["Arn"],
                "UploadSettings": {"Format": (format_hint or "CSV").upper()},
                "InputColumns": [],
            }
        }
    }
    logical_table_map = None

    # If joining a second dataset, create its data source and add a join
    if join_manifest_key and join_key:
        join_source_id = f"{ds_id}-join-source"
        join_ds_response = quicksight.create_data_source(
            AwsAccountId=QS_ACCOUNT_ID,
            DataSourceId=join_source_id,
            Name=f"{name} (Join Source)",
            Type='S3',
            DataSourceParameters={
                'S3Parameters': {
                    'ManifestFileLocation': {
                        'Bucket': manifest_bucket,
                        'Key': join_manifest_key,
                    },
                },
            },
            Permissions=permissions,
        )
        for _ in range(10):
            st = quicksight.describe_data_source(
                AwsAccountId=QS_ACCOUNT_ID, DataSourceId=join_source_id
            )["DataSource"]["Status"]
            if st == "CREATION_SUCCESSFUL":
                break
            if st == "CREATION_FAILED":
                raise RuntimeError("Join DataSource creation failed")
            time.sleep(1)

        physical_table_map["secondary"] = {
            "S3Source": {
                "DataSourceArn": join_ds_response["Arn"],
                "UploadSettings": {"Format": (format_hint or "CSV").upper()},
                "InputColumns": [],
            }
        }
        logical_table_map = {
            "joined": {
                "Alias": "joined",
                "Source": {
                    "JoinInstruction": {
                        "LeftOperand": "primary",
                        "RightOperand": "secondary",
                        "Type": "INNER",
                        "OnClause": f"{join_key} = {join_key}",
                    }
                },
            }
        }

    dataset_kwargs = dict(
        AwsAccountId=QS_ACCOUNT_ID,
        DataSetId=ds_id,
        Name=name,
        PhysicalTableMap=physical_table_map,
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
    if logical_table_map:
        dataset_kwargs['LogicalTableMap'] = logical_table_map

    quicksight.create_data_set(**dataset_kwargs)

    return {
        'dataSourceId': data_source_id,
        'datasetId': ds_id,
        'status': 'created',
    }
