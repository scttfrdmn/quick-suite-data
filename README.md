# Quick Suite Data

**Give Quick Suite access to 500+ public research datasets and your own institutional data — without touching a data pipeline.**

Quick Suite can visualize data that's already in Quick Sight. But getting data *into*
Quick Sight has always required manual work: finding the dataset, downloading or
configuring S3 access, building a manifest file, clicking through the console to create
a data source, waiting for the SPICE import. For a researcher or an IR analyst, that
friction is a barrier to every new question.

This extension removes that barrier. It adds five tools to Quick Suite's chat interface
that let users find, preview, and load data by describing what they need in plain
language — public research datasets from the Registry of Open Data on AWS, and
institutional data from your own S3 buckets. The result lands in Quick Sight, ready for
dashboards or further analysis via the Compute extension.

## What Quick Suite Alone Can't Do Here

- Search the Registry of Open Data on AWS and load a dataset into Quick Sight in a single conversation
- Browse your institution's S3 buckets (SIS exports, financial aid files, research data) without console access
- Preview a dataset's schema and sample rows before committing to a load
- Automatically register loaded datasets with the clAWS pipeline so Compute jobs can use them via a `claws://` URI
- Keep the catalog of 500+ public datasets current without manual maintenance

## What You Get

**Five tools** in Quick Suite's chat interface:

| Tool | What it does |
|------|-------------|
| `roda_search` | Search 500+ public datasets by keyword, tag, or data format |
| `roda_load` | Load a public dataset into Quick Sight as a SPICE dataset |
| `s3_browse` | List files in your configured institutional S3 buckets |
| `s3_preview` | Sample rows and infer schema from any S3 file before loading |
| `s3_load` | Register an S3 path as a Quick Sight data source |

**Three supporting services** running in the background:

| Service | What it does |
|---------|-------------|
| `catalog-sync` | Syncs the RODA catalog (500+ dataset entries) into DynamoDB daily and in real time via SNS |
| `catalog-quality-check` | Weekly scan for stale or unreachable datasets; emits CloudWatch metrics and alarms |
| `claws-resolver` | Translates `claws://` URIs into Quick Sight dataset IDs for the Compute extension |

**The claws:// bridge.** Every time `roda_load` or `s3_load` registers a dataset, it
writes an entry to ClawsLookupTable — a DynamoDB table that maps a short `source_id`
to the Quick Sight dataset ID. The Compute extension uses this table to resolve
`claws://roda-ipeds-enrollment` or `claws://s3-financial-aid-2024` into the right
dataset without requiring anyone to copy-paste an ID. This is what makes a full
"find data → load → analyze" workflow possible in a single conversation.

## Architecture

```
Quick Suite conversation
        │  MCP Actions
        ▼
AgentCore Gateway (Lambda targets)
        │
    ┌───┴────────────────────────────────────────┐
    │                                            │
    ▼ Public data                  Institutional data ▼
roda_search                            s3_browse
roda_load                              s3_preview
    │                                  s3_load
    │                                      │
    └──────────────┬───────────────────────┘
                   ▼
         Quick Sight dataset
         ClawsLookupTable (DynamoDB)
                   │
                   │  claws:// URI
                   ▼
         Compute extension
         clAWS excavation pipeline
```

```
Background services (not user-facing):

  EventBridge (daily)
  SNS (real-time RODA updates)
        │
        ▼
  catalog-sync Lambda
        │
        ▼
  DynamoDB: roda-catalog
  (500+ dataset entries, searchable by tag/format/keyword)

  EventBridge (weekly)
        │
        ▼
  catalog-quality-check Lambda
        │
        ▼
  CloudWatch: StaleDatasets metric + alarm
```

## Quick Start

```bash
git clone https://github.com/scttfrdmn/quick-suite-data.git
cd quick-suite-data

uv sync --extra dev   # or: pip install -r requirements.txt

# Configure your institutional S3 sources (required before deploying)
cp config/sources.example.yaml config/sources.yaml
# Edit config/sources.yaml with your institution's S3 buckets

cdk bootstrap   # first time only, per account/region
cdk deploy
```

After deploying, seed the RODA catalog immediately (otherwise it waits until the daily sync):

```bash
aws lambda invoke \
  --function-name qs-data-catalog-sync \
  /dev/null
```

Register each tool Lambda as an AgentCore Gateway Lambda target. The `ToolArns`
CloudFormation output has all five ARNs as JSON:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text
```

## Configuring Institutional S3 Sources

Copy `config/sources.example.yaml` to `config/sources.yaml` and add your institution's
S3 buckets. The IAM policy is generated from this list at deploy time — `s3_browse` and
`s3_preview` can only reach buckets that are explicitly configured here.

```yaml
sources:
  - label: financial-aid
    bucket: qs-institutional-data
    prefix: financial-aid/
    description: Financial aid records and FAFSA processing data
    allowed_groups:
      - financial-aid-staff
      - institutional-research

  - label: student-outcomes
    bucket: qs-institutional-data
    prefix: student-outcomes/
    description: Graduation, retention, and transfer tracking
    allowed_groups:
      - institutional-research
      - provost-office
```

## Deployment Options

```bash
cdk deploy                                          # standard
cdk deploy -c enable_realtime_sync=true             # subscribe to RODA SNS for real-time updates
cdk deploy -c quicksight_region=us-west-2           # if Quick Sight is in a different region
cdk deploy -c agentcore_gateway_role_arn=arn:...    # AgentCore Gateway execution role
```

## What Data Can Be Loaded

Quick Sight can ingest CSV, TSV, JSON, and Parquet directly from S3. The `roda_load` and
`s3_load` tools support these formats. For other formats common in research data (NetCDF,
Zarr, VCF), the clAWS excavation pipeline can query those files directly using S3 Select
or Athena without needing to convert them first.

## Cost

| Component | Monthly Cost |
|-----------|-------------|
| DynamoDB (catalog + lookup table, on-demand) | ~$0.50 |
| Lambda (sync, search, load, preview, browse) | ~$0.25 |
| EventBridge rules | Free |
| **Infrastructure total** | **~$1/month** |
| Quick Sight SPICE ingestion | Standard Quick Sight pricing per GB |

## License

Apache-2.0 — Copyright 2026 Scott Friedman
