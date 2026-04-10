# Quick Suite Data

**Give Quick Suite access to 500+ public research datasets, institutional data warehouses, and scientific literature — without touching a data pipeline.**

Quick Suite can visualize data that's already in Quick Sight. But getting data *into*
Quick Sight has always required manual work: finding the dataset, downloading or
configuring S3 access, building a manifest file, clicking through the console to create
a data source, waiting for the SPICE import. For a researcher or an IR analyst, that
friction is a barrier to every new question.

This extension removes that barrier. It adds 15 tools to Quick Suite's chat interface
that let users find, preview, and load data by describing what they need in plain
language — public research datasets from the Registry of Open Data on AWS, institutional
data from S3, Snowflake, and Redshift, scientific literature from seven databases, and
research funding data from three federal sources.

## What Quick Suite Alone Can't Do Here

- Search the Registry of Open Data on AWS and load a dataset into Quick Sight in a single conversation
- Browse your institution's S3 buckets (SIS exports, financial aid files, research data) without console access
- Query Snowflake and Redshift data warehouses with parameterized, read-only SQL
- Search PubMed, bioRxiv, Semantic Scholar, arXiv, NIH Reporter, NSF Awards, and IPEDS from the chat interface
- Search across all registered sources at once with unified federated search
- Preview a dataset's schema, sample rows, and per-column quality metrics before committing to a load
- Automatically register loaded datasets with the clAWS pipeline so Compute jobs can use them via a `claws://` URI
- Keep the catalog of 500+ public datasets current without manual maintenance

## What You Get

**15 tool Lambdas** across five source categories:

| Category | Tool | What it does |
|----------|------|-------------|
| Public datasets | `roda_search` | Search 500+ public datasets by keyword, tag, or data format; quality scoring (freshness, schema completeness, last verified) |
| | `roda_load` | Load a public dataset into Quick Sight as a SPICE dataset; writes ClawsLookupTable |
| Institutional S3 | `s3_browse` | List files in configured institutional S3 buckets; supports source registry lookup |
| | `s3_preview` | Sample rows, infer schema, per-column quality metrics (null %, cardinality, duplicate %); file extension allowlist |
| | `s3_load` | Register an S3 path as a Quick Sight data source; multi-prefix support; writes ClawsLookupTable |
| Data warehouses | `snowflake_browse` | List tables in a Snowflake data source (SQL API v2, no vendor SDK) |
| | `snowflake_preview` | Sample rows + schema + quality metrics from a Snowflake table |
| | `snowflake_query` | Parameterized read-only SQL via Snowflake SQL API v2; `?` placeholders, mutation detection, 1000-row limit |
| | `redshift_browse` | List tables in a Redshift Serverless workgroup (Redshift Data API) |
| | `redshift_preview` | Sample rows + schema + quality metrics from a Redshift table |
| | `redshift_query` | Parameterized read-only SQL via Redshift Data API; `?` → `$N` rewriting, async poll, mutation detection |
| Research literature | `pubmed_search` | NCBI E-utilities (esearch + esummary); optional `NCBI_API_KEY`; recency quality score |
| | `biorxiv_search` | bioRxiv/medRxiv details API; `server=both` dual-call; last-30-days default window |
| | `semantic_scholar_search` | Semantic Scholar Graph API; citation + recency scoring; client-side filters for citations, year, fields of study |
| | `arxiv_search` | Atom/XML via `xml.etree.ElementTree`; `category_filter` support |
| | `reagent_search` | Addgene catalog API v2; requires `ADDGENE_API_KEY` |
| Research funding | `ipeds_search` | Urban Institute Education Data Portal API (public, no auth); survey, year range filters |
| | `nih_reporter_search` | NIH Reporter v2 API (public, no auth); fiscal year, institution, PI name filters |
| | `nsf_awards_search` | NSF Award Search API (public, no auth); date range, PI name filters |
| | `research_search` | Zenodo + Figshare public API search with 429 exponential backoff |
| Cross-source | `federated_search` | Unified search across all 12+ registered source types; keyword scoring; `data_classification_filter`; `skipped_sources` reporting |

**Per-caller credential isolation:** Snowflake and Redshift tools accept optional
`caller_secret_arn` validated against an ARN prefix allowlist — no shared service account
required.

**Data quality metrics:** `s3_preview`, `snowflake_preview`, and `redshift_preview`
responses include a `quality` block with `null_pct`, `estimated_cardinality`, and
`duplicate_row_pct` per column.

**Non-native format handling:** Non-native formats (.nc, .h5, .pdf, .geojson) return a
`suggested_profile` pointing to the appropriate Compute ingest profile instead of failing.

**Five internal Lambdas** running in the background:

| Service | What it does |
|---------|-------------|
| `catalog-sync` | Syncs the RODA catalog (500+ dataset entries) into DynamoDB daily and in real time via SNS |
| `catalog-quality-check` | Weekly scan for stale or unreachable datasets; emits `StaleDatasets`/`UnreachableDatasets` CloudWatch metrics |
| `claws-resolver` | Translates `claws://` URIs into Quick Sight dataset IDs for the Compute extension |
| `register-source` | Writes entries to the `qs-data-source-registry` DynamoDB table; format validated per source type |
| `register-memory-source` | Registers clAWS memory NDJSON as QuickSight datasets; idempotent via `qs-claws-memory-registry` table |

**The claws:// bridge.** Every time `roda_load` or `s3_load` registers a dataset, it
writes an entry to ClawsLookupTable — a DynamoDB table that maps a short `source_id`
to the Quick Sight dataset ID. The Compute extension uses this table to resolve
`claws://roda-ipeds-enrollment` or `claws://s3-financial-aid-2024` into the right
dataset without requiring anyone to copy-paste an ID.

**Source registry.** The `qs-data-source-registry` DynamoDB table tracks approved data
sources. clAWS's `discover` tool queries it when searching the `registry` domain.
Compute results auto-register here after successful jobs, making analysis output
discoverable via `federated_search`.

## Architecture

```
Quick Suite conversation
        │  MCP Actions
        ▼
AgentCore Gateway (Lambda targets)
        │
    ┌───┴──────────────────┬─────────────────────┬────────────────────┐
    │                      │                     │                    │
    ▼ Public data     Institutional ▼       Warehouses ▼        Literature ▼
roda_search          s3_browse            snowflake_browse     pubmed_search
roda_load            s3_preview           snowflake_preview    biorxiv_search
                     s3_load              snowflake_query      semantic_scholar_search
                                          redshift_browse      arxiv_search
                                          redshift_preview     reagent_search
                                          redshift_query       ipeds_search
                                                               nih_reporter_search
                                                               nsf_awards_search
                                                               research_search
    │                      │                     │                    │
    └──────────────────────┴─────────┬───────────┴────────────────────┘
                                     │
                              federated_search
                              (cross-source unified search)
                                     │
                                     ▼
                           Quick Sight dataset
                           ClawsLookupTable (DynamoDB)
                           Source Registry (DynamoDB)
                                     │
                                     │  claws:// URI
                                     ▼
                           Compute extension
                           clAWS excavation pipeline
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
CloudFormation output has all ARNs as JSON:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text
```

## Configuring Institutional S3 Sources

Copy `config/sources.example.yaml` → `config/sources.yaml` and populate with
your institution's S3 buckets. The IAM policy is generated from this list —
`s3_browse` and `s3_preview` can only reach buckets that are explicitly configured.

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
cdk deploy -c enable_kms=true                       # customer-managed KMS encryption
cdk deploy -c roda_bucket_arns=arn:...              # narrow RODA S3 read access
cdk deploy -c register_source_admin_arn=arn:...     # restrict register-source invocation
```

## Security

- **S3 IAM scoping**: RODA loader read-only; institutional tools scoped to configured buckets; no PutObject on wildcards
- **SSRF prevention**: catalog quality-check validates S3 bucket names against naming rules before `head_bucket`
- **File extension allowlist**: s3_preview only reads `.parquet`, `.csv`, `.tsv`, `.json`, `.jsonl`, `.ndjson`, and `.gz` variants
- **Error sanitization**: no bucket names, ARNs, account IDs, or exception details in user-facing responses
- **DynamoDB protection**: catalog and source registry tables have deletion protection, PITR, and RETAIN removal policy
- **Mutation detection**: Snowflake and Redshift query tools reject INSERT/UPDATE/DELETE/DROP/CREATE/TRUNCATE/ALTER

## What Data Can Be Loaded

Quick Sight can ingest CSV, TSV, JSON, and Parquet directly from S3. The `roda_load` and
`s3_load` tools support these formats. Non-native formats (NetCDF, HDF5, PDF, GeoJSON)
return a `suggested_profile` pointing to the appropriate Compute ingest profile for
conversion. Snowflake and Redshift data is queried in place — no loading step required.

## Cost

| Component | Monthly Cost |
|-----------|-------------|
| DynamoDB (catalog + lookup + registry tables, on-demand) | ~$1.00 |
| Lambda (all tool + internal functions) | ~$0.50 |
| EventBridge rules | Free |
| **Infrastructure total** | **~$2/month** |
| Quick Sight SPICE ingestion | Standard Quick Sight pricing per GB |

## License

Apache-2.0 — Copyright 2026 Scott Friedman
