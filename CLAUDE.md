# CLAUDE.md — quick-suite-data (v0.13.0)

## What This Is

AgentCore Gateway Lambda targets that give Quick Suite users natural
language access to public datasets (RODA), institutional data (S3,
Snowflake, Redshift), research APIs (IPEDS, NIH Reporter, NSF Awards),
scientific literature (PubMed, bioRxiv, Semantic Scholar, arXiv),
reagent catalogs (Addgene), and repository search (Zenodo, Figshare).
Unified cross-source discovery via `federated_search`.

## Tool Lambdas

| Tool | Lambda | What it does |
|------|--------|-------------|
| `roda_search` | lambdas/roda-search/handler.py | Search 500+ public datasets from RODA; tag-based GSI query + keyword ranking; `exclude_deprecated` filter; pagination; `quality_score` on every result |
| `roda_load` | lambdas/dataset-loader/handler.py | Load a RODA public dataset into Quick Sight; writes `ClawsLookupTable` |
| `s3_browse` | lambdas/s3-browse/handler.py | Browse configured institutional S3 sources; reads from `qs-data-source-registry` when `use_source_registry=true` |
| `s3_preview` | lambdas/s3-preview/handler.py | Sample rows + schema inference from S3 file; file extension allowlist enforced; `quality` block with `null_pct`, `estimated_cardinality`, `duplicate_row_pct` per column |
| `s3_load` | lambdas/s3-load/handler.py | Register S3 path as Quick Sight data source; multi-prefix support; writes `ClawsLookupTable` |
| `snowflake_browse` | lambdas/snowflake-browse/handler.py | List tables in a Snowflake data source (SQL API v2, no vendor SDK) |
| `snowflake_preview` | lambdas/snowflake-preview/handler.py | Sample rows + schema from a Snowflake table; `quality` block |
| `snowflake_query` | lambdas/snowflake-query/handler.py | Parameterized read-only SQL via Snowflake SQL API v2; `?` placeholders, positional bindings, mutation detection, max 1000 rows |
| `redshift_browse` | lambdas/redshift-browse/handler.py | List tables in a Redshift Serverless workgroup (Redshift Data API) |
| `redshift_preview` | lambdas/redshift-preview/handler.py | Sample rows + schema from a Redshift table; `quality` block |
| `redshift_query` | lambdas/redshift-query/handler.py | Parameterized read-only SQL via Redshift Data API; `?` -> `$N` rewriting, async poll, mutation detection, max 1000 rows |
| `federated_search` | lambdas/federated-search/handler.py | Unified search across all 14 registered source types; keyword scoring; `data_classification_filter`; `skipped_sources` |
| `ipeds_search` | lambdas/ipeds-search/handler.py | Search IPEDS via Urban Institute Education Data Portal API (public, no auth); `survey`, `year_range`, `max_results` |
| `nih_reporter_search` | lambdas/nih-reporter-search/handler.py | Search NIH Reporter v2 API (public, no auth); `fiscal_year`, `institution`, `pi_name` filters |
| `nsf_awards_search` | lambdas/nsf-awards-search/handler.py | Search NSF Award Search API (public, no auth); `date_start`, `date_end`, `pi_name` filters |
| `pubmed_search` | lambdas/pubmed-search/handler.py | NCBI E-utilities esearch+esummary; optional `NCBI_API_KEY`; recency quality score |
| `biorxiv_search` | lambdas/biorxiv-search/handler.py | bioRxiv/medRxiv details API; `server=both` dual-call; last-30-days default |
| `semantic_scholar_search` | lambdas/semantic-scholar-search/handler.py | Semantic Scholar Graph API; optional `SEMANTIC_SCHOLAR_API_KEY`; citation+recency quality score; client-side filters |
| `arxiv_search` | lambdas/arxiv-search/handler.py | Atom/XML via `xml.etree.ElementTree`; `category_filter` |
| `reagent_search` | lambdas/reagent-search/handler.py | Addgene catalog API v2; requires `ADDGENE_API_KEY`; returns informational note when absent |
| `research_search` | lambdas/research-search/handler.py | Zenodo + Figshare public API search with 429 exponential backoff |

## Internal Lambdas

These are not AgentCore tools -- they support infrastructure and cross-stack integration.

| Lambda | Directory | What it does |
|--------|-----------|-------------|
| `catalog-sync` | lambdas/catalog-sync/handler.py | Syncs RODA NDJSON catalog into DynamoDB daily (+ SNS real-time) |
| `catalog-quality-check` | lambdas/catalog-quality-check/handler.py | Weekly scan; flags stale/unreachable datasets; writes `last_verified` + `quality_score`; emits `StaleDatasets`/`UnreachableDatasets` CW metrics |
| `claws-resolver` | lambdas/claws-resolver/handler.py | Resolves `claws://` source URIs to Quick Sight dataset IDs via `ClawsLookupTable` |
| `register-source` | lambdas/register-source/handler.py | Writes entries to `qs-data-source-registry` DynamoDB table |
| `register-memory-source` | lambdas/memory/handler.py | Registers clAWS memory NDJSON as Quick Sight dataset; idempotent via `qs-claws-memory-registry` DynamoDB table |

## AgentCore Lambda Target Pattern

All tool Lambdas are invoked directly by AgentCore Gateway:
- **Event** = tool arguments dict (no API Gateway envelope)
- **Return** = plain Python dict (no `statusCode`/`body` wrapping)
- Tool name arrives via `context.client_context.custom['bedrockAgentCoreToolName']`
  (format: `{target_name}___{tool_name}`) but is not currently used since each
  Lambda handles one tool.

```python
def handler(event: dict, context) -> dict:
    slug = event.get('slug', '')   # tool arguments come directly as event
    ...
    return {'status': 'loaded', 'datasetId': ds_id, ...}  # plain dict
```

## Key Data Structures

### ClawsLookupTable

DynamoDB table (`source_id` PK -> `dataset_id`). Written by `roda_load` and
`s3_load`. Read by `claws-resolver`. Enables the clAWS bridge between Open
Data and Compute (`claws://` URI scheme).

### Source Registry

`qs-data-source-registry` DynamoDB table. Stores institutional data source
configurations. SSM param `/quick-suite/data/source-registry-arn` published
for clAWS catalog-aware discover integration. Written by `register-source`.
Read by `s3_browse` (when `use_source_registry=true`) and `federated_search`.

### Memory Registry

`qs-claws-memory-registry` DynamoDB table (PK: `user_arn_hash`, SK:
`dataset_type`). Used by `register-memory-source` for idempotency when
registering clAWS memory NDJSON as Quick Sight datasets. Deletion protection
and PITR enabled.

## Per-Caller Credential Isolation

All four Snowflake and Redshift browse/preview tools accept optional
`caller_secret_arn`. Validated against `arn:aws:secretsmanager:` format and
`CALLER_SECRETS_ALLOWED_ARNS` prefix allowlist. Falls back to shared service
account when not provided.

## Data Quality Metrics

`s3_preview`, `snowflake_preview`, and `redshift_preview` responses include a
`quality` block with per-column metrics: `null_pct`, `estimated_cardinality`,
`duplicate_row_pct`.

## Non-Native Format Handling

`requires_transform` dispatch: non-native formats (`.nc`, `.h5`, `.pdf`,
`.geojson`) return `suggested_profile` pointing to a compute ingest profile
instead of failing silently.

## Security Hardening

- **S3 IAM scoping:** RODA loader wildcard read documented; institutional tools
  scoped to configured buckets; `roda_bucket_arns` CDK context for narrowing
- **SSRF prevention:** catalog quality-check validates S3 bucket names from ARNs
  against naming rules before `head_bucket`
- **s3_preview allowlist:** `.parquet`, `.csv`, `.tsv`, `.json`, `.jsonl`,
  `.ndjson`, `.gz` variants; extension validated before any S3 read
- **register-source auth:** `connection_config` format validated per source type;
  CDK resource policy via `register_source_admin_arn` context
- **Error sanitization:** s3_browse, s3_preview, redshift_browse, snowflake_browse
  return generic messages; no bucket names, ARNs, account IDs, or exception details
- **Redshift workgroup** removed from responses; error messages sanitized
- **DynamoDB protection:** `catalog_table` and `source_registry_table` have
  `deletion_protection=True`, `point_in_time_recovery=True`; catalog removal
  policy set to RETAIN
- **Mutation detection:** `snowflake_query` and `redshift_query` reject
  INSERT/UPDATE/DELETE/DROP/CREATE/TRUNCATE/ALTER statements

## File Map

```
quick-suite-data/
├── app.py                                     CDK entry point
├── cdk.json                                   CDK config + context flags
├── pyproject.toml                             uv / Python project config
├── requirements.txt                           CDK dependencies
├── CLAUDE.md                                  this file
├── stacks/
│   └── open_data_stack.py                     Python CDK stack
├── lambdas/
│   ├── catalog-sync/handler.py                Internal: sync RODA NDJSON -> DynamoDB
│   ├── catalog-quality-check/handler.py       Internal: weekly quality scan
│   ├── claws-resolver/handler.py              Internal: claws:// URI resolution
│   ├── register-source/handler.py             Internal: write to source registry
│   ├── memory/handler.py                      Internal: register-memory-source
│   ├── roda-search/handler.py                 Tool: roda_search
│   ├── dataset-loader/handler.py              Tool: roda_load
│   ├── s3-browse/handler.py                   Tool: s3_browse
│   ├── s3-preview/handler.py                  Tool: s3_preview
│   ├── s3-load/handler.py                     Tool: s3_load
│   ├── snowflake-browse/handler.py            Tool: snowflake_browse
│   ├── snowflake-preview/handler.py           Tool: snowflake_preview
│   ├── snowflake-query/handler.py             Tool: snowflake_query
│   ├── redshift-browse/handler.py             Tool: redshift_browse
│   ├── redshift-preview/handler.py            Tool: redshift_preview
│   ├── redshift-query/handler.py              Tool: redshift_query
│   ├── federated-search/handler.py            Tool: federated_search
│   ├── ipeds-search/handler.py                Tool: ipeds_search
│   ├── nih-reporter-search/handler.py         Tool: nih_reporter_search
│   ├── nsf-awards-search/handler.py           Tool: nsf_awards_search
│   ├── pubmed-search/handler.py               Tool: pubmed_search
│   ├── biorxiv-search/handler.py              Tool: biorxiv_search
│   ├── semantic-scholar-search/handler.py     Tool: semantic_scholar_search
│   ├── arxiv-search/handler.py                Tool: arxiv_search
│   ├── reagent-search/handler.py              Tool: reagent_search
│   ├── research-search/handler.py             Tool: research_search
│   ├── pyarrow-layer/                         Lambda Layer: pyarrow
│   └── common/python/
│       └── data_utils.py                      Shared: detect_formats(), schema inference
├── config/
│   ├── roda-tools.yaml                        Tool schemas for roda_search + roda_load
│   ├── s3-tools.yaml                          Tool schemas for s3_browse/preview/load
│   └── sources.example.yaml                   Institutional sources config template
└── tests/
    ├── test_roda_search.py
    ├── test_dataset_loader.py
    ├── test_s3_tools.py
    ├── test_snowflake_tools.py
    ├── test_redshift_tools.py
    ├── test_federated_search.py
    ├── test_catalog_sync.py
    ├── test_catalog_quality_check.py
    ├── test_source_registry.py
    ├── test_security.py
    ├── test_research_sources.py
    ├── test_literature_sources.py
    ├── test_memory_source.py
    └── test_quality_sources.py
```

## CDK Context Variables

| Key | Default | Description |
|-----|---------|-------------|
| `quicksight_region` | current region | Region for Quick Sight API calls |
| `enable_realtime_sync` | false | Subscribe to RODA SNS topic |
| `manifest_bucket_name` | auto | S3 bucket name for manifests |
| `agentcore_gateway_role_arn` | -- | Gateway execution role ARN for Lambda invoke permissions |
| `enable_kms` | false | Encrypt DynamoDB tables and S3 buckets with customer-managed KMS keys |
| `use_source_registry` | false | Enable `s3_browse` reads from `qs-data-source-registry` |
| `roda_bucket_arns` | -- | Comma-separated ARNs to narrow RODA S3 read access |
| `register_source_admin_arn` | -- | IAM ARN for `register-source` Lambda resource policy |
| `caller_secrets_allowed_arns` | -- | Comma-separated Secrets Manager ARN prefixes for per-caller credential isolation |

Set via `cdk deploy --context agentcore_gateway_role_arn=arn:aws:iam::...`

## Environment Variables (API Keys)

These optional environment variables enable higher rate limits or access to
gated APIs. All corresponding tools degrade gracefully when keys are absent.

| Variable | Used by | Notes |
|----------|---------|-------|
| `NCBI_API_KEY` | `pubmed_search` | Higher rate limit for NCBI E-utilities |
| `SEMANTIC_SCHOLAR_API_KEY` | `semantic_scholar_search` | Higher rate limit for Semantic Scholar Graph API |
| `ADDGENE_API_KEY` | `reagent_search` | Required for Addgene catalog API v2; returns informational note when absent |

## Institutional Sources Config

Copy `config/sources.example.yaml` -> `config/sources.yaml` and populate with
your institution's S3 buckets before deploying. The IAM policy is generated
from this list -- the LLM can ONLY access configured buckets.

```bash
cp config/sources.example.yaml config/sources.yaml
# edit config/sources.yaml
cdk deploy
```

## Deploy

```bash
uv sync                          # install deps
uv run cdk synth                 # validate
uv run cdk deploy                # deploy
```

Or with pip:
```bash
pip install -r requirements.txt
cdk deploy
```

## Post-Deploy: Register with AgentCore Gateway

After `cdk deploy`, register each tool Lambda as an AgentCore Gateway Lambda target.
The `ToolArns` CloudFormation output contains all ARNs as JSON:

```bash
aws cloudformation describe-stacks \
  --stack-name QuickSuiteOpenData \
  --query 'Stacks[0].Outputs[?OutputKey==`ToolArns`].OutputValue' \
  --output text
```

Register each ARN via the AgentCore Gateway console or API. The
`MemoryRegistrarArn` output is also available for cross-stack invocation
by clAWS.

## Code Conventions

- Python 3.12, boto3 + stdlib only. No vendor SDKs.
- Lambda handlers return plain dicts -- no API Gateway envelope.
- `data_utils.py` is a Lambda Layer shared across Lambdas.
- Error conditions return `{'error': 'message'}` (not raised exceptions).
- Structured JSON logging at INFO level.
- All Quick Sight API calls use `boto3.client('quicksight')`.
  User-facing text uses "Quick Sight" (BI tool) or "Quick Suite" (platform).

## Tests

372 unit tests. Run with:

```bash
uv run python -m pytest tests/ -v
```

Substrate integration tests in CI (see capstone `CLAUDE.md` for details).

## Project Tracking

Work is tracked in GitHub -- not in local files. Do not add TODO lists or task
tracking to CLAUDE.md or create TODO.md files.

- **Issues:** https://github.com/scttfrdmn/campus-data/issues
- **Milestones:** https://github.com/scttfrdmn/campus-data/milestones
- **Project board:** https://github.com/users/scttfrdmn/projects/45
- **Changelog:** CHANGELOG.md (keepachangelog format, semver 2.0)

To report a bug or propose a feature, open a GitHub Issue with the appropriate
label. All release planning happens via milestones.
