# CLAUDE.md — quick-suite-data

## What This Is

Five AgentCore Gateway Lambda targets that give Quick Suite users natural
language access to public datasets (RODA) and institutional S3 data:

| Tool | Lambda | What it does |
|------|--------|-------------|
| `roda_search` | lambdas/roda-search/handler.py | Search 500+ public datasets from RODA |
| `roda_load` | lambdas/dataset-loader/handler.py | Load a public dataset into Quick Sight |
| `s3_browse` | lambdas/s3-browse/handler.py | List files in configured institutional S3 sources |
| `s3_preview` | lambdas/s3-preview/handler.py | Sample rows and infer schema from an S3 file |
| `s3_load` | lambdas/s3-load/handler.py | Register an S3 path as a Quick Sight data source |

Plus one non-tool Lambda:
- `catalog-sync` — syncs the RODA NDJSON catalog into DynamoDB; triggered by EventBridge (daily) and optionally SNS (real-time).

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

## File Map

```
quick-suite-data/
├── app.py                          CDK entry point
├── cdk.json                        CDK config + context flags
├── pyproject.toml                  uv / Python project config
├── requirements.txt                CDK dependencies
├── CLAUDE.md                       this file
├── stacks/
│   └── open_data_stack.py          Python CDK stack
├── lambdas/
│   ├── catalog-sync/handler.py     Sync RODA NDJSON → DynamoDB
│   ├── roda-search/handler.py      Tool: roda_search
│   ├── dataset-loader/handler.py   Tool: roda_load
│   ├── s3-browse/handler.py        Tool: s3_browse
│   ├── s3-preview/handler.py       Tool: s3_preview
│   ├── s3-load/handler.py          Tool: s3_load
│   └── common/python/
│       └── data_utils.py           Shared: detect_formats(), schema inference
├── config/
│   ├── roda-tools.yaml             Tool schemas for roda_search + roda_load
│   ├── s3-tools.yaml               Tool schemas for s3_browse/preview/load
│   └── sources.example.yaml        Institutional sources config template
└── tests/                          pytest suite
```

## CDK Context Variables

| Key | Default | Description |
|-----|---------|-------------|
| `quicksight_region` | current region | Region for Quick Sight API calls |
| `enable_realtime_sync` | false | Subscribe to RODA SNS topic |
| `manifest_bucket_name` | auto | S3 bucket name for manifests |
| `agentcore_gateway_role_arn` | — | Gateway execution role ARN for Lambda invoke permissions |

Set via `cdk deploy --context agentcore_gateway_role_arn=arn:aws:iam::...`

## Institutional Sources Config

Copy `config/sources.example.yaml` → `config/sources.yaml` and populate with
your institution's S3 buckets before deploying. The IAM policy is generated
from this list — the LLM can ONLY access configured buckets.

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

Register each ARN via the AgentCore Gateway console or API.

## Code Conventions

- Python 3.12, boto3 + stdlib only. No vendor SDKs.
- Lambda handlers return plain dicts — no API Gateway envelope.
- `data_utils.py` is a Lambda Layer shared across Lambdas.
- Error conditions return `{'error': 'message'}` (not raised exceptions).
- Structured JSON logging at INFO level.
- All Quick Sight API calls use `boto3.client('quicksight')`.
  User-facing text uses "Quick Sight" (BI tool) or "Quick Suite" (platform).

## Project Tracking

Work is tracked in GitHub — not in local files. Do not add TODO lists or task
tracking to CLAUDE.md or create TODO.md files.

- **Issues:** https://github.com/scttfrdmn/quick-suite-data/issues
- **Milestones:** https://github.com/scttfrdmn/quick-suite-data/milestones
- **Project board:** https://github.com/users/scttfrdmn/projects/45
- **Changelog:** CHANGELOG.md (keepachangelog format, semver 2.0)

To report a bug or propose a feature, open a GitHub Issue with the appropriate
label. All release planning happens via milestones.
