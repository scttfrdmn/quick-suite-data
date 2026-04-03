# Open Data Example: RODA Discovery and Load

A self-contained quick-suite-data example showing the data discovery and
loading pipeline using the Registry of Open Data on AWS (RODA).

## What This Shows

- `roda_search` — keyword search across the 500+ dataset RODA catalog
- `roda_load` — register a public dataset as a Quick Sight data source
- `s3_browse` — confirm the source S3 bucket is accessible

## Data

Uses NOAA GHCN (Global Historical Climatology Network Daily) — a public
dataset in the Registry of Open Data on AWS. No data transfer costs; the
S3 bucket is publicly readable.

## Prerequisites

- `QuickSuiteOpenData` stack deployed (includes `catalog-sync` Lambda
  which must have run at least once to populate the RODA DynamoDB catalog)
- `AWS_PROFILE` pointing to the deployment account (region: us-west-2)
- Quick Sight enabled in the account with a valid subscription

## Running

```bash
# Via the capstone scenario runner:
AWS_PROFILE=aws QS_SCENARIO_REGION=us-west-2 \
  python3 -m pytest tests/scenarios/ -v -m scenario -k roda-discovery

# Trigger a catalog sync manually if the catalog is empty:
aws lambda invoke --function-name qs-data-catalog-sync \
  --region us-west-2 --payload '{}' /tmp/sync.json && cat /tmp/sync.json
```

## Notes

`roda_load` writes a record to `qs-open-data-claws-lookup` DynamoDB table,
enabling `claws://` URI resolution from quick-suite-compute. After running
this example, you can start a compute job with:

```
source_uri: claws://{source_id from load step}
```
