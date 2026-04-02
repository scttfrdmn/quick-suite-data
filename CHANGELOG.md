# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.2] - 2026-04-01

### Fixed
- Integration tests unblocked by Substrate 0.45.5: QuickSight `create_data_source` / `describe_data_source` path routing now correct (`data-sources` with hyphen), and DynamoDB `Scan` with empty-string top-level attributes now round-trips correctly — all 7 previously skipped QuickSight integration tests now pass

## [0.4.1] - 2026-04-01

### Added
- Unit tests for `roda_load` join path: `TestDatasetLoaderJoin` (6 tests) — happy path with `LogicalTableMap`, join slug not found, no files for join slug, join data-source failure → manifest-ready, join_slug without join_key ignored, join_key without join_slug ignored
- Unit tests for `roda_load` suggestions: `TestDatasetLoaderSuggestions` (4 tests) — primary-tag GSI query, cap at 5 results, no primaryTag → empty list, GSI error is nonfatal
- Unit tests for `s3_load` multi-prefix: `TestS3LoadMultiPrefix` (6 tests) — two prefixes combined, single prefix backward compat, path-traversal rejection, prefixes takes priority over prefix, sample_only caps total files, empty prefixes falls back to single
- Integration tests for `roda_search`: `TestRodaSearchIntegration` (5 tests) — empty query scan, single-tag GSI query, keyword search, max_results, format filter; seeds Substrate DynamoDB using low-level client with explicit DynamoDB wire format (workaround for scttfrdmn/substrate#254)

## [0.4.0] - 2026-04-01

### Added
- Dataset join support in `roda_load`: optional `join_slug` + `join_key` parameters load a second RODA dataset and join both into a single Quick Sight dataset using `LogicalTableMap` `JoinInstruction` (INNER join)
- Bulk prefix load in `s3_load`: optional `prefixes` list accepts multiple S3 key prefixes; files from all prefixes are combined into a single Quick Sight manifest and dataset; `prefixCount` field added to response
- Related dataset suggestions in `roda_load` response: `suggestions` list (up to 5) returns slug + name of datasets sharing the same primary tag, queried from the `by-primary-tag` GSI

### Changed
- Closed open-data #11 (real-time SNS catalog trigger already implemented in v0.3.0)

## [0.3.0] - 2026-04-01

### Added
- DynamoDB search result cache for `roda_search` with 1-hour TTL; cache key is SHA-256 of query + tags + format + max_results; skipped for empty queries and paginated requests
- Pagination support in `roda_search`: `pagination_token` input decoded to `ExclusiveStartKey`; `next_token` (base64-encoded `LastEvaluatedKey`) returned in every response
- `claws_source_id` field in `roda_load` responses (`roda-{slug}`) and `s3_load` responses (`s3-{normalized-label}`) for downstream clAWS handoff

### Changed
- `roda_search` ranking improvements: exact full-name keyword match adds 5× bonus per keyword; deprecated datasets penalised at 0.5× score multiplier; `daily` update-frequency datasets boosted 1.2×, `weekly` boosted 1.1×
- CI workflow CDK synth step uses `--no-asset-bundling` to skip Docker pyarrow layer build in CI

## [0.2.0] - 2026-04-01

### Added
- Parquet schema inference in `s3_preview` via Docker-built pyarrow Lambda layer (`lambdas/pyarrow-layer/Dockerfile`); attached to `s3-preview` Lambda only — other Lambdas are unaffected
- Post-deploy helper script (`scripts/post-deploy.sh`) — retrieves all tool Lambda ARNs from CloudFormation and prints AgentCore Gateway registration commands for all five tools
- AgentCore Gateway registration guide (`docs/agentcore-registration.md`) — console and CLI registration steps, invoke permission setup, and verification
- Institutional S3 sources configuration guide (`docs/sources-config.md`) — format reference, IAM security model, examples for university/lab/government configurations, and redeploy workflow

## [0.1.0] - 2026-04-01

### Added
- Daily catalog sync from RODA NDJSON into DynamoDB (`catalog-sync` Lambda), with optional real-time SNS trigger
- Dataset search across 500+ public AWS open datasets (`roda_search` AgentCore tool)
- One-click dataset loading into Quick Sight via S3 manifest (`roda_load` AgentCore tool)
- Institutional S3 bucket browsing with prefix navigation (`s3_browse` AgentCore tool)
- Schema inference and row sampling from S3 files (`s3_preview` AgentCore tool) — supports CSV, TSV, Parquet, JSON, NDJSON
- S3 path registration as Quick Sight data source (`s3_load` AgentCore tool)
- Shared `data_utils` Lambda layer for format detection and schema inference
- CDK stack with AgentCore Gateway Lambda target wiring and IAM policies scoped to configured sources

[unreleased]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/scttfrdmn/quick-suite-open-data/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scttfrdmn/quick-suite-open-data/releases/tag/v0.1.0
