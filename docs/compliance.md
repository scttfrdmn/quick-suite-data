# Quick Suite Open Data — Compliance Guide

Target audience: health science school IT administrators deploying Quick Suite
in environments subject to HIPAA or institutional data governance requirements.

---

## Enabling KMS Encryption

By default, DynamoDB tables use AWS-managed keys and the S3 manifest bucket
uses SSE-S3. To use customer-managed KMS keys for all three resources (RODA
catalog table, source registry table, and manifest bucket), set the
`enable_kms` CDK context flag:

```bash
cdk deploy --context enable_kms=true
```

When `enable_kms=true` is set:

- A new KMS key (`alias/qs-open-data-data-key`) is created with automatic
  annual rotation enabled and a `RETAIN` removal policy to protect data
  after stack deletion.
- The RODA catalog DynamoDB table, source registry DynamoDB table, and S3
  manifest bucket are all encrypted with this key.
- All Lambda execution roles are automatically granted `kms:Decrypt` and
  `kms:GenerateDataKey` permissions on the key.

To rotate or restrict access to the key, use the AWS KMS console or CLI and
update the key policy. Do not delete the key while encrypted data exists.

---

## VPC S3 Endpoint Recommendation

For PHI or restricted data environments, route all S3 traffic through a VPC
gateway endpoint to avoid data leaving your VPC over the public internet:

1. In your VPC, create an S3 gateway endpoint:
   ```bash
   aws ec2 create-vpc-endpoint \
     --vpc-id vpc-XXXXXX \
     --service-name com.amazonaws.us-east-1.s3 \
     --route-table-ids rtb-XXXXXX
   ```

2. Attach your Lambda functions to the VPC (CDK `vpc` property on each
   `lambda.Function`) so they use the endpoint automatically.

3. Add a bucket policy condition requiring `aws:sourceVpce` to enforce that
   S3 access only comes through the endpoint, blocking public paths.

These steps are outside the CDK stack and must be applied to your networking
layer before deploying data tools in a HIPAA context.

---

## Data Classification Tagging

The source registry (`qs-data-source-registry` DynamoDB table) stores a
`data_classification` attribute for each source. Valid values, in order of
sensitivity (lowest to highest): `public`, `internal`, `restricted`, `phi`.

When registering a source, always set `data_classification` to the most
sensitive level present in the data. For health science data:

- `public` — de-identified aggregate statistics, published datasets (e.g.,
  CDC public surveillance data, RODA genomics datasets)
- `internal` — institutional data not intended for public release (e.g.,
  research cohort metadata, enrollment statistics)
- `restricted` — data with contractual or regulatory access controls (e.g.,
  limited data sets under a DUA, IRB-protected research data)
- `phi` — Protected Health Information as defined by HIPAA (e.g., EHR
  extracts, claims data, patient-level clinical records)

To register a PHI source using the `register-source` Lambda:

```json
{
  "source_id": "ehr-redshift-prod",
  "type": "redshift",
  "display_name": "EHR Redshift Serverless",
  "description": "De-identified inpatient encounter data",
  "data_classification": "phi",
  "connection_config": "{\"workgroup\": \"ehr-wg\", \"database\": \"clinical\"}"
}
```

Once registered, `federated_search` and `s3_browse` will only return this
source to callers whose `caller_clearance` field is `phi`. Callers without
an explicit clearance default to `public` and will not see the source.

---

## Recommended Source Registry Setup for Health Science Data

For a typical health science school environment:

1. Register public RODA datasets (e.g., CDC, NCBI) with
   `data_classification: public`. These are accessible to all Quick Suite
   users.

2. Register institutional research S3 buckets with
   `data_classification: internal` or `restricted` depending on IRB and
   data use agreement terms.

3. Register clinical or claims data sources (Redshift or Snowflake) with
   `data_classification: phi`. Limit access to roles with verified HIPAA
   training completion.

4. Set `caller_clearance` in AgentCore Gateway tool calls based on the
   authenticated user's role. Map your IdP groups (e.g., `phi-researchers`)
   to clearance levels in your Gateway OAuth configuration.

5. Enable KMS (`enable_kms=true`) for all PHI and restricted environments.
   Store the KMS key ARN in your change management system.

---

## Cross-Reference: Router Compliance Guide

The model router (`quick-suite-router`) applies Bedrock Guardrails to all
external provider responses. For HIPAA environments, configure the Guardrail
to block PII and PHI passthrough. See the router's compliance notes in
[quick-suite-router/docs/](../../quick-suite-router/docs/) for guardrail
configuration and `apply_guardrail_safe()` behavior.
