"""
Quick Suite Open Data — CDK Stack

Deploys RODA public dataset tools and institutional S3 browser tools
as AgentCore Gateway Lambda targets.

Resources:
  - DynamoDB: RODA catalog (slug PK, by-primary-tag GSI)
  - S3: manifest storage bucket (7-day lifecycle)
  - Lambda: catalog-sync, roda-search, roda-load, s3-browse, s3-preview, s3-load
  - Lambda Layer: shared data utilities
  - EventBridge: daily catalog sync rule
  - SNS: optional real-time RODA update subscription
  - IAM: Quick Sight create/describe, S3 read on configured buckets
  - CfnOutputs: Lambda ARNs for AgentCore Gateway target registration

AgentCore Integration:
  After deployment, register each tool Lambda as an AgentCore Gateway
  Lambda target. The Gateway invokes Lambdas directly — no API Gateway needed.
  See README.md for step-by-step registration.
"""

import json
from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import (
    aws_cloudwatch as cw,
)
from aws_cdk import (
    aws_dynamodb as dynamodb,
)
from aws_cdk import (
    aws_events as events,
)
from aws_cdk import (
    aws_events_targets as targets,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_kms as kms,
)
from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as sns_subscriptions,
)
from aws_cdk import (
    aws_ssm as ssm,
)
from constructs import Construct

try:
    import yaml
    def _load_yaml(path):
        with open(path) as f:
            return yaml.safe_load(f)
except ImportError:
    def _load_yaml(path):
        raise RuntimeError("pyyaml required — run: pip install pyyaml")


class OpenDataStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        prefix = "qs-open-data"

        account_id = self.account
        qs_region = self.node.try_get_context("quicksight_region") or self.region
        qs_user = self.node.try_get_context("quicksight_user") or "Admin"
        enable_realtime_sync = bool(self.node.try_get_context("enable_realtime_sync"))
        manifest_bucket_name = self.node.try_get_context("manifest_bucket_name")

        # Load institutional sources config if present
        sources_path = Path(__file__).parent.parent / "config" / "sources.yaml"
        if not sources_path.exists():
            sources_path = Path(__file__).parent.parent / "config" / "sources.example.yaml"
        institutional_sources = []
        if sources_path.exists():
            config = _load_yaml(str(sources_path))
            institutional_sources = config.get("institutional_sources", [])

        enable_kms = bool(self.node.try_get_context("enable_kms"))

        # -----------------------------------------------------------------
        # KMS: Customer-managed key (enable_kms=true only)
        # -----------------------------------------------------------------
        cmk: kms.Key | None = None
        if enable_kms:
            cmk = kms.Key(
                self,
                "DataKey",
                alias=f"alias/{prefix}-data-key",
                description="CMK for Quick Suite Open Data DynamoDB tables and S3 manifest bucket",
                enable_key_rotation=True,
                removal_policy=RemovalPolicy.RETAIN,
            )

        # -----------------------------------------------------------------
        # DynamoDB: RODA Catalog
        # -----------------------------------------------------------------
        _ddb_encryption = (
            dynamodb.TableEncryption.CUSTOMER_MANAGED if enable_kms
            else dynamodb.TableEncryption.AWS_MANAGED
        )
        _ddb_kms_kwargs: dict = {"encryption": _ddb_encryption}
        if enable_kms and cmk:
            _ddb_kms_kwargs["encryption_key"] = cmk

        search_cache_table = dynamodb.Table(
            self,
            "RodaSearchCache",
            table_name=f"{prefix}-roda-search-cache",
            partition_key=dynamodb.Attribute(
                name="cache_key", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="ttl",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            **_ddb_kms_kwargs,
        )

        catalog_table = dynamodb.Table(
            self,
            "RodaCatalog",
            table_name=f"{prefix}-roda-catalog",
            partition_key=dynamodb.Attribute(
                name="slug", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            **_ddb_kms_kwargs,
        )

        catalog_table.add_global_secondary_index(
            index_name="by-primary-tag",
            partition_key=dynamodb.Attribute(
                name="primaryTag", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="name", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        claws_lookup_table = dynamodb.Table(
            self,
            "ClawsLookupTable",
            table_name=f"{prefix}-claws-lookup",
            partition_key=dynamodb.Attribute(
                name="source_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # -----------------------------------------------------------------
        # S3: Manifest Bucket
        # -----------------------------------------------------------------
        _s3_encryption = s3.BucketEncryption.KMS if enable_kms else s3.BucketEncryption.S3_MANAGED
        _s3_kms_kwargs: dict = {"encryption": _s3_encryption}
        if enable_kms and cmk:
            _s3_kms_kwargs["encryption_key"] = cmk

        manifest_bucket = s3.Bucket(
            self,
            "ManifestBucket",
            bucket_name=manifest_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(7))
            ],
            **_s3_kms_kwargs,
        )

        # -----------------------------------------------------------------
        # Shared Lambda Layer (data_utils.py)
        # -----------------------------------------------------------------
        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            code=lambda_.Code.from_asset("lambdas/common"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Shared data utilities (detect_formats, schema inference)",
        )

        # pyarrow layer — Docker-built binary layer for Parquet schema inference.
        # Attached to s3-preview only; other Lambdas use common_layer only.
        pyarrow_layer = lambda_.LayerVersion(
            self,
            "PyarrowLayer",
            code=lambda_.Code.from_docker_build(path="lambdas/pyarrow-layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="pyarrow for Parquet schema inference in s3_preview",
        )

        # -----------------------------------------------------------------
        # Lambda: Catalog Sync (EventBridge — not an AgentCore tool)
        # -----------------------------------------------------------------
        sync_fn = lambda_.Function(
            self,
            "CatalogSync",
            function_name=f"{prefix}-catalog-sync",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/catalog-sync"),
            layers=[common_layer],
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "TABLE_NAME": catalog_table.table_name,
                "RODA_BUCKET": "registry.opendata.aws",
                "RODA_PREFIX": "roda/ndjson/",
            },
        )
        catalog_table.grant_write_data(sync_fn)
        sync_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    "arn:aws:s3:::registry.opendata.aws",
                    "arn:aws:s3:::registry.opendata.aws/*",
                ],
            )
        )

        events.Rule(
            self,
            "DailySyncRule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(sync_fn)],
        )

        if enable_realtime_sync:
            roda_sns_arn = (
                self.node.try_get_context("roda_sns_arn")
                or "arn:aws:sns:us-east-1:652627389412:roda-object_created"
            )
            roda_topic = sns.Topic.from_topic_arn(self, "RodaTopic", roda_sns_arn)
            roda_topic.add_subscription(
                sns_subscriptions.LambdaSubscription(sync_fn)
            )

        # -----------------------------------------------------------------
        # Lambda: RODA Search (AgentCore tool: roda_search)
        # -----------------------------------------------------------------
        search_fn = lambda_.Function(
            self,
            "RodaSearch",
            function_name=f"{prefix}-roda-search",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/roda-search"),
            layers=[common_layer],
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "TABLE_NAME": catalog_table.table_name,
                "SEARCH_CACHE_TABLE": search_cache_table.table_name,
            },
        )
        catalog_table.grant_read_data(search_fn)
        search_cache_table.grant_read_write_data(search_fn)

        # -----------------------------------------------------------------
        # Lambda: RODA Load (AgentCore tool: roda_load)
        # -----------------------------------------------------------------
        loader_fn = lambda_.Function(
            self,
            "RodaLoader",
            function_name=f"{prefix}-roda-load",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/dataset-loader"),
            layers=[common_layer],
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                "TABLE_NAME": catalog_table.table_name,
                "MANIFEST_BUCKET": manifest_bucket.bucket_name,
                "QUICKSIGHT_ACCOUNT_ID": account_id,
                "QUICKSIGHT_REGION": qs_region,
                "QUICKSIGHT_USER": qs_user,
                "CLAWS_LOOKUP_TABLE": claws_lookup_table.table_name,
            },
        )
        catalog_table.grant_read_data(loader_fn)
        claws_lookup_table.grant_write_data(loader_fn)
        manifest_bucket.grant_read_write(loader_fn)
        loader_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "quicksight:CreateDataSource",
                    "quicksight:CreateDataSet",
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSet",
                    "quicksight:UpdateDataSource",
                    "quicksight:PassDataSource",
                ],
                resources=[f"arn:aws:quicksight:{qs_region}:{account_id}:*"],
            )
        )
        # Read from public RODA buckets
        loader_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=["arn:aws:s3:::*"],
            )
        )

        # -----------------------------------------------------------------
        # Lambda: S3 Browse (AgentCore tool: s3_browse)
        # -----------------------------------------------------------------
        s3_browse_env = {
            "SOURCES_CONFIG": json.dumps(
                [{"label": s["label"], "bucket": s["bucket"],
                  "prefix": s.get("prefix", ""),
                  "description": s.get("description", "")}
                 for s in institutional_sources]
            ),
        }

        browse_fn = lambda_.Function(
            self,
            "S3Browse",
            function_name=f"{prefix}-s3-browse",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/s3-browse"),
            layers=[common_layer],
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=s3_browse_env,
        )

        # -----------------------------------------------------------------
        # Lambda: S3 Preview (AgentCore tool: s3_preview)
        # -----------------------------------------------------------------
        preview_fn = lambda_.Function(
            self,
            "S3Preview",
            function_name=f"{prefix}-s3-preview",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/s3-preview"),
            layers=[common_layer, pyarrow_layer],
            timeout=Duration.seconds(30),
            memory_size=512,
            environment=s3_browse_env,
        )

        # -----------------------------------------------------------------
        # Lambda: S3 Load (AgentCore tool: s3_load)
        # -----------------------------------------------------------------
        s3_load_fn = lambda_.Function(
            self,
            "S3Load",
            function_name=f"{prefix}-s3-load",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/s3-load"),
            layers=[common_layer],
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                **s3_browse_env,
                "MANIFEST_BUCKET": manifest_bucket.bucket_name,
                "QUICKSIGHT_ACCOUNT_ID": account_id,
                "QUICKSIGHT_REGION": qs_region,
                "QUICKSIGHT_USER": qs_user,
                "CLAWS_LOOKUP_TABLE": claws_lookup_table.table_name,
            },
        )
        manifest_bucket.grant_read_write(s3_load_fn)
        claws_lookup_table.grant_write_data(s3_load_fn)
        s3_load_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "quicksight:CreateDataSource",
                    "quicksight:CreateDataSet",
                    "quicksight:DescribeDataSource",
                    "quicksight:PassDataSource",
                ],
                resources=[f"arn:aws:quicksight:{qs_region}:{account_id}:*"],
            )
        )

        # S3 read permissions: configured institutional buckets only
        tool_fns = [browse_fn, preview_fn, s3_load_fn]
        if institutional_sources:
            allowed_arns = []
            for src in institutional_sources:
                bucket = src["bucket"]
                pfx = src.get("prefix", "")
                allowed_arns.extend([
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/{pfx}*" if pfx else f"arn:aws:s3:::{bucket}/*",
                ])
            for fn in tool_fns:
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["s3:GetObject", "s3:ListBucket"],
                        resources=allowed_arns,
                    )
                )
        else:
            # No sources configured — grant no S3 access (tools will report misconfiguration)
            pass

        # -----------------------------------------------------------------
        # Lambda: clAWS Resolver (internal — not an AgentCore tool)
        # -----------------------------------------------------------------
        claws_resolver_fn = lambda_.Function(
            self,
            "ClawsResolver",
            function_name=f"{prefix}-claws-resolver",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/claws-resolver"),
            timeout=Duration.seconds(10),
            memory_size=128,
            environment={
                "CLAWS_LOOKUP_TABLE": claws_lookup_table.table_name,
            },
        )
        claws_lookup_table.grant_read_data(claws_resolver_fn)

        CfnOutput(
            self,
            "ClawsResolverArn",
            value=claws_resolver_fn.function_arn,
            description="clAWS resolver Lambda ARN — used by compute extract Lambda",
            export_name="qs-open-data-claws-resolver-arn",
        )

        # -----------------------------------------------------------------
        # Lambda: Catalog Quality Check (EventBridge weekly — not an AgentCore tool)
        # -----------------------------------------------------------------
        quality_check_fn = lambda_.Function(
            self,
            "CatalogQualityCheck",
            function_name=f"{prefix}-catalog-quality-check",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/catalog-quality-check"),
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "TABLE_NAME": catalog_table.table_name,
            },
        )
        catalog_table.grant_read_write_data(quality_check_fn)
        quality_check_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        events.Rule(
            self,
            "WeeklyQualityCheckRule",
            schedule=events.Schedule.rate(Duration.days(7)),
            targets=[targets.LambdaFunction(quality_check_fn)],
        )

        cw.Alarm(
            self,
            "StaleDatasetAlarm",
            alarm_name=f"{prefix}-stale-datasets",
            alarm_description="More than 10 stale datasets detected in the RODA catalog",
            metric=cw.Metric(
                namespace="QuickSuiteOpenData",
                metric_name="StaleDatasets",
                period=Duration.days(7),
                statistic="Sum",
            ),
            threshold=10,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        # -----------------------------------------------------------------
        # SSM exports for qs-discover unified discovery Lambda
        # Written at deploy time so shared/ stack can read without CFN dep.
        # -----------------------------------------------------------------
        ssm.StringParameter(
            self,
            "RodaSearchArnParam",
            parameter_name="/quick-suite/lambdas/roda-search-arn",
            string_value=search_fn.function_arn,
            description="roda-search Lambda ARN for qs-discover fan-out",
        )
        ssm.StringParameter(
            self,
            "S3BrowseArnParam",
            parameter_name="/quick-suite/lambdas/s3-browse-arn",
            string_value=browse_fn.function_arn,
            description="s3-browse Lambda ARN for qs-discover fan-out",
        )

        # -----------------------------------------------------------------
        # DynamoDB: Source Registry (Feature 14)
        # -----------------------------------------------------------------
        source_registry_table = dynamodb.Table(
            self,
            "SourceRegistry",
            table_name="qs-data-source-registry",
            partition_key=dynamodb.Attribute(
                name="source_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            **_ddb_kms_kwargs,
        )

        # CDK migration flag — when true, s3-browse loads sources from DynamoDB
        use_source_registry = self.node.try_get_context("use_source_registry") or False
        if use_source_registry:
            browse_fn.add_environment("USE_SOURCE_REGISTRY", "true")
            browse_fn.add_environment("SOURCE_REGISTRY_TABLE", source_registry_table.table_name)
            source_registry_table.grant_read_data(browse_fn)

        # SSM parameter for clAWS cross-stack integration
        ssm.StringParameter(
            self,
            "SourceRegistryArnParam",
            parameter_name="/quick-suite/data/source-registry-arn",
            string_value=source_registry_table.table_arn,
            description="Source registry DynamoDB table ARN for clAWS cross-stack integration",
        )

        # -----------------------------------------------------------------
        # Lambda: Register Source (internal — not an AgentCore tool)
        # -----------------------------------------------------------------
        register_source_fn = lambda_.Function(
            self,
            "RegisterSource",
            function_name=f"{prefix}-register-source",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/register-source"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "SOURCE_REGISTRY_TABLE": source_registry_table.table_name,
            },
        )
        source_registry_table.grant_write_data(register_source_fn)

        # -----------------------------------------------------------------
        # Lambda: Snowflake Browse + Preview (AgentCore tools)
        # -----------------------------------------------------------------
        snowflake_secret_arn = self.node.try_get_context("snowflake_secret_arn") or ""

        snowflake_browse_fn = lambda_.Function(
            self,
            "SnowflakeBrowse",
            function_name=f"{prefix}-snowflake-browse",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/snowflake-browse"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "SNOWFLAKE_SECRET_ARN": snowflake_secret_arn,
            },
        )

        snowflake_preview_fn = lambda_.Function(
            self,
            "SnowflakePreview",
            function_name=f"{prefix}-snowflake-preview",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/snowflake-preview"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "SNOWFLAKE_SECRET_ARN": snowflake_secret_arn,
            },
        )

        if snowflake_secret_arn:
            for fn in [snowflake_browse_fn, snowflake_preview_fn]:
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["secretsmanager:GetSecretValue"],
                        resources=[snowflake_secret_arn],
                    )
                )

        # -----------------------------------------------------------------
        # Lambda: Redshift Browse + Preview (AgentCore tools)
        # -----------------------------------------------------------------
        redshift_secret_arn = self.node.try_get_context("redshift_secret_arn") or ""

        redshift_browse_fn = lambda_.Function(
            self,
            "RedshiftBrowse",
            function_name=f"{prefix}-redshift-browse",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/redshift-browse"),
            timeout=Duration.seconds(60),
            memory_size=128,
            environment={
                "REDSHIFT_SECRET_ARN": redshift_secret_arn,
            },
        )

        redshift_preview_fn = lambda_.Function(
            self,
            "RedshiftPreview",
            function_name=f"{prefix}-redshift-preview",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/redshift-preview"),
            timeout=Duration.seconds(60),
            memory_size=128,
            environment={
                "REDSHIFT_SECRET_ARN": redshift_secret_arn,
            },
        )

        redshift_data_policy = iam.PolicyStatement(
            actions=[
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
            ],
            resources=["*"],
        )
        for fn in [redshift_browse_fn, redshift_preview_fn]:
            fn.add_to_role_policy(redshift_data_policy)
            if redshift_secret_arn:
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["secretsmanager:GetSecretValue"],
                        resources=[redshift_secret_arn],
                    )
                )

        # -----------------------------------------------------------------
        # Lambda: Federated Search (AgentCore tool)
        # -----------------------------------------------------------------
        federated_search_fn = lambda_.Function(
            self,
            "FederatedSearch",
            function_name=f"{prefix}-federated-search",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/federated-search"),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "REGISTRY_TABLE": source_registry_table.table_name,
                "CATALOG_TABLE": catalog_table.table_name,
                "SNOWFLAKE_SECRET_ARN": snowflake_secret_arn,
                "REDSHIFT_SECRET_ARN": redshift_secret_arn,
            },
        )
        source_registry_table.grant_read_data(federated_search_fn)
        catalog_table.grant_read_data(federated_search_fn)
        federated_search_fn.add_to_role_policy(redshift_data_policy)
        if snowflake_secret_arn:
            federated_search_fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[snowflake_secret_arn],
                )
            )
        if redshift_secret_arn:
            federated_search_fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[redshift_secret_arn],
                )
            )

        # -----------------------------------------------------------------
        # AgentCore Gateway permission
        # Allow the AgentCore Gateway execution role to invoke tool Lambdas.
        # Set agentcore_gateway_role_arn context or add manually post-deploy.
        # -----------------------------------------------------------------
        gateway_role_arn = self.node.try_get_context("agentcore_gateway_role_arn")
        _new_tool_fns = [
            snowflake_browse_fn,
            snowflake_preview_fn,
            redshift_browse_fn,
            redshift_preview_fn,
            federated_search_fn,
        ]
        if gateway_role_arn:
            for fn in [search_fn, loader_fn, browse_fn, preview_fn, s3_load_fn] + _new_tool_fns:
                fn.add_permission(
                    "AgentCoreInvoke",
                    principal=iam.ArnPrincipal(gateway_role_arn),
                    action="lambda:InvokeFunction",
                )

        # -----------------------------------------------------------------
        # KMS grants — Lambda execution roles need decrypt + generate key
        # -----------------------------------------------------------------
        if enable_kms and cmk:
            _all_lambda_fns = [
                sync_fn, search_fn, loader_fn, browse_fn, preview_fn, s3_load_fn,
                claws_resolver_fn, quality_check_fn, register_source_fn,
                snowflake_browse_fn, snowflake_preview_fn,
                redshift_browse_fn, redshift_preview_fn,
                federated_search_fn,
            ]
            for fn in _all_lambda_fns:
                cmk.grant(fn.grant_principal, "kms:Decrypt", "kms:GenerateDataKey")

        # -----------------------------------------------------------------
        # Outputs — Lambda ARNs for AgentCore Gateway target registration
        # -----------------------------------------------------------------
        tool_arns = {
            "roda_search": search_fn.function_arn,
            "roda_load": loader_fn.function_arn,
            "s3_browse": browse_fn.function_arn,
            "s3_preview": preview_fn.function_arn,
            "s3_load": s3_load_fn.function_arn,
            "snowflake_browse": snowflake_browse_fn.function_arn,
            "snowflake_preview": snowflake_preview_fn.function_arn,
            "redshift_browse": redshift_browse_fn.function_arn,
            "redshift_preview": redshift_preview_fn.function_arn,
            "federated_search": federated_search_fn.function_arn,
        }

        for tool_name, arn_value in tool_arns.items():
            CfnOutput(
                self,
                f"{tool_name.title().replace('_', '')}Arn",
                value=arn_value,
                description=f"Register as AgentCore Gateway Lambda target: {tool_name}",
                export_name=f"qs-open-data-{tool_name.replace('_', '-')}-arn",
            )

        CfnOutput(
            self,
            "CatalogTableName",
            value=catalog_table.table_name,
        )

        CfnOutput(
            self,
            "ManifestBucketName",
            value=manifest_bucket.bucket_name,
        )

        # Convenience: all tool ARNs as a single JSON output
        CfnOutput(
            self,
            "ToolArns",
            value=json.dumps(tool_arns),
            description="All tool Lambda ARNs — register each as AgentCore Gateway Lambda target",
        )
