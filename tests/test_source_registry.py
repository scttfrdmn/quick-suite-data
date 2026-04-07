"""
Integration tests for register-source/handler.py.

Uses Substrate (real AWS emulator) for DynamoDB — never moto.
"""

import importlib
import importlib.util
import os
import sys

import boto3
import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
_TABLE_NAME = "qs-data-source-registry-test"


class TestRegisterSource:
    """Substrate-backed integration tests for the register-source Lambda."""

    def _create_table(self, substrate_url):
        ddb = boto3.client("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")
        ddb.create_table(
            TableName=_TABLE_NAME,
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.get_waiter("table_exists").wait(TableName=_TABLE_NAME)
        return boto3.resource("dynamodb", endpoint_url=substrate_url, region_name="us-east-1")

    def _reload(self, substrate_url, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", substrate_url)
        monkeypatch.setenv("SOURCE_REGISTRY_TABLE", _TABLE_NAME)
        path = os.path.join(REPO_ROOT, "lambdas", "register-source", "handler.py")
        alias = "_register_source_integ"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod

    @pytest.mark.integration
    def test_happy_path_valid_registration(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        result = mod.handler({
            "source_id": "s3-test-bucket",
            "type": "s3",
            "connection_config": {"bucket": "my-bucket", "prefix": "data/"},
            "display_name": "Test Bucket",
            "description": "A test S3 bucket",
            "tags": ["research", "test"],
            "data_classification": "internal",
        }, None)

        assert result["status"] == "registered"
        assert result["source_id"] == "s3-test-bucket"

        item = resource.Table(_TABLE_NAME).get_item(Key={"source_id": "s3-test-bucket"}).get("Item")
        assert item is not None
        assert item["type"] == "s3"
        assert item["display_name"] == "Test Bucket"
        assert "registered_at" in item

    @pytest.mark.integration
    def test_missing_required_field_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        result = mod.handler({
            "source_id": "partial-source",
            "type": "s3",
            # missing connection_config, display_name, description, data_classification
        }, None)

        assert "error" in result
        assert "Missing required fields" in result["error"]

    @pytest.mark.integration
    def test_invalid_type_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        result = mod.handler({
            "source_id": "bad-type-source",
            "type": "mysql",
            "connection_config": "some-arn",
            "display_name": "Bad Type",
            "description": "Invalid type",
            "data_classification": "public",
        }, None)

        assert "error" in result
        assert "Invalid type" in result["error"]

    @pytest.mark.integration
    def test_invalid_data_classification_returns_error(self, substrate_url, reset_substrate, monkeypatch):
        self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        result = mod.handler({
            "source_id": "bad-class-source",
            "type": "s3",
            "connection_config": "some-arn",
            "display_name": "Bad Class",
            "description": "Invalid classification",
            "data_classification": "confidential",
        }, None)

        assert "error" in result
        assert "Invalid data_classification" in result["error"]

    @pytest.mark.integration
    def test_reregistration_updates_item(self, substrate_url, reset_substrate, monkeypatch):
        resource = self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        mod.handler({
            "source_id": "reregister-source",
            "type": "s3",
            "connection_config": {"bucket": "my-bucket", "prefix": "data/"},
            "display_name": "Old Name",
            "description": "Original description",
            "data_classification": "public",
        }, None)

        result = mod.handler({
            "source_id": "reregister-source",
            "type": "snowflake",
            "connection_config": "arn:aws:secretsmanager:us-east-1:123456789012:secret:sf",
            "display_name": "New Name",
            "description": "Updated description",
            "data_classification": "restricted",
        }, None)

        assert result["status"] == "registered"
        item = resource.Table(_TABLE_NAME).get_item(Key={"source_id": "reregister-source"}).get("Item")
        assert item["display_name"] == "New Name"
        assert item["type"] == "snowflake"
        assert item["data_classification"] == "restricted"

    @pytest.mark.integration
    def test_all_valid_types_accepted(self, substrate_url, reset_substrate, monkeypatch):
        self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        # Use type-valid connection_config values (#55 — format validation)
        valid_configs = {
            "s3": {"bucket": "my-bucket"},
            "snowflake": "arn:aws:secretsmanager:us-east-1:123456789012:secret:sf",
            "redshift": "arn:aws:secretsmanager:us-east-1:123456789012:secret:rs",
            "roda": "{}",
        }
        for source_type in ["s3", "snowflake", "redshift", "roda"]:
            result = mod.handler({
                "source_id": f"test-{source_type}",
                "type": source_type,
                "connection_config": valid_configs[source_type],
                "display_name": f"Test {source_type}",
                "description": "Description",
                "data_classification": "public",
            }, None)
            assert result["status"] == "registered", f"Failed for type={source_type}: {result}"

    @pytest.mark.integration
    def test_all_valid_classifications_accepted(self, substrate_url, reset_substrate, monkeypatch):
        self._create_table(substrate_url)
        mod = self._reload(substrate_url, monkeypatch)

        for i, cls in enumerate(["public", "internal", "restricted", "phi"]):
            result = mod.handler({
                "source_id": f"test-cls-{i}",
                "type": "s3",
                "connection_config": {"bucket": "my-bucket"},  # valid s3 config (#55)
                "display_name": f"Test {cls}",
                "description": "Description",
                "data_classification": cls,
            }, None)
            assert result["status"] == "registered", f"Failed for classification={cls}: {result}"
