"""
Unit tests for register-source/handler.py.

Uses moto to mock DynamoDB.
"""

import importlib
import importlib.util
import os
import sys

import boto3
import pytest
from moto import mock_aws

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
_TABLE_NAME = "qs-data-source-registry-test"


def _load(table_name=_TABLE_NAME):
    """Load register-source/handler.py as a unique module."""
    env_patch = {"SOURCE_REGISTRY_TABLE": table_name}
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(os.environ, env_patch):
        path = os.path.join(REPO_ROOT, "lambdas", "register-source", "handler.py")
        alias = f"_register_source_{table_name}"
        spec = importlib.util.spec_from_file_location(alias, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod


def _create_table(ddb_client):
    ddb_client.create_table(
        TableName=_TABLE_NAME,
        KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=_TABLE_NAME)


class TestRegisterSource:
    @pytest.fixture(autouse=True)
    def setup_moto(self):
        with mock_aws():
            self.ddb = boto3.client("dynamodb", region_name="us-east-1")
            _create_table(self.ddb)
            self.resource = boto3.resource("dynamodb", region_name="us-east-1")
            self.mod = _load(_TABLE_NAME)
            self.mod.dynamodb = self.resource
            yield

    def _call(self, event):
        return self.mod.handler(event, None)

    def test_happy_path_valid_registration(self):
        result = self._call({
            "source_id": "s3-test-bucket",
            "type": "s3",
            "connection_config": {"bucket": "my-bucket", "prefix": "data/"},
            "display_name": "Test Bucket",
            "description": "A test S3 bucket",
            "tags": ["research", "test"],
            "data_classification": "internal",
        })
        assert result["status"] == "registered"
        assert result["source_id"] == "s3-test-bucket"

        # Verify item written to DDB
        table = self.resource.Table(_TABLE_NAME)
        item = table.get_item(Key={"source_id": "s3-test-bucket"}).get("Item")
        assert item is not None
        assert item["type"] == "s3"
        assert item["display_name"] == "Test Bucket"
        assert "registered_at" in item

    def test_missing_required_field_returns_error(self):
        result = self._call({
            "source_id": "partial-source",
            "type": "s3",
            # missing connection_config, display_name, description, data_classification
        })
        assert "error" in result
        assert "Missing required fields" in result["error"]

    def test_invalid_type_returns_error(self):
        result = self._call({
            "source_id": "bad-type-source",
            "type": "mysql",
            "connection_config": "some-arn",
            "display_name": "Bad Type",
            "description": "Invalid type",
            "data_classification": "public",
        })
        assert "error" in result
        assert "Invalid type" in result["error"]

    def test_invalid_data_classification_returns_error(self):
        result = self._call({
            "source_id": "bad-class-source",
            "type": "s3",
            "connection_config": "some-arn",
            "display_name": "Bad Class",
            "description": "Invalid classification",
            "data_classification": "confidential",
        })
        assert "error" in result
        assert "Invalid data_classification" in result["error"]

    def test_reregistration_updates_item(self):
        # Register once
        self._call({
            "source_id": "reregister-source",
            "type": "s3",
            "connection_config": "config-v1",
            "display_name": "Old Name",
            "description": "Original description",
            "data_classification": "public",
        })
        # Register again with updated name
        result = self._call({
            "source_id": "reregister-source",
            "type": "snowflake",
            "connection_config": "arn:aws:secretsmanager:us-east-1:123:secret:sf",
            "display_name": "New Name",
            "description": "Updated description",
            "data_classification": "restricted",
        })
        assert result["status"] == "registered"

        table = self.resource.Table(_TABLE_NAME)
        item = table.get_item(Key={"source_id": "reregister-source"}).get("Item")
        assert item["display_name"] == "New Name"
        assert item["type"] == "snowflake"
        assert item["data_classification"] == "restricted"

    def test_all_valid_types_accepted(self):
        for source_type in ["s3", "snowflake", "redshift", "roda"]:
            result = self._call({
                "source_id": f"test-{source_type}",
                "type": source_type,
                "connection_config": "config",
                "display_name": f"Test {source_type}",
                "description": "Description",
                "data_classification": "public",
            })
            assert result["status"] == "registered", f"Failed for type={source_type}: {result}"

    def test_all_valid_classifications_accepted(self):
        for i, cls in enumerate(["public", "internal", "restricted", "phi"]):
            result = self._call({
                "source_id": f"test-cls-{i}",
                "type": "s3",
                "connection_config": "config",
                "display_name": f"Test {cls}",
                "description": "Description",
                "data_classification": cls,
            })
            assert result["status"] == "registered", f"Failed for classification={cls}: {result}"
