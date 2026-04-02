"""
Unit tests for catalog-quality-check/handler.py.

Tests stale detection logic using Substrate stubs — no real AWS calls.
"""

import importlib.util
import json
import os
import sys
import time

import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

TWO_YEARS_SECONDS = 2 * 365 * 24 * 3600


def _load():
    """Load catalog-quality-check/handler.py as a unique module."""
    path = os.path.join(REPO_ROOT, "lambdas", "catalog-quality-check", "handler.py")
    spec = importlib.util.spec_from_file_location("_catalog_quality_check_handler", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_catalog_quality_check_handler"] = mod
    return spec, mod


# ---------------------------------------------------------------------------
# Substrate stubs
# ---------------------------------------------------------------------------

class _FakeTable:
    """In-memory DynamoDB table stub."""

    def __init__(self, items):
        self._items = list(items)
        self.updated = {}  # slug → new attribute values

    def scan(self, **kwargs):
        projection = kwargs.get("ProjectionExpression", "")
        fields = [f.strip() for f in projection.split(",") if f.strip()]
        projected = []
        for item in self._items:
            if fields:
                projected.append({k: v for k, v in item.items() if k in fields})
            else:
                projected.append(dict(item))
        return {"Items": projected}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues):
        slug = Key["slug"]
        # Parse simple SET expression
        for k, v in ExpressionAttributeValues.items():
            attr = UpdateExpression.split("SET", 1)[1].split("=")[0].strip()
            self.updated[slug] = {attr: v}


class _FakeCW:
    """CloudWatch stub that records put_metric_data calls."""

    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kwargs):
        self.calls.append(kwargs)


def _make_handler(items):
    """
    Load the quality-check handler with patched boto3 resources pointing
    at in-memory stubs. Returns (module, fake_table, fake_cw).
    """
    fake_table = _FakeTable(items)
    fake_cw = _FakeCW()

    import unittest.mock as mock

    fake_dynamodb = mock.MagicMock()
    fake_dynamodb.Table.return_value = fake_table
    fake_boto3 = mock.MagicMock()
    fake_boto3.resource.return_value = fake_dynamodb
    fake_boto3.client.return_value = fake_cw

    # Reload the module with patched boto3
    spec, mod = _load()
    mod.__dict__["boto3"] = fake_boto3
    mod.__dict__["TABLE_NAME"] = "test-catalog"
    mod.__dict__["dynamodb"] = fake_dynamodb
    mod.__dict__["cw"] = fake_cw
    spec.loader.exec_module(mod)

    # After exec_module, the module-level TABLE_NAME / dynamodb / cw are reset
    # by the module code. Override them directly on the module object.
    mod.dynamodb = fake_dynamodb
    mod.cw = fake_cw
    mod.TABLE_NAME = "test-catalog"

    return mod, fake_table, fake_cw


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCatalogQualityCheck:
    def test_missing_last_updated_marked_stale(self):
        """Item with no last_updated field → marked stale, count = 1."""
        items = [{"slug": "no-timestamp"}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        assert result["scanned"] == 1
        assert "no-timestamp" in table.updated

    def test_old_last_updated_marked_stale(self):
        """Item with last_updated > 2 years ago → marked stale."""
        old_ts = int(time.time()) - TWO_YEARS_SECONDS - 86400  # 1 day past threshold
        items = [{"slug": "old-dataset", "last_updated": old_ts}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        assert "old-dataset" in table.updated

    def test_recent_last_updated_not_stale(self):
        """Item with recent last_updated → NOT marked stale."""
        recent_ts = int(time.time()) - 86400  # 1 day ago
        items = [{"slug": "fresh-dataset", "last_updated": recent_ts}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 0
        assert result["scanned"] == 1
        assert "fresh-dataset" not in table.updated

    def test_boundary_exactly_two_years_is_stale(self):
        """Item with last_updated exactly at 2-year mark → stale (cutoff = now - 2yr, item < cutoff)."""
        # cutoff = now - TWO_YEARS_SECONDS; item exactly at cutoff is NOT less than cutoff
        # so it should NOT be stale. Test the just-past-boundary case.
        now = int(time.time())
        cutoff = now - TWO_YEARS_SECONDS
        # One second past the cutoff (older than 2 years)
        boundary_ts = cutoff - 1
        items = [{"slug": "boundary-dataset", "last_updated": boundary_ts}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 1
        assert "boundary-dataset" in table.updated

    def test_boundary_just_under_two_years_not_stale(self):
        """Item with last_updated just under 2 years ago → NOT stale."""
        now = int(time.time())
        cutoff = now - TWO_YEARS_SECONDS
        # One second before the cutoff (newer than 2 years)
        boundary_ts = cutoff + 1
        items = [{"slug": "under-boundary", "last_updated": boundary_ts}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["stale_count"] == 0
        assert "under-boundary" not in table.updated

    def test_cloudwatch_metric_emitted(self):
        """StaleDatasets metric is published to CloudWatch with the stale count."""
        items = [{"slug": "stale-one"}, {"slug": "stale-two"}]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert len(cw.calls) == 1
        call = cw.calls[0]
        assert call["Namespace"] == "QuickSuiteOpenData"
        metric = call["MetricData"][0]
        assert metric["MetricName"] == "StaleDatasets"
        assert metric["Value"] == 2

    def test_mixed_items_correct_counts(self):
        """Mix of stale and fresh items — counts match."""
        now = int(time.time())
        items = [
            {"slug": "fresh-a", "last_updated": now - 86400},
            {"slug": "stale-b"},  # missing
            {"slug": "stale-c", "last_updated": now - TWO_YEARS_SECONDS - 86400},
            {"slug": "fresh-d", "last_updated": now - 30 * 86400},
        ]
        mod, table, cw = _make_handler(items)

        result = mod.handler({}, None)

        assert result["scanned"] == 4
        assert result["stale_count"] == 2
        assert "stale-b" in table.updated
        assert "stale-c" in table.updated
        assert "fresh-a" not in table.updated
        assert "fresh-d" not in table.updated
