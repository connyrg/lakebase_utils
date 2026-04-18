"""Tests for TableManager (CRUD via psycopg2)."""

import pytest
from unittest.mock import MagicMock

from lakebase_utils.models import ColumnInfo
from lakebase_utils.table import TableManager


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def manager(mock_client):
    return TableManager(mock_client)


@pytest.fixture
def sample_columns():
    return [
        ColumnInfo("id", "bigint", nullable=False),
        ColumnInfo("name", "text"),
        ColumnInfo("created_at", "timestamptz"),
    ]


class TestTableCreate:
    def test_create_raises_not_implemented(self, manager, sample_columns):
        with pytest.raises(NotImplementedError):
            manager.create("events", schema="raw", database="analytics", columns=sample_columns)


class TestTableGet:
    def test_get_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.get("events", schema="raw", database="analytics")


class TestTableList:
    def test_list_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.list(schema="raw", database="analytics")


class TestTableRename:
    def test_rename_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.rename("events", "events_v2", schema="raw", database="analytics")


class TestTableAlterColumn:
    def test_add_column_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.add_column(
                "events", "raw", "analytics",
                ColumnInfo("payload", "jsonb"),
            )

    def test_drop_column_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.drop_column("events", "raw", "analytics", "payload")

    def test_alter_column_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.alter_column("events", "raw", "analytics", "name", new_type="varchar(255)")


class TestTableDelete:
    def test_delete_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.delete("events", schema="raw", database="analytics")
