"""Tests for SchemaManager (CRUD via psycopg2)."""

import pytest
from unittest.mock import MagicMock

from lakebase_utils.schema import SchemaManager


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def manager(mock_client):
    return SchemaManager(mock_client)


class TestSchemaCreate:
    def test_create_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.create("raw", database="analytics")


class TestSchemaGet:
    def test_get_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.get("raw", database="analytics")


class TestSchemaList:
    def test_list_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.list(database="analytics")


class TestSchemaRename:
    def test_rename_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.rename("raw", "bronze", database="analytics")


class TestSchemaDelete:
    def test_delete_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.delete("raw", database="analytics")
