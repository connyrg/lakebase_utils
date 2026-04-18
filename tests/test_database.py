"""Tests for DatabaseManager (CRUD via psycopg2)."""

import pytest
from unittest.mock import MagicMock

from lakebase_utils.database import DatabaseManager


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def manager(mock_client):
    return DatabaseManager(mock_client)


class TestDatabaseCreate:
    def test_create_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.create("analytics")


class TestDatabaseGet:
    def test_get_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.get("analytics")


class TestDatabaseList:
    def test_list_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.list()


class TestDatabaseRename:
    def test_rename_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.rename("analytics", "analytics_v2")


class TestDatabaseDelete:
    def test_delete_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.delete("analytics")
