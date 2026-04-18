"""Tests for DatabaseManager (CRUD via psycopg2)."""

import pytest
import psycopg2.errors
from unittest.mock import MagicMock, patch

from lakebase_utils.database import DatabaseManager
from lakebase_utils.models import DatabaseInfo
from lakebase_utils.exceptions import (
    LakebaseAlreadyExistsError,
    LakebaseNotFoundError,
    LakebaseOperationError,
)


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def manager(mock_client):
    return DatabaseManager(mock_client)


_UNSET = object()


def _setup_cursor(mock_client, fetchone=_UNSET, fetchall=_UNSET):
    """Wire up mock_client.pg_connection so cursor returns given data."""
    conn = mock_client.pg_connection.return_value.__enter__.return_value
    cur = conn.cursor.return_value.__enter__.return_value
    if fetchone is not _UNSET:
        cur.fetchone.return_value = fetchone
    if fetchall is not _UNSET:
        cur.fetchall.return_value = fetchall
    return cur


class TestDatabaseCreate:
    def test_create_returns_database_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("analytics", "admin", "my db"))
        result = manager.create("analytics")
        assert isinstance(result, DatabaseInfo)
        assert result.name == "analytics"

    def test_create_exist_ok_returns_existing_on_duplicate(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=("analytics", "admin", None))
        cur.execute.side_effect = [
            psycopg2.errors.DuplicateDatabase("already exists"),
            None,  # for get() SELECT
        ]
        result = manager.create("analytics", exist_ok=True)
        assert result.name == "analytics"

    def test_create_raises_already_exists_without_exist_ok(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateDatabase("already exists")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.create("analytics")

    def test_create_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("connection refused")
        with pytest.raises(LakebaseOperationError):
            manager.create("analytics")


class TestDatabaseGet:
    def test_get_returns_database_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("analytics", "admin", "desc"))
        result = manager.get("analytics")
        assert result.name == "analytics"
        assert result.owner == "admin"
        assert result.comment == "desc"

    def test_get_raises_not_found_when_no_row(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=None)
        with pytest.raises(LakebaseNotFoundError):
            manager.get("nonexistent")

    def test_get_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.get("analytics")


class TestDatabaseList:
    def test_list_returns_databases(self, manager, mock_client):
        rows = [("analytics", "admin", None), ("raw", "admin", "raw db")]
        _setup_cursor(mock_client, fetchall=rows)
        result = manager.list()
        assert len(result) == 2
        assert result[0].name == "analytics"
        assert result[1].name == "raw"

    def test_list_returns_empty(self, manager, mock_client):
        _setup_cursor(mock_client, fetchall=[])
        assert manager.list() == []

    def test_list_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.list()


class TestDatabaseRename:
    def test_rename_returns_database_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("analytics_v2", "admin", None))
        result = manager.rename("analytics", "analytics_v2")
        assert result.name == "analytics_v2"

    def test_rename_raises_not_found(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidCatalogName("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.rename("missing", "other")

    def test_rename_raises_already_exists(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateDatabase("dup")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.rename("analytics", "existing")


class TestDatabaseDelete:
    def test_delete_succeeds(self, manager, mock_client):
        _setup_cursor(mock_client)
        manager.delete("analytics")  # should not raise

    def test_delete_not_found_ok_silently_returns(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidCatalogName("not found")
        manager.delete("missing", not_found_ok=True)  # should not raise

    def test_delete_raises_not_found_without_flag(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidCatalogName("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.delete("missing")

    def test_delete_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("active connections")
        with pytest.raises(LakebaseOperationError):
            manager.delete("analytics")
