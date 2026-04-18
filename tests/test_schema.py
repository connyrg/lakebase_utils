"""Tests for SchemaManager (CRUD via psycopg2)."""

import pytest
import psycopg2.errors
from unittest.mock import MagicMock

from lakebase_utils.schema import SchemaManager
from lakebase_utils.models import SchemaInfo
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
    return SchemaManager(mock_client)


_UNSET = object()


def _setup_cursor(mock_client, fetchone=_UNSET, fetchall=_UNSET):
    conn = mock_client.pg_connection.return_value.__enter__.return_value
    cur = conn.cursor.return_value.__enter__.return_value
    if fetchone is not _UNSET:
        cur.fetchone.return_value = fetchone
    if fetchall is not _UNSET:
        cur.fetchall.return_value = fetchall
    return cur


class TestSchemaCreate:
    def test_create_returns_schema_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("raw", "admin", None))
        result = manager.create("raw", database="analytics")
        assert isinstance(result, SchemaInfo)
        assert result.name == "raw"
        assert result.database == "analytics"

    def test_create_exist_ok_returns_existing_on_duplicate(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=("raw", "admin", None))
        cur.execute.side_effect = [
            psycopg2.errors.DuplicateSchema("dup"),
            None,
        ]
        result = manager.create("raw", database="analytics", exist_ok=True)
        assert result.name == "raw"

    def test_create_raises_already_exists_without_exist_ok(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateSchema("dup")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.create("raw", database="analytics")

    def test_create_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("fail")
        with pytest.raises(LakebaseOperationError):
            manager.create("raw", database="analytics")


class TestSchemaGet:
    def test_get_returns_schema_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("raw", "admin", "landing zone"))
        result = manager.get("raw", database="analytics")
        assert result.name == "raw"
        assert result.database == "analytics"
        assert result.owner == "admin"
        assert result.comment == "landing zone"

    def test_get_raises_not_found_when_no_row(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=None)
        with pytest.raises(LakebaseNotFoundError):
            manager.get("missing", database="analytics")

    def test_get_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.get("raw", database="analytics")


class TestSchemaList:
    def test_list_returns_schemas(self, manager, mock_client):
        rows = [("bronze", "admin", None), ("raw", "admin", "landing")]
        _setup_cursor(mock_client, fetchall=rows)
        result = manager.list(database="analytics")
        assert len(result) == 2
        assert result[0].name == "bronze"
        assert result[0].database == "analytics"

    def test_list_returns_empty(self, manager, mock_client):
        _setup_cursor(mock_client, fetchall=[])
        assert manager.list(database="analytics") == []

    def test_list_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.list(database="analytics")


class TestSchemaRename:
    def test_rename_returns_schema_info(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=("bronze", "admin", None))
        result = manager.rename("raw", "bronze", database="analytics")
        assert result.name == "bronze"

    def test_rename_raises_not_found(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidSchemaName("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.rename("missing", "other", database="analytics")

    def test_rename_raises_already_exists(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateSchema("dup")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.rename("raw", "existing", database="analytics")


class TestSchemaDelete:
    def test_delete_succeeds(self, manager, mock_client):
        _setup_cursor(mock_client)
        manager.delete("raw", database="analytics")

    def test_delete_cascade(self, manager, mock_client):
        _setup_cursor(mock_client)
        manager.delete("raw", database="analytics", cascade=True)  # should not raise

    def test_delete_not_found_ok_silently_returns(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidSchemaName("not found")
        manager.delete("missing", database="analytics", not_found_ok=True)

    def test_delete_raises_not_found_without_flag(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.InvalidSchemaName("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.delete("missing", database="analytics")
