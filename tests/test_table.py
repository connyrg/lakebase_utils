"""Tests for TableManager (CRUD via psycopg2)."""

import pytest
import psycopg2.errors
from unittest.mock import MagicMock

from lakebase_utils.models import ColumnInfo, TableInfo
from lakebase_utils.table import TableManager
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
    return TableManager(mock_client)


@pytest.fixture
def sample_columns():
    return [
        ColumnInfo("id", "bigint", nullable=False),
        ColumnInfo("name", "text"),
        ColumnInfo("created_at", "timestamptz"),
    ]


_UNSET = object()


def _setup_cursor(mock_client, fetchone=_UNSET, fetchall=_UNSET):
    conn = mock_client.pg_connection.return_value.__enter__.return_value
    cur = conn.cursor.return_value.__enter__.return_value
    if fetchone is not _UNSET:
        cur.fetchone.return_value = fetchone
    if fetchall is not _UNSET:
        cur.fetchall.return_value = fetchall
    return cur


_TABLE_ROW = ("events", "admin", "event log")
_COL_ROWS = [
    ("events", "id", "bigint", False, None, None),
    ("events", "name", "text", True, None, None),
]


class TestTableCreate:
    def test_create_returns_table_info(self, manager, mock_client, sample_columns):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        result = manager.create("events", schema="raw", database="analytics", columns=sample_columns)
        assert isinstance(result, TableInfo)
        assert result.name == "events"
        assert result.schema == "raw"
        assert result.database == "analytics"

    def test_create_exist_ok_returns_existing_on_duplicate(self, manager, mock_client, sample_columns):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        cur.execute.side_effect = [
            psycopg2.errors.DuplicateTable("dup"),
            None,   # get() table SELECT
            None,   # get() column SELECT
        ]
        result = manager.create("events", schema="raw", database="analytics", columns=sample_columns, exist_ok=True)
        assert result.name == "events"

    def test_create_raises_already_exists_without_exist_ok(self, manager, mock_client, sample_columns):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateTable("dup")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.create("events", schema="raw", database="analytics", columns=sample_columns)

    def test_create_raises_operation_error_on_pg_failure(self, manager, mock_client, sample_columns):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("fail")
        with pytest.raises(LakebaseOperationError):
            manager.create("events", schema="raw", database="analytics", columns=sample_columns)


class TestTableGet:
    def test_get_returns_table_info(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        result = manager.get("events", schema="raw", database="analytics")
        assert result.name == "events"
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"

    def test_get_raises_not_found_when_no_row(self, manager, mock_client):
        _setup_cursor(mock_client, fetchone=None)
        with pytest.raises(LakebaseNotFoundError):
            manager.get("missing", schema="raw", database="analytics")

    def test_get_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.get("events", schema="raw", database="analytics")


class TestTableList:
    def test_list_returns_tables(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.fetchall.side_effect = [
            [_TABLE_ROW],
            _COL_ROWS,
        ]
        result = manager.list(schema="raw", database="analytics")
        assert len(result) == 1
        assert result[0].name == "events"

    def test_list_returns_empty(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.fetchall.side_effect = [[], []]
        assert manager.list(schema="raw", database="analytics") == []

    def test_list_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("oops")
        with pytest.raises(LakebaseOperationError):
            manager.list(schema="raw", database="analytics")


class TestTableRename:
    def test_rename_returns_table_info(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=("events_v2", "admin", None))
        cur.fetchall.return_value = _COL_ROWS
        result = manager.rename("events", "events_v2", schema="raw", database="analytics")
        assert result.name == "events_v2"

    def test_rename_raises_not_found(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedTable("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.rename("missing", "other", schema="raw", database="analytics")

    def test_rename_raises_already_exists(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateTable("dup")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.rename("events", "existing", schema="raw", database="analytics")


class TestTableAlterColumn:
    def test_add_column_returns_updated_table(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        result = manager.add_column(
            "events", "raw", "analytics", ColumnInfo("payload", "jsonb")
        )
        assert result.name == "events"

    def test_add_column_raises_not_found(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedTable("no table")
        with pytest.raises(LakebaseNotFoundError):
            manager.add_column("missing", "raw", "analytics", ColumnInfo("col", "text"))

    def test_add_column_raises_already_exists(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.DuplicateColumn("dup col")
        with pytest.raises(LakebaseAlreadyExistsError):
            manager.add_column("events", "raw", "analytics", ColumnInfo("id", "bigint"))

    def test_drop_column_returns_updated_table(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        result = manager.drop_column("events", "raw", "analytics", "name")
        assert result.name == "events"

    def test_drop_column_raises_not_found_for_missing_column(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedColumn("no col")
        with pytest.raises(LakebaseNotFoundError):
            manager.drop_column("events", "raw", "analytics", "missing_col")

    def test_alter_column_type_returns_updated_table(self, manager, mock_client):
        cur = _setup_cursor(mock_client, fetchone=_TABLE_ROW)
        cur.fetchall.return_value = _COL_ROWS
        result = manager.alter_column(
            "events", "raw", "analytics", "name", new_type="varchar(255)"
        )
        assert result.name == "events"

    def test_alter_column_raises_not_found_for_missing_table(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedTable("no table")
        with pytest.raises(LakebaseNotFoundError):
            manager.alter_column("missing", "raw", "analytics", "col", new_type="text")


class TestTableDelete:
    def test_delete_succeeds(self, manager, mock_client):
        _setup_cursor(mock_client)
        manager.delete("events", schema="raw", database="analytics")

    def test_delete_not_found_ok_silently_returns(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedTable("not found")
        manager.delete("missing", schema="raw", database="analytics", not_found_ok=True)

    def test_delete_raises_not_found_without_flag(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.errors.UndefinedTable("not found")
        with pytest.raises(LakebaseNotFoundError):
            manager.delete("missing", schema="raw", database="analytics")

    def test_delete_raises_operation_error_on_pg_failure(self, manager, mock_client):
        cur = _setup_cursor(mock_client)
        cur.execute.side_effect = psycopg2.OperationalError("active connections")
        with pytest.raises(LakebaseOperationError):
            manager.delete("events", schema="raw", database="analytics")
