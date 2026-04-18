"""CRUD operations for tables inside a Lakebase database and schema.

All methods connect to the target database via
:meth:`LakebaseClient.pg_connection`.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Optional

import psycopg2
import psycopg2.errors
from psycopg2 import sql as pgsql

from .exceptions import LakebaseAlreadyExistsError, LakebaseNotFoundError, LakebaseOperationError
from .models import ColumnInfo, TableInfo

if TYPE_CHECKING:
    from .client import LakebaseClient

_TABLE_SELECT = """
    SELECT t.table_name,
           pg_get_userbyid(c.relowner) AS owner,
           obj_description(c.oid, 'pg_class') AS comment
    FROM information_schema.tables t
    JOIN pg_class c ON c.relname = t.table_name
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
    WHERE t.table_type = 'BASE TABLE'
"""

_COL_SELECT = """
    SELECT c.table_name,
           c.column_name,
           c.data_type,
           CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END AS nullable,
           c.column_default,
           pgd.description AS comment
    FROM information_schema.columns c
    LEFT JOIN pg_class pc ON pc.relname = c.table_name
    LEFT JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = c.table_schema
    LEFT JOIN pg_attribute pa
           ON pa.attrelid = pc.oid AND pa.attname = c.column_name AND pa.attnum > 0
    LEFT JOIN pg_description pgd
           ON pgd.objoid = pc.oid AND pgd.objsubid = pa.attnum
    WHERE c.table_schema = %s
"""


class TableManager:
    """Create, read, update, and delete tables within a Lakebase schema.

    Obtained via ``client.tables``.

    Example::

        from lakebase_utils.models import ColumnInfo

        client = LakebaseClient(host="...", token="...", pg_host="...")

        client.tables.create(
            name="events",
            schema="raw",
            database="analytics",
            columns=[
                ColumnInfo("id", "bigint", nullable=False),
                ColumnInfo("event_type", "text"),
                ColumnInfo("created_at", "timestamptz"),
            ],
        )

        tables = client.tables.list(schema="raw", database="analytics")
        client.tables.add_column("events", "raw", "analytics", ColumnInfo("payload", "jsonb"))
        client.tables.drop_column("events", "raw", "analytics", "payload")
        client.tables.rename("events", "events_v2", schema="raw", database="analytics")
        client.tables.delete("events_v2", schema="raw", database="analytics")
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create(
        self,
        name: str,
        schema: str,
        database: str,
        columns: list[ColumnInfo],
        owner: Optional[str] = None,
        comment: Optional[str] = None,
        exist_ok: bool = False,
    ) -> TableInfo:
        """Create a table in *schema*.*database*."""
        col_defs = []
        for col in columns:
            parts: list = [pgsql.Identifier(col.name), pgsql.SQL(col.data_type)]
            if not col.nullable:
                parts.append(pgsql.SQL("NOT NULL"))
            if col.default is not None:
                parts.append(pgsql.SQL(f"DEFAULT {col.default}"))
            col_defs.append(pgsql.SQL(" ").join(parts))

        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("CREATE TABLE {}.{} ({})").format(
                            pgsql.Identifier(schema),
                            pgsql.Identifier(name),
                            pgsql.SQL(", ").join(col_defs),
                        )
                    )
                    if owner:
                        cur.execute(
                            pgsql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(owner),
                            )
                        )
        except psycopg2.errors.DuplicateTable:
            if exist_ok:
                return self.get(name, schema, database)
            raise LakebaseAlreadyExistsError(
                f"Table {schema!r}.{name!r} already exists in database {database!r}."
            )
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"CREATE TABLE {schema}.{name} failed: {exc}"
            ) from exc

        if comment:
            self.update_comment(name, schema, database, comment)
        return self.get(name, schema, database)

    def get(self, name: str, schema: str, database: str) -> TableInfo:
        """Return metadata (including column definitions) for a single table."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        _TABLE_SELECT + " AND t.table_schema = %s AND t.table_name = %s",
                        (schema, name),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise LakebaseNotFoundError(
                            f"Table {schema!r}.{name!r} not found in database {database!r}."
                        )
                    cur.execute(_COL_SELECT + " AND c.table_name = %s ORDER BY c.ordinal_position", (schema, name))
                    col_rows = cur.fetchall()
        except LakebaseNotFoundError:
            raise
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to fetch table {schema}.{name}: {exc}"
            ) from exc
        columns = [self._col_row_to_info(r[1:]) for r in col_rows]
        return self._row_to_table_info(row, schema, database, columns)

    def list(self, schema: str, database: str) -> list[TableInfo]:
        """Return metadata for all tables in *schema*.*database*.

        Column information is included for each table.
        """
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        _TABLE_SELECT + " AND t.table_schema = %s ORDER BY t.table_name",
                        (schema,),
                    )
                    rows = cur.fetchall()
                    cur.execute(
                        _COL_SELECT + " ORDER BY c.table_name, c.ordinal_position",
                        (schema,),
                    )
                    col_rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to list tables in {schema!r}: {exc}"
            ) from exc

        cols_by_table: dict[str, list[ColumnInfo]] = defaultdict(list)
        for r in col_rows:
            cols_by_table[r[0]].append(self._col_row_to_info(r[1:]))

        return [
            self._row_to_table_info(row, schema, database, cols_by_table[row[0]])
            for row in rows
        ]

    def rename(self, name: str, new_name: str, schema: str, database: str) -> TableInfo:
        """Rename a table."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            pgsql.Identifier(schema),
                            pgsql.Identifier(name),
                            pgsql.Identifier(new_name),
                        )
                    )
        except psycopg2.errors.UndefinedTable as exc:
            raise LakebaseNotFoundError(f"Table {schema!r}.{name!r} not found.") from exc
        except psycopg2.errors.DuplicateTable as exc:
            raise LakebaseAlreadyExistsError(
                f"Table {schema!r}.{new_name!r} already exists."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"RENAME TABLE {schema}.{name} failed: {exc}"
            ) from exc
        return self.get(new_name, schema, database)

    def update_comment(
        self, name: str, schema: str, database: str, comment: Optional[str]
    ) -> TableInfo:
        """Set or clear the comment on a table."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    if comment is None:
                        cur.execute(
                            pgsql.SQL("COMMENT ON TABLE {}.{} IS NULL").format(
                                pgsql.Identifier(schema), pgsql.Identifier(name)
                            )
                        )
                    else:
                        cur.execute(
                            pgsql.SQL("COMMENT ON TABLE {}.{} IS %s").format(
                                pgsql.Identifier(schema), pgsql.Identifier(name)
                            ),
                            (comment,),
                        )
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to update comment on table {schema}.{name}: {exc}"
            ) from exc
        return self.get(name, schema, database)

    def add_column(
        self, name: str, schema: str, database: str, column: ColumnInfo
    ) -> TableInfo:
        """Add a column to an existing table (``ALTER TABLE … ADD COLUMN``)."""
        parts: list = [pgsql.Identifier(column.name), pgsql.SQL(column.data_type)]
        if not column.nullable:
            parts.append(pgsql.SQL("NOT NULL"))
        col_def = pgsql.SQL(" ").join(parts)

        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("ALTER TABLE {}.{} ADD COLUMN {}").format(
                            pgsql.Identifier(schema),
                            pgsql.Identifier(name),
                            col_def,
                        )
                    )
        except psycopg2.errors.UndefinedTable as exc:
            raise LakebaseNotFoundError(f"Table {schema!r}.{name!r} not found.") from exc
        except psycopg2.errors.DuplicateColumn as exc:
            raise LakebaseAlreadyExistsError(
                f"Column {column.name!r} already exists in {schema}.{name}."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"ADD COLUMN {column.name!r} on {schema}.{name} failed: {exc}"
            ) from exc
        return self.get(name, schema, database)

    def drop_column(
        self, name: str, schema: str, database: str, column_name: str
    ) -> TableInfo:
        """Drop a column from a table (``ALTER TABLE … DROP COLUMN``)."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                            pgsql.Identifier(schema),
                            pgsql.Identifier(name),
                            pgsql.Identifier(column_name),
                        )
                    )
        except psycopg2.errors.UndefinedTable as exc:
            raise LakebaseNotFoundError(f"Table {schema!r}.{name!r} not found.") from exc
        except psycopg2.errors.UndefinedColumn as exc:
            raise LakebaseNotFoundError(
                f"Column {column_name!r} not found in {schema}.{name}."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"DROP COLUMN {column_name!r} on {schema}.{name} failed: {exc}"
            ) from exc
        return self.get(name, schema, database)

    def alter_column(
        self,
        name: str,
        schema: str,
        database: str,
        column_name: str,
        new_type: Optional[str] = None,
        new_nullable: Optional[bool] = None,
        new_default: Optional[str] = None,
        new_comment: Optional[str] = None,
    ) -> TableInfo:
        """Alter an existing column's type, nullability, default, or comment."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    if new_type is not None:
                        cur.execute(
                            pgsql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}").format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                                pgsql.SQL(new_type),
                            )
                        )
                    if new_nullable is True:
                        cur.execute(
                            pgsql.SQL(
                                "ALTER TABLE {}.{} ALTER COLUMN {} DROP NOT NULL"
                            ).format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                            )
                        )
                    elif new_nullable is False:
                        cur.execute(
                            pgsql.SQL(
                                "ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL"
                            ).format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                            )
                        )
                    if new_default == "":
                        cur.execute(
                            pgsql.SQL(
                                "ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT"
                            ).format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                            )
                        )
                    elif new_default is not None:
                        cur.execute(
                            pgsql.SQL(
                                "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {}"
                            ).format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                                pgsql.SQL(new_default),
                            )
                        )
                    if new_comment == "":
                        cur.execute(
                            pgsql.SQL("COMMENT ON COLUMN {}.{}.{} IS NULL").format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                            )
                        )
                    elif new_comment is not None:
                        cur.execute(
                            pgsql.SQL("COMMENT ON COLUMN {}.{}.{} IS %s").format(
                                pgsql.Identifier(schema),
                                pgsql.Identifier(name),
                                pgsql.Identifier(column_name),
                            ),
                            (new_comment,),
                        )
        except psycopg2.errors.UndefinedTable as exc:
            raise LakebaseNotFoundError(f"Table {schema!r}.{name!r} not found.") from exc
        except psycopg2.errors.UndefinedColumn as exc:
            raise LakebaseNotFoundError(
                f"Column {column_name!r} not found in {schema}.{name}."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"ALTER COLUMN {column_name!r} on {schema}.{name} failed: {exc}"
            ) from exc
        return self.get(name, schema, database)

    def delete(
        self, name: str, schema: str, database: str, not_found_ok: bool = False
    ) -> None:
        """Drop a table."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("DROP TABLE {}.{}").format(
                            pgsql.Identifier(schema), pgsql.Identifier(name)
                        )
                    )
        except psycopg2.errors.UndefinedTable as exc:
            if not_found_ok:
                return
            raise LakebaseNotFoundError(
                f"Table {schema!r}.{name!r} not found in database {database!r}."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"DROP TABLE {schema}.{name} failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_columns(
        self, name: str, schema: str, database: str
    ) -> list[ColumnInfo]:
        """Query ``information_schema.columns`` and return column definitions."""
        with self._client.pg_connection(database=database) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _COL_SELECT + " AND c.table_name = %s ORDER BY c.ordinal_position",
                    (schema, name),
                )
                rows = cur.fetchall()
        return [self._col_row_to_info(r[1:]) for r in rows]

    def _col_row_to_info(self, row: tuple) -> ColumnInfo:
        col_name, data_type, nullable, default, comment = row
        return ColumnInfo(
            name=col_name,
            data_type=data_type,
            nullable=bool(nullable),
            default=default,
            comment=comment,
        )

    def _row_to_table_info(
        self, row: tuple, schema: str, database: str, columns: list[ColumnInfo]
    ) -> TableInfo:
        """Convert a raw ``information_schema.tables`` row to :class:`TableInfo`."""
        name, owner, comment = row
        return TableInfo(
            name=name,
            schema=schema,
            database=database,
            columns=columns,
            owner=owner,
            comment=comment,
        )
