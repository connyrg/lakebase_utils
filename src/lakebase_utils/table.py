"""CRUD operations for tables inside a Lakebase database and schema.

All methods connect to the target database via
:meth:`LakebaseClient.pg_connection`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .exceptions import LakebaseAlreadyExistsError, LakebaseNotFoundError, LakebaseOperationError
from .models import ColumnInfo, TableInfo

if TYPE_CHECKING:
    from .client import LakebaseClient


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
        """Create a table in *schema*.*database*.

        Parameters
        ----------
        name:
            Table name.
        schema:
            Schema that will contain the table.
        database:
            Database that contains the schema.
        columns:
            Ordered list of column definitions.  At least one column is required.
        owner:
            Role to assign as owner. Defaults to the connecting user.
        comment:
            Optional description.
        exist_ok:
            If ``True``, silently return existing table info instead of raising
            :class:`LakebaseAlreadyExistsError`.

        Returns
        -------
        TableInfo
            Metadata of the newly created (or pre-existing) table.

        Raises
        ------
        LakebaseAlreadyExistsError
            If the table already exists and ``exist_ok=False``.
        LakebaseNotFoundError
            If *schema* or *database* does not exist.
        LakebaseOperationError
            If the ``CREATE TABLE`` statement fails for any other reason.
        """
        raise NotImplementedError

    def get(self, name: str, schema: str, database: str) -> TableInfo:
        """Return metadata (including column definitions) for a single table.

        Raises
        ------
        LakebaseNotFoundError
            If the table does not exist.
        """
        raise NotImplementedError

    def list(self, schema: str, database: str) -> list[TableInfo]:
        """Return metadata for all tables in *schema*.*database*.

        Column information is included for each table.
        """
        raise NotImplementedError

    def rename(self, name: str, new_name: str, schema: str, database: str) -> TableInfo:
        """Rename a table.

        Parameters
        ----------
        name:
            Current table name.
        new_name:
            New table name.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.

        Raises
        ------
        LakebaseNotFoundError
            If *name* does not exist.
        LakebaseAlreadyExistsError
            If *new_name* is already taken in the same schema.
        """
        raise NotImplementedError

    def update_comment(
        self, name: str, schema: str, database: str, comment: Optional[str]
    ) -> TableInfo:
        """Set or clear the comment on a table.

        Parameters
        ----------
        name:
            Table to update.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.
        comment:
            New comment text, or ``None`` to remove the existing comment.
        """
        raise NotImplementedError

    def add_column(
        self, name: str, schema: str, database: str, column: ColumnInfo
    ) -> TableInfo:
        """Add a column to an existing table (``ALTER TABLE … ADD COLUMN``).

        Parameters
        ----------
        name:
            Table to alter.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.
        column:
            Column definition to add.

        Raises
        ------
        LakebaseNotFoundError
            If the table does not exist.
        LakebaseAlreadyExistsError
            If a column with the same name already exists.
        """
        raise NotImplementedError

    def drop_column(
        self, name: str, schema: str, database: str, column_name: str
    ) -> TableInfo:
        """Drop a column from a table (``ALTER TABLE … DROP COLUMN``).

        Parameters
        ----------
        name:
            Table to alter.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.
        column_name:
            Name of the column to remove.

        Raises
        ------
        LakebaseNotFoundError
            If the table or column does not exist.
        """
        raise NotImplementedError

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
        """Alter an existing column's type, nullability, default, or comment.

        Only the parameters that are not ``None`` are applied.

        Parameters
        ----------
        name:
            Table to alter.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.
        column_name:
            Column to alter.
        new_type:
            New SQL data type (e.g. ``"bigint"``).
        new_nullable:
            ``True`` to drop NOT NULL; ``False`` to add NOT NULL.
        new_default:
            New default expression, or ``""`` to drop the existing default.
        new_comment:
            New comment text, or ``""`` to clear the comment.

        Raises
        ------
        LakebaseNotFoundError
            If the table or column does not exist.
        LakebaseOperationError
            If the ``ALTER COLUMN`` statement fails (e.g. type cast error).
        """
        raise NotImplementedError

    def delete(
        self, name: str, schema: str, database: str, not_found_ok: bool = False
    ) -> None:
        """Drop a table.

        Parameters
        ----------
        name:
            Table to drop.
        schema:
            Schema that contains the table.
        database:
            Database that contains the schema.
        not_found_ok:
            If ``True``, silently do nothing if the table does not exist.

        Raises
        ------
        LakebaseNotFoundError
            If the table does not exist and ``not_found_ok=False``.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_columns(
        self, name: str, schema: str, database: str
    ) -> list[ColumnInfo]:
        """Query ``information_schema.columns`` and return column definitions."""
        raise NotImplementedError

    def _row_to_table_info(
        self, row: tuple, schema: str, database: str, columns: list[ColumnInfo]
    ) -> TableInfo:
        """Convert a raw ``information_schema.tables`` row to :class:`TableInfo`."""
        raise NotImplementedError
