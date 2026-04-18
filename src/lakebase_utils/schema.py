"""CRUD operations for schemas (namespaces) inside a Lakebase database.

Schemas are PostgreSQL ``SCHEMA`` objects.  All methods connect to the
target database via :meth:`LakebaseClient.pg_connection`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import psycopg2
import psycopg2.errors
from psycopg2 import sql as pgsql

from .exceptions import LakebaseAlreadyExistsError, LakebaseNotFoundError, LakebaseOperationError
from .models import SchemaInfo

if TYPE_CHECKING:
    from .client import LakebaseClient


class SchemaManager:
    """Create, read, update, and delete schemas within a Lakebase database.

    Obtained via ``client.schemas``.

    Example::

        client = LakebaseClient(host="...", token="...", pg_host="...")
        client.schemas.create("raw", database="analytics")
        schemas = client.schemas.list(database="analytics")
        client.schemas.rename("raw", "bronze", database="analytics")
        client.schemas.delete("bronze", database="analytics")
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create(
        self,
        name: str,
        database: str,
        owner: Optional[str] = None,
        comment: Optional[str] = None,
        exist_ok: bool = False,
    ) -> SchemaInfo:
        """Create a schema in *database*."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    if owner:
                        q = pgsql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                            pgsql.Identifier(name), pgsql.Identifier(owner)
                        )
                    else:
                        q = pgsql.SQL("CREATE SCHEMA {}").format(pgsql.Identifier(name))
                    cur.execute(q)
        except psycopg2.errors.DuplicateSchema:
            if exist_ok:
                return self.get(name, database)
            raise LakebaseAlreadyExistsError(
                f"Schema {name!r} already exists in database {database!r}."
            )
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"CREATE SCHEMA {name!r} failed: {exc}") from exc

        if comment:
            self.update_comment(name, database, comment)
        return self.get(name, database)

    def get(self, name: str, database: str) -> SchemaInfo:
        """Return metadata for a single schema by name."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.schema_name,
                               s.schema_owner,
                               obj_description(n.oid, 'pg_namespace') AS comment
                        FROM information_schema.schemata s
                        JOIN pg_namespace n ON n.nspname = s.schema_name
                        WHERE s.schema_name = %s
                        """,
                        (name,),
                    )
                    row = cur.fetchone()
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"Failed to fetch schema {name!r}: {exc}") from exc
        if row is None:
            raise LakebaseNotFoundError(
                f"Schema {name!r} not found in database {database!r}."
            )
        return self._row_to_schema_info(row, database)

    def list(self, database: str) -> list[SchemaInfo]:
        """Return metadata for all user-defined schemas in *database*.

        System schemas (``information_schema``, ``pg_catalog``, ``pg_toast``,
        and names starting with ``pg_``) are excluded.
        """
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.schema_name,
                               s.schema_owner,
                               obj_description(n.oid, 'pg_namespace') AS comment
                        FROM information_schema.schemata s
                        JOIN pg_namespace n ON n.nspname = s.schema_name
                        WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                          AND s.schema_name NOT LIKE 'pg_%'
                        ORDER BY s.schema_name
                        """
                    )
                    rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to list schemas in {database!r}: {exc}"
            ) from exc
        return [self._row_to_schema_info(r, database) for r in rows]

    def rename(self, name: str, new_name: str, database: str) -> SchemaInfo:
        """Rename a schema within *database*."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("ALTER SCHEMA {} RENAME TO {}").format(
                            pgsql.Identifier(name), pgsql.Identifier(new_name)
                        )
                    )
        except psycopg2.errors.InvalidSchemaName as exc:
            raise LakebaseNotFoundError(
                f"Schema {name!r} not found in database {database!r}."
            ) from exc
        except psycopg2.errors.DuplicateSchema as exc:
            raise LakebaseAlreadyExistsError(f"Schema {new_name!r} already exists.") from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"RENAME SCHEMA {name!r} failed: {exc}") from exc
        return self.get(new_name, database)

    def update_comment(self, name: str, database: str, comment: Optional[str]) -> SchemaInfo:
        """Set or clear the comment on a schema."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    if comment is None:
                        cur.execute(
                            pgsql.SQL("COMMENT ON SCHEMA {} IS NULL").format(
                                pgsql.Identifier(name)
                            )
                        )
                    else:
                        cur.execute(
                            pgsql.SQL("COMMENT ON SCHEMA {} IS %s").format(
                                pgsql.Identifier(name)
                            ),
                            (comment,),
                        )
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to update comment on schema {name!r}: {exc}"
            ) from exc
        return self.get(name, database)

    def delete(
        self,
        name: str,
        database: str,
        cascade: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        """Drop a schema."""
        try:
            with self._client.pg_connection(database=database) as conn:
                with conn.cursor() as cur:
                    suffix = pgsql.SQL("CASCADE") if cascade else pgsql.SQL("RESTRICT")
                    cur.execute(
                        pgsql.SQL("DROP SCHEMA {} {}").format(
                            pgsql.Identifier(name), suffix
                        )
                    )
        except psycopg2.errors.InvalidSchemaName as exc:
            if not_found_ok:
                return
            raise LakebaseNotFoundError(
                f"Schema {name!r} not found in database {database!r}."
            ) from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"DROP SCHEMA {name!r} failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_schema_info(self, row: tuple, database: str) -> SchemaInfo:
        """Convert a raw ``information_schema.schemata`` row to :class:`SchemaInfo`."""
        name, owner, comment = row
        return SchemaInfo(name=name, database=database, owner=owner, comment=comment)
