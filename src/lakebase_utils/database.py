"""CRUD operations for PostgreSQL databases inside a Lakebase instance.

All methods connect to the instance's PostgreSQL endpoint via psycopg2.
``CREATE DATABASE`` and ``DROP DATABASE`` cannot run inside a transaction,
so the connection always uses ``autocommit = True`` (set by
:meth:`LakebaseClient.pg_connection`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import psycopg2
import psycopg2.errors
from psycopg2 import sql as pgsql

from .exceptions import LakebaseAlreadyExistsError, LakebaseNotFoundError, LakebaseOperationError
from .models import DatabaseInfo

if TYPE_CHECKING:
    from .client import LakebaseClient


class DatabaseManager:
    """Create, read, update, and delete PostgreSQL databases.

    Obtained via ``client.databases``.

    Example::

        client = LakebaseClient(host="...", token="...", pg_host="...")
        client.databases.create("analytics")
        dbs = client.databases.list()
        client.databases.rename("analytics", "analytics_v2")
        client.databases.delete("analytics_v2")
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create(
        self,
        name: str,
        owner: Optional[str] = None,
        comment: Optional[str] = None,
        exist_ok: bool = False,
    ) -> DatabaseInfo:
        """Create a new PostgreSQL database."""
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    if owner:
                        q = pgsql.SQL("CREATE DATABASE {} OWNER {}").format(
                            pgsql.Identifier(name), pgsql.Identifier(owner)
                        )
                    else:
                        q = pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(name))
                    cur.execute(q)
        except psycopg2.errors.DuplicateDatabase:
            if exist_ok:
                return self.get(name)
            raise LakebaseAlreadyExistsError(f"Database {name!r} already exists.")
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"CREATE DATABASE {name!r} failed: {exc}") from exc

        if comment:
            self.update_comment(name, comment)
        return self.get(name)

    def get(self, name: str) -> DatabaseInfo:
        """Return metadata for a single database by name."""
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT d.datname,
                               pg_get_userbyid(d.datdba) AS owner,
                               shobj_description(d.oid, 'pg_database') AS comment
                        FROM pg_database d
                        WHERE d.datname = %s
                        """,
                        (name,),
                    )
                    row = cur.fetchone()
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"Failed to fetch database {name!r}: {exc}") from exc
        if row is None:
            raise LakebaseNotFoundError(f"Database {name!r} not found.")
        return self._row_to_database_info(row)

    def list(self) -> list[DatabaseInfo]:
        """Return metadata for all databases in the instance.

        System databases (``postgres``, ``template0``, ``template1``) are
        excluded from the results.
        """
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT d.datname,
                               pg_get_userbyid(d.datdba) AS owner,
                               shobj_description(d.oid, 'pg_database') AS comment
                        FROM pg_database d
                        WHERE d.datistemplate = false
                          AND d.datname NOT IN ('postgres', 'template0', 'template1')
                        ORDER BY d.datname
                        """
                    )
                    rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"Failed to list databases: {exc}") from exc
        return [self._row_to_database_info(r) for r in rows]

    def rename(self, name: str, new_name: str) -> DatabaseInfo:
        """Rename a database."""
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("ALTER DATABASE {} RENAME TO {}").format(
                            pgsql.Identifier(name), pgsql.Identifier(new_name)
                        )
                    )
        except psycopg2.errors.InvalidCatalogName as exc:
            raise LakebaseNotFoundError(f"Database {name!r} not found.") from exc
        except psycopg2.errors.DuplicateDatabase as exc:
            raise LakebaseAlreadyExistsError(f"Database {new_name!r} already exists.") from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"RENAME DATABASE {name!r} failed: {exc}") from exc
        return self.get(new_name)

    def update_comment(self, name: str, comment: Optional[str]) -> DatabaseInfo:
        """Set or clear the comment on a database."""
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    if comment is None:
                        cur.execute(
                            pgsql.SQL("COMMENT ON DATABASE {} IS NULL").format(
                                pgsql.Identifier(name)
                            )
                        )
                    else:
                        cur.execute(
                            pgsql.SQL("COMMENT ON DATABASE {} IS %s").format(
                                pgsql.Identifier(name)
                            ),
                            (comment,),
                        )
        except psycopg2.Error as exc:
            raise LakebaseOperationError(
                f"Failed to update comment on database {name!r}: {exc}"
            ) from exc
        return self.get(name)

    def delete(self, name: str, not_found_ok: bool = False) -> None:
        """Drop a database and all objects within it."""
        try:
            with self._client.pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        pgsql.SQL("DROP DATABASE {}").format(pgsql.Identifier(name))
                    )
        except psycopg2.errors.InvalidCatalogName as exc:
            if not_found_ok:
                return
            raise LakebaseNotFoundError(f"Database {name!r} not found.") from exc
        except psycopg2.Error as exc:
            raise LakebaseOperationError(f"DROP DATABASE {name!r} failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_database_info(self, row: tuple) -> DatabaseInfo:
        """Convert a raw ``pg_database`` row to :class:`DatabaseInfo`."""
        name, owner, comment = row
        return DatabaseInfo(name=name, owner=owner, comment=comment)
