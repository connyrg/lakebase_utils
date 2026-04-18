"""CRUD operations for PostgreSQL databases inside a Lakebase instance.

All methods connect to the instance's PostgreSQL endpoint via psycopg2.
``CREATE DATABASE`` and ``DROP DATABASE`` cannot run inside a transaction,
so the connection always uses ``autocommit = True`` (set by
:meth:`LakebaseClient.pg_connection`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

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
        """Create a new PostgreSQL database.

        Parameters
        ----------
        name:
            Name of the database to create.
        owner:
            Role that will own the database. Defaults to the connecting user.
        comment:
            Optional description stored as a database comment.
        exist_ok:
            If ``True``, silently return the existing database instead of
            raising :class:`LakebaseAlreadyExistsError`.

        Returns
        -------
        DatabaseInfo
            Metadata of the newly created (or pre-existing) database.

        Raises
        ------
        LakebaseAlreadyExistsError
            If the database already exists and ``exist_ok=False``.
        LakebaseOperationError
            If the ``CREATE DATABASE`` statement fails for any other reason.
        """
        raise NotImplementedError

    def get(self, name: str) -> DatabaseInfo:
        """Return metadata for a single database by name.

        Raises
        ------
        LakebaseNotFoundError
            If the database does not exist.
        """
        raise NotImplementedError

    def list(self) -> list[DatabaseInfo]:
        """Return metadata for all databases in the instance.

        System databases (``postgres``, ``template0``, ``template1``) are
        excluded from the results.
        """
        raise NotImplementedError

    def rename(self, name: str, new_name: str) -> DatabaseInfo:
        """Rename a database.

        Parameters
        ----------
        name:
            Current database name.
        new_name:
            New database name.

        Returns
        -------
        DatabaseInfo
            Metadata of the renamed database.

        Raises
        ------
        LakebaseNotFoundError
            If *name* does not exist.
        LakebaseAlreadyExistsError
            If *new_name* is already taken.
        """
        raise NotImplementedError

    def update_comment(self, name: str, comment: Optional[str]) -> DatabaseInfo:
        """Set or clear the comment on a database.

        Parameters
        ----------
        name:
            Database to update.
        comment:
            New comment text, or ``None`` to remove the existing comment.
        """
        raise NotImplementedError

    def delete(self, name: str, not_found_ok: bool = False) -> None:
        """Drop a database and all objects within it.

        Parameters
        ----------
        name:
            Database to drop.
        not_found_ok:
            If ``True``, silently do nothing if the database does not exist.

        Raises
        ------
        LakebaseNotFoundError
            If the database does not exist and ``not_found_ok=False``.
        LakebaseOperationError
            If the ``DROP DATABASE`` statement fails (e.g. active connections).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_database_info(self, row: tuple) -> DatabaseInfo:
        """Convert a raw ``pg_database`` row to :class:`DatabaseInfo`."""
        raise NotImplementedError
