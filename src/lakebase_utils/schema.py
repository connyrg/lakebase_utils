"""CRUD operations for schemas (namespaces) inside a Lakebase database.

Schemas are PostgreSQL ``SCHEMA`` objects.  All methods connect to the
target database via :meth:`LakebaseClient.pg_connection`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

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
        """Create a schema in *database*.

        Parameters
        ----------
        name:
            Name of the schema to create.
        database:
            Target database.
        owner:
            Role that will own the schema. Defaults to the connecting user.
        comment:
            Optional description.
        exist_ok:
            If ``True``, silently return the existing schema instead of
            raising :class:`LakebaseAlreadyExistsError`.

        Returns
        -------
        SchemaInfo
            Metadata of the newly created (or pre-existing) schema.

        Raises
        ------
        LakebaseAlreadyExistsError
            If the schema already exists and ``exist_ok=False``.
        LakebaseNotFoundError
            If *database* does not exist.
        LakebaseOperationError
            If the ``CREATE SCHEMA`` statement fails for any other reason.
        """
        raise NotImplementedError

    def get(self, name: str, database: str) -> SchemaInfo:
        """Return metadata for a single schema by name.

        Raises
        ------
        LakebaseNotFoundError
            If the schema does not exist in *database*.
        """
        raise NotImplementedError

    def list(self, database: str) -> list[SchemaInfo]:
        """Return metadata for all user-defined schemas in *database*.

        System schemas (``information_schema``, ``pg_catalog``, ``pg_toast``,
        and names starting with ``pg_``) are excluded.
        """
        raise NotImplementedError

    def rename(self, name: str, new_name: str, database: str) -> SchemaInfo:
        """Rename a schema within *database*.

        Parameters
        ----------
        name:
            Current schema name.
        new_name:
            New schema name.
        database:
            Database that contains the schema.

        Raises
        ------
        LakebaseNotFoundError
            If *name* does not exist.
        LakebaseAlreadyExistsError
            If *new_name* is already taken.
        """
        raise NotImplementedError

    def update_comment(self, name: str, database: str, comment: Optional[str]) -> SchemaInfo:
        """Set or clear the comment on a schema.

        Parameters
        ----------
        name:
            Schema to update.
        database:
            Database that contains the schema.
        comment:
            New comment text, or ``None`` to remove the existing comment.
        """
        raise NotImplementedError

    def delete(self, name: str, database: str, cascade: bool = False, not_found_ok: bool = False) -> None:
        """Drop a schema.

        Parameters
        ----------
        name:
            Schema to drop.
        database:
            Database that contains the schema.
        cascade:
            If ``True``, automatically drop all objects (tables, views, etc.)
            contained in the schema.  If ``False`` (default), the operation
            fails if the schema is not empty.
        not_found_ok:
            If ``True``, silently do nothing if the schema does not exist.

        Raises
        ------
        LakebaseNotFoundError
            If the schema does not exist and ``not_found_ok=False``.
        LakebaseOperationError
            If the schema is not empty and ``cascade=False``.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_schema_info(self, row: tuple, database: str) -> SchemaInfo:
        """Convert a raw ``information_schema.schemata`` row to :class:`SchemaInfo`."""
        raise NotImplementedError
