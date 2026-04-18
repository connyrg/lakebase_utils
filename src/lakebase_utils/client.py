"""LakebaseClient — shared entry point for all lakebase-utils operations.

Authentication resolution order (mirrors databricks-sdk defaults):
  1. Explicit keyword arguments passed to ``LakebaseClient()``.
  2. Environment variables: ``DATABRICKS_HOST``, ``DATABRICKS_TOKEN``,
     ``DATABRICKS_CLIENT_ID``, ``DATABRICKS_CLIENT_SECRET``.

Usage::

    # PAT token (explicit)
    client = LakebaseClient(host="https://<workspace>.azuredatabricks.net", token="dapi...")

    # Service Principal (explicit)
    client = LakebaseClient(
        host="https://<workspace>.azuredatabricks.net",
        client_id="<sp-client-id>",
        client_secret="<sp-client-secret>",
    )

    # From environment variables only
    client = LakebaseClient()

    # Use sub-managers
    info = client.instance.get("my-lakebase-instance")
    client.databases.create("analytics")
    client.schemas.create("raw", database="analytics")
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg2
import psycopg2.extensions
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from .exceptions import LakebaseAuthError, LakebaseConnectionError


class LakebaseClient:
    """Unified client for Databricks Lakebase Autoscaling operations.

    Owns two connections:
    - ``_ws``: :class:`databricks.sdk.WorkspaceClient` for control-plane calls
      (instance metadata, listing instances).
    - PostgreSQL connection factory for data-plane DDL (databases, schemas, tables).

    Parameters
    ----------
    host:
        Databricks workspace URL, e.g. ``https://<workspace>.azuredatabricks.net``.
        Falls back to ``DATABRICKS_HOST`` env var.
    token:
        Personal Access Token. Falls back to ``DATABRICKS_TOKEN`` env var.
        Mutually exclusive with ``client_id`` / ``client_secret``.
    client_id:
        Service Principal application (client) ID.
        Falls back to ``DATABRICKS_CLIENT_ID`` env var.
    client_secret:
        Service Principal client secret.
        Falls back to ``DATABRICKS_CLIENT_SECRET`` env var.
    pg_host:
        PostgreSQL endpoint hostname for the Lakebase instance.
        Required for data-plane operations (databases / schemas / tables).
        Can be retrieved from :meth:`instance.get` and set later via
        :meth:`set_pg_endpoint`.
    pg_port:
        PostgreSQL endpoint port. Defaults to 5432.
    pg_user:
        PostgreSQL username. Defaults to ``token`` (Lakebase accepts the
        Databricks token as the password for the ``token`` user).
    pg_password:
        PostgreSQL password. Defaults to the resolved Databricks token.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        pg_host: Optional[str] = None,
        pg_port: int = 5432,
        pg_user: Optional[str] = None,
        pg_password: Optional[str] = None,
    ) -> None:
        resolved_host = host or os.environ.get("DATABRICKS_HOST")
        resolved_token = token or os.environ.get("DATABRICKS_TOKEN")
        resolved_client_id = client_id or os.environ.get("DATABRICKS_CLIENT_ID")
        resolved_client_secret = client_secret or os.environ.get("DATABRICKS_CLIENT_SECRET")

        if not resolved_host:
            raise LakebaseAuthError(
                "Databricks host is required. Pass host= or set DATABRICKS_HOST."
            )

        try:
            if resolved_token:
                cfg = Config(host=resolved_host, token=resolved_token)
            elif resolved_client_id and resolved_client_secret:
                cfg = Config(
                    host=resolved_host,
                    client_id=resolved_client_id,
                    client_secret=resolved_client_secret,
                )
            else:
                # Let databricks-sdk attempt its own credential chain
                cfg = Config(host=resolved_host)

            self._ws = WorkspaceClient(config=cfg)
        except Exception as exc:
            raise LakebaseAuthError(f"Failed to initialise Databricks client: {exc}") from exc

        # PostgreSQL data-plane settings
        self._pg_host = pg_host
        self._pg_port = pg_port
        self._pg_user = pg_user or "token"
        self._pg_password = pg_password or resolved_token

        # Lazy-initialised sub-managers (populated on first access)
        self._instance_manager: Optional[_InstanceManagerAccessor] = None
        self._database_manager: Optional[_DatabaseManagerAccessor] = None
        self._schema_manager: Optional[_SchemaManagerAccessor] = None
        self._table_manager: Optional[_TableManagerAccessor] = None

    # ------------------------------------------------------------------
    # Sub-manager accessors
    # ------------------------------------------------------------------

    @property
    def instance(self) -> "_InstanceManagerAccessor":
        from .instance import InstanceManager

        if self._instance_manager is None:
            self._instance_manager = InstanceManager(self)  # type: ignore[assignment]
        return self._instance_manager  # type: ignore[return-value]

    @property
    def databases(self) -> "_DatabaseManagerAccessor":
        from .database import DatabaseManager

        if self._database_manager is None:
            self._database_manager = DatabaseManager(self)  # type: ignore[assignment]
        return self._database_manager  # type: ignore[return-value]

    @property
    def schemas(self) -> "_SchemaManagerAccessor":
        from .schema import SchemaManager

        if self._schema_manager is None:
            self._schema_manager = SchemaManager(self)  # type: ignore[assignment]
        return self._schema_manager  # type: ignore[return-value]

    @property
    def tables(self) -> "_TableManagerAccessor":
        from .table import TableManager

        if self._table_manager is None:
            self._table_manager = TableManager(self)  # type: ignore[assignment]
        return self._table_manager  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # PostgreSQL connection helpers
    # ------------------------------------------------------------------

    def set_pg_endpoint(self, host: str, port: int = 5432) -> None:
        """Set (or update) the PostgreSQL endpoint for data-plane operations.

        Call this after resolving the endpoint from :meth:`instance.get`.
        """
        self._pg_host = host
        self._pg_port = port

    @contextmanager
    def pg_connection(
        self, database: str = "postgres"
    ) -> Generator[psycopg2.extensions.connection, None, None]:
        """Yield a psycopg2 connection to *database* on the Lakebase instance.

        The connection is closed when the context manager exits.

        Parameters
        ----------
        database:
            PostgreSQL database to connect to. Defaults to ``postgres``
            (used for DDL operations that cannot run inside a user database,
            such as ``CREATE DATABASE``).
        """
        if not self._pg_host:
            raise LakebaseConnectionError(
                "PostgreSQL endpoint is not configured. "
                "Pass pg_host= to LakebaseClient or call set_pg_endpoint() first."
            )
        try:
            conn = psycopg2.connect(
                host=self._pg_host,
                port=self._pg_port,
                dbname=database,
                user=self._pg_user,
                password=self._pg_password,
                connect_timeout=10,
            )
            conn.autocommit = True
            try:
                yield conn
            finally:
                conn.close()
        except psycopg2.OperationalError as exc:
            raise LakebaseConnectionError(
                f"Could not connect to Lakebase PostgreSQL endpoint "
                f"{self._pg_host}:{self._pg_port}/{database}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Type stubs used only for IDE type-checking of sub-manager properties
# (runtime types are the actual manager classes imported lazily above)
# ---------------------------------------------------------------------------

class _InstanceManagerAccessor:  # pragma: no cover
    pass


class _DatabaseManagerAccessor:  # pragma: no cover
    pass


class _SchemaManagerAccessor:  # pragma: no cover
    pass


class _TableManagerAccessor:  # pragma: no cover
    pass
