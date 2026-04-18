"""lakebase-utils — utilities for Databricks Lakebase Autoscaling operations.

Typical usage::

    from lakebase_utils import LakebaseClient

    client = LakebaseClient(
        host="https://<workspace>.azuredatabricks.net",
        token="dapi...",
        pg_host="<lakebase-pg-endpoint>",
    )

    # Read-only instance info
    info = client.instance.get("my-lakebase")

    # Database CRUD
    client.databases.create("analytics")

    # Schema CRUD
    client.schemas.create("raw", database="analytics")

    # Table CRUD
    from lakebase_utils import ColumnInfo
    client.tables.create(
        "events", schema="raw", database="analytics",
        columns=[ColumnInfo("id", "bigint", nullable=False)],
    )
"""

__version__ = "0.1.0"

from .client import LakebaseClient
from .exceptions import (
    LakebaseAlreadyExistsError,
    LakebaseAuthError,
    LakebaseConnectionError,
    LakebaseError,
    LakebaseNotFoundError,
    LakebaseOperationError,
)
from .models import ColumnInfo, DatabaseInfo, InstanceInfo, SchemaInfo, TableInfo

__all__ = [
    # Client
    "LakebaseClient",
    # Exceptions
    "LakebaseError",
    "LakebaseAuthError",
    "LakebaseConnectionError",
    "LakebaseNotFoundError",
    "LakebaseAlreadyExistsError",
    "LakebaseOperationError",
    # Models
    "InstanceInfo",
    "DatabaseInfo",
    "SchemaInfo",
    "TableInfo",
    "ColumnInfo",
]
