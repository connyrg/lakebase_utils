"""Dataclass models returned by lakebase-utils managers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Instance / project
# ---------------------------------------------------------------------------


@dataclass
class InstanceInfo:
    """Read-only metadata for a Lakebase Autoscaling instance."""

    instance_id: str
    name: str
    state: str
    """Lifecycle state reported by Databricks (e.g. 'RUNNING', 'STOPPED')."""
    pg_host: str
    """PostgreSQL-compatible endpoint hostname."""
    pg_port: int
    """PostgreSQL-compatible endpoint port (default 5432)."""
    capacity_min: Optional[int] = None
    """Minimum autoscaling capacity (DBUs or connection units)."""
    capacity_max: Optional[int] = None
    """Maximum autoscaling capacity."""
    creator: Optional[str] = None
    """Identity that created the instance."""
    tags: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


@dataclass
class DatabaseInfo:
    """Metadata for a PostgreSQL database inside a Lakebase instance."""

    name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    tags: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@dataclass
class SchemaInfo:
    """Metadata for a schema (namespace) within a Lakebase database."""

    name: str
    database: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    tags: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------


@dataclass
class ColumnInfo:
    """Metadata for a single column in a Lakebase table."""

    name: str
    data_type: str
    """SQL type string as reported by information_schema (e.g. 'integer', 'text')."""
    nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class TableInfo:
    """Metadata for a table within a Lakebase database and schema."""

    name: str
    schema: str
    database: str
    columns: list[ColumnInfo] = field(default_factory=list)
    owner: Optional[str] = None
    comment: Optional[str] = None
    tags: dict[str, str] = field(default_factory=dict)
