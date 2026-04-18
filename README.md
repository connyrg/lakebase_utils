# lakebase-utils

Utility package for **Databricks Lakebase Autoscaling** operations.

Provides a single `LakebaseClient` entry point for:

- **Control-plane** — discovering Lakebase instances via the Databricks SDK
- **Data-plane** — managing PostgreSQL databases, schemas, and tables via psycopg2

---

## Requirements

- Python ≥ 3.10
- `databricks-sdk >= 0.30`
- `psycopg2-binary >= 2.9`

---

## Installation

```bash
pip install lakebase-utils
```

For local development:

```bash
git clone <repo-url>
cd lakebase-utils
pip install -e ".[dev]"
```

---

## Quick start

```python
from lakebase_utils import LakebaseClient, ColumnInfo

# 1. Connect
client = LakebaseClient(
    host="https://<workspace>.azuredatabricks.net",
    token="dapi...",
)

# 2. Discover instance and wire up the PostgreSQL endpoint
instance = client.instance.get("my-lakebase")
client.set_pg_endpoint(instance.pg_host, instance.pg_port)

# 3. Create a database, schema, and table
client.databases.create("analytics", exist_ok=True)
client.schemas.create("raw", database="analytics", exist_ok=True)
client.tables.create(
    name="events",
    schema="raw",
    database="analytics",
    columns=[
        ColumnInfo("id",         "bigint",     nullable=False),
        ColumnInfo("event_type", "text",        nullable=False),
        ColumnInfo("payload",    "jsonb"),
        ColumnInfo("created_at", "timestamptz", default="now()"),
    ],
    exist_ok=True,
)
```

---

## Authentication

Authentication is resolved in this order:

| Method | How |
|---|---|
| Personal Access Token | `token=` kwarg or `DATABRICKS_TOKEN` env var |
| Service Principal | `client_id=` + `client_secret=` kwargs, or `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` env vars |
| SDK default chain | Omit credentials — the Databricks SDK tries `.databrickscfg`, Azure CLI, etc. |

`DATABRICKS_HOST` is always required (or pass `host=`).

---

## API reference

### `LakebaseClient`

```python
LakebaseClient(
    host=None,           # Databricks workspace URL
    token=None,          # Personal Access Token
    client_id=None,      # Service Principal client ID
    client_secret=None,  # Service Principal client secret
    pg_host=None,        # PostgreSQL endpoint (set later via set_pg_endpoint)
    pg_port=5432,
    pg_user=None,        # defaults to "token"
    pg_password=None,    # defaults to the resolved Databricks token
)
```

#### `client.instance`

| Method | Description |
|---|---|
| `get(instance_name)` | Fetch metadata for a single instance |
| `list()` | List all instances visible in the workspace |

#### `client.databases`

| Method | Description |
|---|---|
| `create(name, *, comment, exist_ok)` | Create a database |
| `get(name)` | Fetch database metadata |
| `list()` | List all user databases |
| `rename(name, new_name)` | Rename a database |
| `update_comment(name, comment)` | Set or clear the database comment |
| `delete(name, *, not_found_ok)` | Drop a database |

#### `client.schemas`

| Method | Description |
|---|---|
| `create(name, database, *, comment, exist_ok)` | Create a schema |
| `get(name, database)` | Fetch schema metadata |
| `list(database)` | List all user schemas in a database |
| `rename(name, new_name, database)` | Rename a schema |
| `update_comment(name, database, comment)` | Set or clear the schema comment |
| `delete(name, database, *, cascade, not_found_ok)` | Drop a schema |

#### `client.tables`

| Method | Description |
|---|---|
| `create(name, schema, database, columns, *, comment, exist_ok)` | Create a table |
| `get(name, schema, database)` | Fetch table metadata including columns |
| `list(schema, database)` | List all tables in a schema |
| `rename(name, new_name, schema, database)` | Rename a table |
| `update_comment(name, schema, database, comment)` | Set or clear the table comment |
| `add_column(name, schema, database, column)` | Add a column |
| `drop_column(name, schema, database, column_name)` | Drop a column |
| `alter_column(name, schema, database, column_name, *, new_type, new_nullable, new_default, new_comment)` | Alter a column |
| `delete(name, schema, database, *, not_found_ok)` | Drop a table |

---

## Exceptions

All exceptions inherit from `LakebaseError`.

| Exception | When raised |
|---|---|
| `LakebaseAuthError` | Auth/config failure at client initialisation |
| `LakebaseConnectionError` | Cannot reach the PostgreSQL endpoint |
| `LakebaseNotFoundError` | Resource does not exist |
| `LakebaseAlreadyExistsError` | Resource already exists (`exist_ok=False`) |
| `LakebaseOperationError` | DDL statement failed for any other reason |

---

## Running tests

```bash
pytest
```

---

## Sample notebook

A complete walkthrough is available in [`notebooks/lakebase_utils_sample.py`](notebooks/lakebase_utils_sample.py) — import it directly into your Databricks workspace.
