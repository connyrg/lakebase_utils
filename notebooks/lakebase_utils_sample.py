# Databricks notebook source

# MAGIC %md
# MAGIC # lakebase-utils — Sample Notebook
# MAGIC
# MAGIC This notebook walks through the full lifecycle of a Lakebase Autoscaling instance using `lakebase-utils`:
# MAGIC
# MAGIC 1. Install and import
# MAGIC 2. Configuration — provide your **project name**, **branch name**, **database name**, and **grantee role**
# MAGIC 3. Connect and discover the Lakebase project
# MAGIC 4. Database operations
# MAGIC 5. Schema operations + access control
# MAGIC 6. Table operations + access control
# MAGIC 7. Rename objects
# MAGIC 8. Error handling patterns
# MAGIC 9. Clean up
# MAGIC
# MAGIC ### Access control primer
# MAGIC In PostgreSQL, permissions are **not inherited** from parent objects.
# MAGIC To read a table a user needs three independent grants:
# MAGIC `CONNECT` on the database → `USAGE` on the schema → `SELECT` on the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install

# COMMAND ----------

# MAGIC %pip install lakebase-utils --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Fill in all required values below. Everything else is derived at runtime.

# COMMAND ----------

# ── Required inputs ──────────────────────────────────────────────────────────
PROJECT_NAME  = "my-project"      # Lakebase project name / ID
BRANCH_NAME   = "main"            # Branch name
DATABASE_NAME = "analytics"       # PostgreSQL database to create

# Role or user to grant access to (e.g. a service principal name or Databricks user)
GRANTEE_ROLE  = "analyst_role"

# PostgreSQL connection details — provided by your Databricks admin (read-only, cannot be created or updated via API)
PG_HOST     = ""    # e.g. "lb-abc123.postgres.database.azure.com"
PG_PORT     = 5432
PG_ENDPOINT = ""    # e.g. "projects/my-project/branches/main/endpoints/primary"
                    # Used by the SDK to generate a short-lived OAuth token (pg_password)
# ─────────────────────────────────────────────────────────────────────────────

# Auth — prefer environment variables (DATABRICKS_HOST / DATABRICKS_TOKEN)
# or leave the strings below empty and the SDK will pick up workspace defaults.
DATABRICKS_HOST  = ""             # e.g. "https://<workspace>.azuredatabricks.net"
DATABRICKS_TOKEN = ""             # e.g. "dapi..."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect

# COMMAND ----------

from lakebase_utils import LakebaseClient

client = LakebaseClient(
    host=DATABRICKS_HOST or None,
    token=DATABRICKS_TOKEN or None,
    pg_host=PG_HOST,
    pg_port=PG_PORT,
    pg_endpoint=PG_ENDPOINT,
)

print("Client initialised.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover the Lakebase project
# MAGIC
# MAGIC `client.instance.get()` fetches read-only control-plane metadata (state, creator).
# MAGIC The PostgreSQL endpoint cannot be created or updated via the API — it is provisioned by your
# MAGIC Databricks admin and must be supplied as `PG_HOST` in the configuration cell above.

# COMMAND ----------

instance = client.instance.get(PROJECT_NAME)

print(f"Project   : {instance.name}  (id={instance.instance_id})")
print(f"State     : {instance.state}")
print(f"Creator   : {instance.creator}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### List all projects in the workspace

# COMMAND ----------

all_instances = client.instance.list()
print(f"{len(all_instances)} project(s) found:")
for inst in all_instances:
    print(f"  {inst.name!r:40s}  state={inst.state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Inspect branches and endpoints
# MAGIC
# MAGIC Branches and endpoints are read-only — they are provisioned by your Databricks admin.
# MAGIC Use `list_endpoints` to confirm the `PG_HOST` you were given.

# COMMAND ----------

branches = client.instance.list_branches(PROJECT_NAME)
print(f"Branches for '{PROJECT_NAME}':")
for b in branches:
    print(f"  {b['name']}")

endpoints = client.instance.list_endpoints(PROJECT_NAME, BRANCH_NAME)
print(f"\nEndpoints for branch '{BRANCH_NAME}':")
for ep in endpoints:
    s = ep.get("status", {})
    print(f"  {ep['name']}  →  {s.get('host')}:{s.get('port')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Database operations

# COMMAND ----------

# Create — exist_ok=True silently returns the existing DB if it already exists
db = client.databases.create(
    name=DATABASE_NAME,
    comment=f"Created from branch {BRANCH_NAME!r}",
    exist_ok=True,
)
print(f"Database  : {db.name}  owner={db.owner}  comment={db.comment!r}")

# COMMAND ----------

# List all user databases (system DBs are excluded)
databases = client.databases.list()
print(f"{len(databases)} database(s):")
for d in databases:
    print(f"  {d.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema operations

# COMMAND ----------

SCHEMA_NAME = "raw"

schema = client.schemas.create(
    name=SCHEMA_NAME,
    database=DATABASE_NAME,
    comment="Landing zone for raw ingestion",
    exist_ok=True,
)
print(f"Schema    : {schema.database}.{schema.name}  owner={schema.owner}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Check schema access
# MAGIC
# MAGIC Query `information_schema.role_usage_grants` to see who currently has `USAGE`
# MAGIC on the schema.  An empty result means only the owner can see inside it.

# COMMAND ----------

with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT grantee, privilege_type, is_grantable
            FROM information_schema.role_usage_grants
            WHERE object_type = 'SCHEMA'
              AND object_schema = %s
            ORDER BY grantee
            """,
            (SCHEMA_NAME,),
        )
        rows = cur.fetchall()

if rows:
    print(f"Current USAGE grants on schema '{SCHEMA_NAME}':")
    for grantee, priv, grantable in rows:
        print(f"  {grantee:30s}  {priv}  (grantable={grantable})")
else:
    print(f"No USAGE grants found on schema '{SCHEMA_NAME}' — only the owner has access.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Grant schema access
# MAGIC
# MAGIC Two grants are needed before a user can touch any table in this schema:
# MAGIC - `CONNECT` on the database (one-time, per database)
# MAGIC - `USAGE` on the schema
# MAGIC
# MAGIC Note: `USAGE` lets a role see and interact with objects inside the schema.
# MAGIC It does **not** grant read/write on individual tables — that comes in section 6b.

# COMMAND ----------

from psycopg2 import sql as pgsql

# Grant CONNECT on the database
with client.pg_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            pgsql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                pgsql.Identifier(DATABASE_NAME),
                pgsql.Identifier(GRANTEE_ROLE),
            )
        )
print(f"Granted CONNECT on database '{DATABASE_NAME}' to '{GRANTEE_ROLE}'.")

# Grant USAGE on the schema
with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            pgsql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                pgsql.Identifier(SCHEMA_NAME),
                pgsql.Identifier(GRANTEE_ROLE),
            )
        )
print(f"Granted USAGE on schema '{SCHEMA_NAME}' to '{GRANTEE_ROLE}'.")

# COMMAND ----------

# Verify the grant was applied
with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT grantee, privilege_type
            FROM information_schema.role_usage_grants
            WHERE object_type = 'SCHEMA'
              AND object_schema = %s
            ORDER BY grantee
            """,
            (SCHEMA_NAME,),
        )
        rows = cur.fetchall()

print(f"USAGE grants on schema '{SCHEMA_NAME}' after grant:")
for grantee, priv in rows:
    print(f"  {grantee:30s}  {priv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Table operations

# COMMAND ----------

from lakebase_utils import ColumnInfo

TABLE_NAME = "events"

table = client.tables.create(
    name=TABLE_NAME,
    schema=SCHEMA_NAME,
    database=DATABASE_NAME,
    columns=[
        ColumnInfo("id",         "bigint",      nullable=False),
        ColumnInfo("event_type", "text",         nullable=False),
        ColumnInfo("payload",    "jsonb"),
        ColumnInfo("created_at", "timestamptz",  default="now()"),
    ],
    comment=f"Events table — branch {BRANCH_NAME!r}",
    exist_ok=True,
)
print(f"Table     : {table.database}.{table.schema}.{table.name}")
print(f"Columns   : {[c.name for c in table.columns]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Check table access
# MAGIC
# MAGIC Query `information_schema.role_table_grants` to see existing privileges.
# MAGIC A freshly created table has no grants other than to its owner.

# COMMAND ----------

with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT grantee, privilege_type, is_grantable
            FROM information_schema.role_table_grants
            WHERE table_schema = %s
              AND table_name   = %s
            ORDER BY grantee, privilege_type
            """,
            (SCHEMA_NAME, TABLE_NAME),
        )
        rows = cur.fetchall()

if rows:
    print(f"Current grants on table '{SCHEMA_NAME}.{TABLE_NAME}':")
    for grantee, priv, grantable in rows:
        print(f"  {grantee:30s}  {priv:20s}  (grantable={grantable})")
else:
    print(f"No grants found on '{SCHEMA_NAME}.{TABLE_NAME}' — only the owner has access.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Grant table access
# MAGIC
# MAGIC Grant the minimum required privileges.  Common patterns:
# MAGIC
# MAGIC | Use case | Privileges |
# MAGIC |---|---|
# MAGIC | Read-only analyst | `SELECT` |
# MAGIC | ETL writer | `INSERT`, `UPDATE`, `DELETE` |
# MAGIC | Full access | `ALL PRIVILEGES` |
# MAGIC
# MAGIC > **Reminder:** `USAGE` on the schema (section 5b) must already be in place,
# MAGIC > otherwise the grantee cannot reach this table even with a table-level grant.

# COMMAND ----------

with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            pgsql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {}.{} TO {}").format(
                pgsql.Identifier(SCHEMA_NAME),
                pgsql.Identifier(TABLE_NAME),
                pgsql.Identifier(GRANTEE_ROLE),
            )
        )
print(f"Granted SELECT/INSERT/UPDATE/DELETE on '{SCHEMA_NAME}.{TABLE_NAME}' to '{GRANTEE_ROLE}'.")

# COMMAND ----------

# Verify the grant was applied
with client.pg_connection(database=DATABASE_NAME) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT grantee, privilege_type
            FROM information_schema.role_table_grants
            WHERE table_schema = %s
              AND table_name   = %s
            ORDER BY grantee, privilege_type
            """,
            (SCHEMA_NAME, TABLE_NAME),
        )
        rows = cur.fetchall()

print(f"Grants on '{SCHEMA_NAME}.{TABLE_NAME}' after grant:")
for grantee, priv in rows:
    print(f"  {grantee:30s}  {priv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Column operations

# COMMAND ----------

# Add a column
table = client.tables.add_column(
    TABLE_NAME, SCHEMA_NAME, DATABASE_NAME,
    ColumnInfo("user_id", "bigint", comment="FK to users table"),
)
print(f"Columns after add  : {[c.name for c in table.columns]}")

# COMMAND ----------

# Alter a column — narrow the type of event_type
table = client.tables.alter_column(
    TABLE_NAME, SCHEMA_NAME, DATABASE_NAME,
    column_name="event_type",
    new_type="varchar(128)",
)
print("Column altered.")

# COMMAND ----------

# Drop a column
table = client.tables.drop_column(TABLE_NAME, SCHEMA_NAME, DATABASE_NAME, "user_id")
print(f"Columns after drop : {[c.name for c in table.columns]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Rename objects

# COMMAND ----------

# Rename table
renamed_table = client.tables.rename(
    TABLE_NAME, f"{TABLE_NAME}_v2",
    schema=SCHEMA_NAME, database=DATABASE_NAME,
)
print(f"Table renamed to : {renamed_table.name}")
TABLE_NAME = renamed_table.name  # keep variable in sync

# COMMAND ----------

# Rename schema
renamed_schema = client.schemas.rename(SCHEMA_NAME, "bronze", database=DATABASE_NAME)
print(f"Schema renamed to : {renamed_schema.name}")
SCHEMA_NAME = renamed_schema.name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Error handling
# MAGIC
# MAGIC `lakebase-utils` raises typed exceptions so you can handle each case precisely.

# COMMAND ----------

from lakebase_utils import (
    LakebaseNotFoundError,
    LakebaseAlreadyExistsError,
    LakebaseOperationError,
)

try:
    client.databases.get("does_not_exist")
except LakebaseNotFoundError as exc:
    print(f"Not found      : {exc}")

try:
    client.databases.create(DATABASE_NAME)  # exist_ok defaults to False
except LakebaseAlreadyExistsError as exc:
    print(f"Already exists : {exc}")

try:
    client.tables.drop_column(TABLE_NAME, SCHEMA_NAME, DATABASE_NAME, "nonexistent_col")
except LakebaseNotFoundError as exc:
    print(f"Column missing : {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Clean up
# MAGIC
# MAGIC Drops everything created above. Skip this cell to keep the objects.

# COMMAND ----------

# Drop table
client.tables.delete(TABLE_NAME, SCHEMA_NAME, DATABASE_NAME, not_found_ok=True)
print(f"Dropped table '{TABLE_NAME}'.")

# Drop schema — cascade removes remaining objects
client.schemas.delete(SCHEMA_NAME, DATABASE_NAME, cascade=True, not_found_ok=True)
print(f"Dropped schema '{SCHEMA_NAME}'.")

# Drop database
client.databases.delete(DATABASE_NAME, not_found_ok=True)
print(f"Dropped database '{DATABASE_NAME}'.")
