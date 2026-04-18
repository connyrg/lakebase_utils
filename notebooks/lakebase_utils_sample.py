# Databricks notebook source

# MAGIC %md
# MAGIC # lakebase-utils — Sample Notebook
# MAGIC
# MAGIC This notebook walks through the full lifecycle of a Lakebase Autoscaling instance using `lakebase-utils`:
# MAGIC
# MAGIC 1. Install and import
# MAGIC 2. Configuration — provide your **project name**, **branch name**, and **database name**
# MAGIC 3. Connect and discover the Lakebase instance
# MAGIC 4. Database → Schema → Table CRUD
# MAGIC 5. Rename objects
# MAGIC 6. Error handling patterns
# MAGIC 7. Clean up

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install

# COMMAND ----------

# MAGIC %pip install lakebase-utils --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Fill in the three required values below. Everything else is derived at runtime.

# COMMAND ----------

# ── Required inputs ──────────────────────────────────────────────────────────
PROJECT_NAME  = "my-project"      # Lakebase instance / project name
BRANCH_NAME   = "main"            # Branch or environment label (used in comments / tags)
DATABASE_NAME = "analytics"       # PostgreSQL database to create

# PostgreSQL endpoint — provided by your Databricks admin (read-only, cannot be created or updated via API)
PG_HOST = ""                      # e.g. "lb-abc123.postgres.database.azure.com"
PG_PORT = 5432
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
)

print("Client initialised.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover the Lakebase instance
# MAGIC
# MAGIC `client.instance.get()` fetches read-only control-plane metadata (state, capacity, creator).
# MAGIC The PostgreSQL endpoint cannot be created or updated via the API — it is provisioned by your
# MAGIC Databricks admin and must be supplied as `PG_HOST` in the configuration cell above.

# COMMAND ----------

instance = client.instance.get(PROJECT_NAME)

print(f"Instance  : {instance.name}")
print(f"State     : {instance.state}")
print(f"Capacity  : {instance.capacity_min} – {instance.capacity_max}")
print(f"Creator   : {instance.creator}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### List all instances in the workspace

# COMMAND ----------

all_instances = client.instance.list()
print(f"{len(all_instances)} instance(s) found:")
for inst in all_instances:
    print(f"  {inst.name!r:40s}  state={inst.state}")

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

# Fetch a single database by name
db_info = client.databases.get(DATABASE_NAME)
print(db_info)

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

# List all schemas in the database (system schemas are excluded)
schemas = client.schemas.list(database=DATABASE_NAME)
print(f"{len(schemas)} schema(s) in '{DATABASE_NAME}':")
for s in schemas:
    print(f"  {s.name}")

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

# List all tables in the schema
tables = client.tables.list(schema=SCHEMA_NAME, database=DATABASE_NAME)
print(f"{len(tables)} table(s) in '{DATABASE_NAME}.{SCHEMA_NAME}':")
for t in tables:
    print(f"  {t.name:30s}  cols={[c.name for c in t.columns]}")

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
