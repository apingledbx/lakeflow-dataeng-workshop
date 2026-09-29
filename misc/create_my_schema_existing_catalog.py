# Databricks notebook source
# MAGIC %md
# MAGIC # Before you start — create your personal schema (existing-catalog variant)
# MAGIC
# MAGIC Run this once. It creates your own schema `<catalog>.<your_short_name>` and makes you its
# MAGIC owner, so the labs have somewhere to write. This variant uses the customer's existing
# MAGIC catalog: set the `catalog` widget to the same catalog the workshop was set up in with
# MAGIC `misc/setup_workshop_existing_catalog.py`.
# MAGIC
# MAGIC Your `short_name` is derived from your login email: the part before `@`, with `.` and `-`
# MAGIC turned into `_` (e.g. `jane.doe@example.com` becomes `jane_doe`).
# MAGIC
# MAGIC No Vocareum, no pre-assigned id — everyone self-serves from here.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Workshop catalog (same as setup)")

# COMMAND ----------

import re

# Catalog names may contain hyphens (e.g. dev-monet); allow them and backtick-quote in SQL.
_CATALOG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError(
        "Set the 'catalog' widget to the workshop catalog (the one used in "
        "misc/setup_workshop_existing_catalog.py). Ask your instructor if unsure."
    )
if not _CATALOG_RE.fullmatch(CATALOG):
    raise ValueError(
        "Set 'catalog' to a UC catalog identifier: letters, numbers, underscores, and hyphens "
        "(e.g. dev-monet); it must start with a letter, number, or underscore."
    )

# Backtick-quoted catalog for SQL (handles hyphenated names like `dev-monet`).
CAT = f"`{CATALOG}`"

# COMMAND ----------

user = spark.sql("SELECT current_user()").first()[0]
short_name = user.split("@")[0].replace(".", "_").replace("-", "_").lower()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.`{short_name}` COMMENT 'Personal workshop schema for {user}'")
spark.sql(f"ALTER SCHEMA {CAT}.`{short_name}` OWNER TO `{user}`")

print("=" * 60)
print(f"  Your schema is:  {CATALOG}.{short_name}")
print(f"  Use SHORT_NAME = {short_name!r} everywhere the lab guide asks for it.")
print("=" * 60)
