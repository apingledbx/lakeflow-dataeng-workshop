# Databricks notebook source
# MAGIC %md
# MAGIC # Before you start — create your personal schema
# MAGIC
# MAGIC Run this once. It creates your own schema `de_workshop.<your_short_name>` and makes you
# MAGIC its owner, so Labs 2-4 have somewhere to write. Your `short_name` is derived from
# MAGIC your login email: the part before `@`, with `.` and `-` turned into `_`
# MAGIC (e.g. `jane.doe@example.com` → `jane_doe`).
# MAGIC
# MAGIC No Vocareum, no pre-assigned id — everyone self-serves from here.

# COMMAND ----------

CATALOG = "de_workshop"

user = spark.sql("SELECT current_user()").first()[0]
short_name = user.split("@")[0].replace(".", "_").replace("-", "_").lower()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{short_name} COMMENT 'Personal workshop schema for {user}'")
spark.sql(f"ALTER SCHEMA {CATALOG}.{short_name} OWNER TO `{user}`")

print("=" * 60)
print(f"  Your schema is:  {CATALOG}.{short_name}")
print(f"  Use SHORT_NAME = {short_name!r} everywhere the lab guide asks for it.")
print("=" * 60)
