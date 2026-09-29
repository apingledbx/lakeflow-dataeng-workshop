# Databricks notebook source

# MAGIC %md
# MAGIC # Workshop setup (single existing catalog, no Zerobus)
# MAGIC
# MAGIC Use this variant when the workshop runs inside a customer's **existing** Unity Catalog
# MAGIC catalog and nobody has rights to `CREATE CATALOG`. Everything the workshop needs, both the
# MAGIC shared landing data and the per-attendee schemas, lives in the one catalog you pass in via
# MAGIC the `catalog` widget. There is no `ops_data` catalog and no Zerobus provisioning here.
# MAGIC
# MAGIC It is idempotent (safe to re-run) and does not touch per-attendee schemas.
# MAGIC
# MAGIC **What this notebook does, all inside `<catalog>`:**
# MAGIC 1. Verify the catalog exists and is reachable (it is NOT created here).
# MAGIC 2. Schema `<catalog>.shared` (if not exists).
# MAGIC 3. Managed volume `<catalog>.shared.landing` (if not exists).
# MAGIC 4. Folder `booking_fraud_flags/` in that volume seeded with JSONL fraud markers for a
# MAGIC    configurable percentage of distinct `booking_id`s from `samples.wanderbricks.booking_updates`.
# MAGIC 5. Grants to `account users`: USE CATALOG on the catalog, USE_SCHEMA on `<catalog>.shared`,
# MAGIC    READ_VOLUME on `<catalog>.shared.landing`, and CREATE SCHEMA on the catalog so attendees
# MAGIC    self-serve their own schema `<catalog>.<short_name>` via `misc/create_my_schema.py`.
# MAGIC
# MAGIC **Differences vs `setup_workshop.py`:** no `CREATE CATALOG`, no second `ops_data` catalog,
# MAGIC and Part B (Zerobus SP / OAuth secret / config table / gRPC smoke test) is removed entirely.
# MAGIC
# MAGIC > Thread the same catalog name through the lab notebooks and `misc/create_my_schema.py`, and
# MAGIC > point any lab that reads the shared landing at `<catalog>.shared.landing` (this variant
# MAGIC > does not use `ops_data`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Runner check (informational, not a hard gate)
# MAGIC
# MAGIC Unlike `setup_workshop.py`, this variant creates no account-wide objects (no service
# MAGIC principals, no OAuth secrets), so it does not require a workspace admin. It does require
# MAGIC that the runner can create a schema/volume in the target catalog and grant on it. If those
# MAGIC privileges are missing, the relevant step below prints a clear warning telling you exactly
# MAGIC what to ask the catalog owner to run.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
_me = _w.current_user.me()
_groups = {g.display for g in (_me.groups or [])}
print(f"Running as: {_me.user_name}")
print(f"Workspace admin: {'yes' if 'admins' in _groups else 'no (fine for this notebook, as long as you have privileges on the target catalog)'}")

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Existing catalog (required)")
dbutils.widgets.text("fraud_pct", "3.0", "% of bookings to flag as fraud")
dbutils.widgets.text("num_files", "5", "Number of JSONL files to split the seed across")

# COMMAND ----------

import re

_CATALOG_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError(
        "Set the 'catalog' widget to the customer's existing catalog. This notebook does not "
        "create a catalog; the catalog must already exist and you must have privileges on it."
    )
if not _CATALOG_RE.fullmatch(CATALOG):
    raise ValueError(
        "Set 'catalog' to a simple UC identifier: letters, numbers, and underscores; "
        "it must start with a letter or underscore."
    )

try:
    FRAUD_PCT = float(dbutils.widgets.get("fraud_pct"))
except ValueError as e:
    raise ValueError("Set 'fraud_pct' to a numeric percentage, e.g. 3.0.") from e

try:
    NUM_FILES = int(dbutils.widgets.get("num_files"))
except ValueError as e:
    raise ValueError("Set 'num_files' to a positive integer, e.g. 5.") from e

if not 0.0 <= FRAUD_PCT <= 100.0:
    raise ValueError("Set 'fraud_pct' between 0 and 100.")
if NUM_FILES < 1:
    raise ValueError("Set 'num_files' to at least 1.")

print(f"catalog={CATALOG}  fraud_pct={FRAUD_PCT}%  num_files={NUM_FILES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify the catalog exists (it is NOT created here)

# COMMAND ----------

# The catalog must already exist and be reachable by the runner. We do a light USE CATALOG
# probe (works with just USE CATALOG privilege) and give an actionable error if it fails.
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    print(f"Catalog {CATALOG!r} is reachable.")
except Exception as e:
    raise RuntimeError(
        f"Cannot use catalog {CATALOG!r}. This notebook does not create catalogs. Confirm the "
        f"catalog name with the customer and that you have at least USE CATALOG on it. "
        f"Underlying error: {e}"
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create shared schema and landing volume (idempotent)
# MAGIC
# MAGIC Both live inside the single target catalog. Requires CREATE SCHEMA / CREATE VOLUME on it.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.shared COMMENT 'Workshop shared assets (landing volume for the lab source data)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.shared.landing COMMENT 'Shared landing for workshop seed data'")

VOLUME_PATH = f"/Volumes/{CATALOG}/shared/landing/booking_fraud_flags"
dbutils.fs.mkdirs(VOLUME_PATH)
print(f"Volume folder ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data generator — seed fraud markers (% of bookings, written as JSONL)
# MAGIC
# MAGIC Re-run overwrites the seeded files so attendees always see a fresh, consistent set.

# COMMAND ----------

from pyspark.sql import functions as F

REASONS = [
    "velocity_check_failed",
    "stolen_card_report",
    "unusual_location",
    "device_fingerprint_mismatch",
    "suspicious_pattern",
]

bookings = (
    spark.table("samples.wanderbricks.booking_updates")
    .select("booking_id")
    .distinct()
)

fraction = FRAUD_PCT / 100.0
flagged = (
    bookings.sample(withReplacement=False, fraction=fraction, seed=42)
    .withColumn("flag", F.lit("fraud"))
    .withColumn(
        "reason",
        F.element_at(F.array(*[F.lit(r) for r in REASONS]),
                     (F.abs(F.hash("booking_id")) % len(REASONS)) + 1),
    )
    .withColumn(
        "flagged_at",
        F.date_format(
            F.expr("current_timestamp() - make_interval(0, 0, 0, abs(hash(booking_id)) % 90)"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        ),
    )
    .withColumn(
        "confidence",
        F.round(F.lit(0.70) + (F.abs(F.hash("booking_id")) % 3000) / 10000.0, 4),
    )
)

marker_count = flagged.count()
print(f"Generated {marker_count} fraud markers ({FRAUD_PCT}% of distinct bookings)")

# COMMAND ----------

# Clear any prior seed files so re-runs produce a clean directory, then write JSONL.
for f in dbutils.fs.ls(VOLUME_PATH):
    if f.name.endswith(".json") or f.name.startswith("_"):
        dbutils.fs.rm(f.path)

(flagged
    .repartition(NUM_FILES)
    .write
    .mode("overwrite")
    .format("json")
    .save(VOLUME_PATH))

# Rename part-*.json files so the directory stays tidy (Auto Loader reads either form).
for f in dbutils.fs.ls(VOLUME_PATH):
    if f.name.startswith("part-") and f.name.endswith(".json"):
        idx = f.name.split("-")[1]
        dbutils.fs.mv(f.path, f"{VOLUME_PATH}/fraud_markers_{idx}.json")
    elif f.name.startswith("_"):
        dbutils.fs.rm(f.path)

print("Seed files written:")
for f in dbutils.fs.ls(VOLUME_PATH):
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grants (best-effort) — VOLUME is read-only for attendees
# MAGIC
# MAGIC Each grant is attempted independently. If the runner is not the catalog owner / lacks
# MAGIC MANAGE, a grant may fail; the notebook keeps going and prints exactly what a catalog owner
# MAGIC needs to run. The data generator above does not depend on these grants.

# COMMAND ----------

def try_grant(sql: str, desc: str):
    try:
        spark.sql(sql)
        print(f"OK   : {desc}")
    except Exception as e:
        print(f"SKIP : {desc}")
        print(f"       could not grant ({type(e).__name__}). Ask the catalog owner to run:")
        print(f"         {sql}")

# Attendees self-serve their own schema `<catalog>.<short_name>` via misc/create_my_schema.py,
# so they need USE CATALOG + CREATE SCHEMA. They also read the shared landing volume.
try_grant(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`",
          f"USE CATALOG on {CATALOG} -> account users")
try_grant(f"GRANT CREATE SCHEMA ON CATALOG {CATALOG} TO `account users`",
          f"CREATE SCHEMA on {CATALOG} -> account users (attendees self-serve their schema)")
try_grant(f"GRANT USE_SCHEMA ON SCHEMA {CATALOG}.shared TO `account users`",
          f"USE_SCHEMA on {CATALOG}.shared -> account users")
try_grant(f"GRANT READ_VOLUME ON VOLUME {CATALOG}.shared.landing TO `account users`",
          f"READ_VOLUME on {CATALOG}.shared.landing -> account users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification

# COMMAND ----------

sample = (
    spark.read.format("json")
    .load(VOLUME_PATH)
    .limit(5)
)
display(sample)

total = spark.read.format("json").load(VOLUME_PATH).count()
print(f"Total markers in volume: {total}")
print(f"Expected ~{int(47726 * FRAUD_PCT / 100)} at {FRAUD_PCT}% of ~47,726 distinct bookings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("WORKSHOP SETUP (single existing catalog, no Zerobus) — SUMMARY")
print("=" * 70)
print(f"Catalog             : {CATALOG} (existing; used for BOTH shared assets and attendee schemas)")
print(f"Shared schema       : {CATALOG}.shared")
print(f"Landing volume      : {CATALOG}.shared.landing")
print(f"Seed folder         : {VOLUME_PATH}")
print(f"Fraud markers seeded: {marker_count} ({FRAUD_PCT}% of distinct bookings)")
print(f"Attendee self-serve : misc/create_my_schema.py -> {CATALOG}.<short_name>  (point it at this catalog)")
print("Zerobus provisioning: skipped (not part of this variant)")
print("=" * 70)
