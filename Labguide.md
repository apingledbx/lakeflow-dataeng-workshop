# AI-powered Data Engineering with Lakeflow

**Version 2.1 - Sept 2026**

👋 Welcome. This is the lab guide for the Databricks Data Engineering workshop. We're excited to have you here!

Over the next ~75 minutes we'll work through the core data engineering knowledge every data engineer should have (ingestion, transformation, and orchestration) using the newest Databricks products and OSS frameworks across four labs.

Take your time, ask questions, and don't worry about breaking anything, your schema is yours alone. Let's build. 🚀

> **Audience**: entry- to mid-level data engineers, with little or some prior Databricks knowledge. You have a workspace with Unity Catalog and serverless enabled, and you create your own schema `de_workshop.<short_name>` in a one-time setup step below.
> This is an instructor-led course, not a DIY manual. 

### Overview

This Labguide lives in this repo (`Labguide.md`). It is adapted from the upstream Databricks Technical Marketing workshop at [databricks/tmm — Lakeflow-DataEng-Workshop](https://github.com/databricks/tmm/tree/main/Lakeflow-DataEng-Workshop) (original material by the Databricks TMM team; see the [credits](#credits) at the end). 

- **Lab 1 — Work with Zerobus Ingest to push IoT data (ingestion)** *(live instructor demo; attendees may follow along)*: one `ingest_record(...)` call via the official `databricks-zerobus-ingest-sdk` (gRPC) lands a row in `ops_data.zerobus.measurements`, with credentials fetched from a shared UC config table. Reference files in [`labs/01-Zerobus/`](./labs/01-Zerobus/).
- **Lab 2 — Manually code an SDP pipeline**: streaming table in **Python**, materialized view in **SQL** with three data-quality expectations wired in from the start. Reference files in [`labs/02-SDP/`](./labs/02-SDP/).
- **Lab 3 — Learn how to use Genie Code as a data engineer**: all-**SQL** pipeline (AutoCDC + Auto Loader + join gold MV), produced from a single Genie Code prompt, and verified by you before it runs. Reference files in [`labs/03-GenieCode/`](./labs/03-GenieCode/).
- **Lab 4 — Build a continuous medallion pipeline**: a **continuous** SDP pipeline fed by **two** `rate` sources (a readings feed and a sensor registry). Bronze and silver are **streaming tables** (each silver kept current by **AutoCDC** SCD Type 1), gold is a **materialized view** that joins the two silvers by zone, and you watch the downstream tables update live. Reference file in [`labs/04-Continuous/`](./labs/04-Continuous/).

## Important — your SHORT_NAME and your schema

We separate each attendee's schemas and pipelines by a **SHORT_NAME** so nobody clashes. Your
`SHORT_NAME` is derived from your login email: the part before `@`, with `.` and `-` turned
into `_`, lowercased. Example: `jane.doe@example.com` gives `SHORT_NAME = jane_doe`.

Your personal schema is `de_workshop.SHORT_NAME`, and it is writable by you.

### Before you start — create your schema (run once)

Open [`misc/create_my_schema.py`](./misc/create_my_schema.py) from the cloned repo, attach
serverless, and **Run all**. It figures out your `SHORT_NAME` automatically, creates
`de_workshop.SHORT_NAME`, makes you the owner, and prints the exact schema name to use.
Copy that `SHORT_NAME` and use it everywhere this guide says `SHORT_NAME`.

Throughout this guide, replace `SHORT_NAME` with that exact value.

## Prerequisites (already done by the setup notebook)

- The catalog `de_workshop` exists and you can create your own schema in it (Step above). Your schema `de_workshop.SHORT_NAME` is writable by you.
- A shared volume exists at `/Volumes/ops_data/shared/landing/` with a seeded subdirectory `booking_fraud_flags/` containing JSON fraud markers keyed by `booking_id`. The volume is **read-only** for attendees (every attendee has `READ_VOLUME`, nobody has `WRITE_VOLUME`), so one attendee cannot disrupt another.
- The Zerobus target table `ops_data.zerobus.measurements` (`id, city, temperature, comment`), the shared service principal `workshop-zerobus-sp` (with `USE CATALOG` on `ops_data`, `USE SCHEMA` on `ops_data.zerobus`, and `MODIFY + SELECT` on the table), and the config table `ops_data.zerobus.config` (single row holding `client_id`, `client_secret`, `workspace_url`, `workspace_id`, `zerobus_endpoint`) are all pre-provisioned for Lab 1.
- This lab runs completely serverless.
- You can read `samples.bakehouse.*` and `samples.wanderbricks.*` (public sample data).

### Substitutions

Three placeholders show up throughout — resolve them once here, then paste blocks run as-is.

| Placeholder | What to use |
|---|---|
| `SHORT_NAME` | Your short name, derived from your login email (see preceding section). Example: `jane_doe`. Your schema is `de_workshop.SHORT_NAME`. Throughout the lab, replace SHORT_NAME with your own value. |
| `de_workshop` | The **attendee** catalog. Per-`SHORT_NAME` schemas live here, and Lab 2, Lab 3, and Lab 4 outputs land in `de_workshop.SHORT_NAME`. Fixed. |
| `ops_data` | The **shared ops** catalog. Holds the read-only landing volume `/Volumes/ops_data/shared/landing/` (Lab 3 source) and the Zerobus tables `ops_data.zerobus.measurements` + `ops_data.zerobus.config` (Lab 1). Fixed. |
| `<course_warehouse_name>` / `<course_warehouse_id>`  | The course SQL warehouse. Your instructor shares the exact name and ID. |

## One-time setup — Clone this workshop repo

Clone this repo into your Workspace once at the start. You get this lab guide and all the labs locally.

1. Workspace sidebar → **Workspace** → **Create** → **Git folder**.
2. In the **Create Git folder** dialog:
   - **Git repository URL**: `https://github.com/apingledbx/lakeflow-dataeng-workshop`  *(your instructor shares this)*
   - **Git provider**: GitHub
   - **Git folder name**: `de-workshop-repo`
3. Click **Create Git folder**. The repo clones into `de-workshop-repo/` in your workspace.

Most of those lab folders have reference files only. Some folders include notebooks that you can run directly (for example `misc/create_my_schema.py`), as described below.


## Lab 1 — Work with Zerobus Ingest to push IoT data (ingestion)

Lab 1 is **ingestion**: you play the IoT producer, emitting events straight into a Delta table in the Lakehouse via Zerobus Ingest. Later labs transform data that already lives in the Lakehouse; here you get it *in*.

**What's already provisioned for you:**

- **Zerobus table** — a Delta table `ops_data.zerobus.measurements` (`id STRING, city STRING, temperature FLOAT, comment STRING`). Every event lands here. The table lives in the shared `ops_data` catalog (separate from your attendee catalog `de_workshop`) so attendee schemas and shared ops assets stay cleanly governed.
- **Producer identity** — a service principal `workshop-zerobus-sp` with `USE CATALOG` on `ops_data`, `USE SCHEMA` on `ops_data.zerobus`, and `MODIFY + SELECT` on that one table. The SDK authenticates as this SP from your notebook (the full UC chain is required for the OAuth `authorization_details` payload).
- **Config table** — This is only required to execute the lab. Typically these values are available in your IoT producer. We use the table `ops_data.zerobus.config`, a single row holding `client_id`, `client_secret`, `workspace_url`, `workspace_id`, and `zerobus_endpoint`. The notebook (IoT producer) reads its config data from here.

**Overview of what you do:** open the notebook, set three parameters (city, temperature, comment), use the official **Databricks Zerobus Ingest SDK**, which opens a gRPC stream, ingests one event with a fresh UUID, flushes for durability, closes. 

Then you verify the event landed — first inside the notebook, then from a SQL warehouse as a downstream consumer.

**Shared Zerobus table, thousands of simultaneous producers.** This same `ops_data.zerobus.measurements` table is the target for *every* attendee in the workshop. When the instructor signals go, all of you fire `ingest_record` and write to the same Delta table. Zerobus is built to absorb exactly this shape of load: many concurrent streams converging on one table. 

By the end of the lab you'll see every attendee's events sitting alongside your own — a live demo of what a real fleet of IoT producers looks like on the wire.

### Target

| catalog | schema | table | columns |
|---|---|---|---|
| `ops_data` | `zerobus` | `measurements` | `id STRING, city STRING, temperature FLOAT, comment STRING` |


### Step 1a — Open the notebook from the cloned repo

In the Workspace sidebar, navigate to your cloned `de-workshop-repo/labs/01-Zerobus/send_city_iot_data.py` and click to open it. The notebook runs directly from the Git folder.

The first cell installs the Zerobus Ingest SDK via `%pip install` and restarts Python so the install takes effect (~10-30 seconds). Run it before the rest of the cells.

### Step 1b — Fill in the widgets at the top

- **City** — your city (e.g. `Munich`)
- **Temperature (°C)** — any float (e.g. `21.5`)
- **Comment (optional)** — a free-form note (e.g. `Hello Zerobus`); leave the default or blank if you don't care.

### Step 1c — run the **Submit** cell

That cell calls `submit_iot_record(CITY, TEMPERATURE, COMMENT)`, which the notebook's plumbing cell defines. The plumbing cell is marked **⛔ DO NOT MODIFY** 

```python
sdk = ZerobusSdk(_SERVER_ENDPOINT, unity_catalog_url=_WORKSPACE_URL)
stream = sdk.create_stream(_CLIENT_ID, _CLIENT_SECRET, table_props, options)
stream.ingest_record(json.dumps(record))
stream.flush()
stream.close()
```

The SDK handles the OAuth token exchange and scoping internally — the attendee never sees an `authorization_details` payload or a bearer token. On success you'll see:

```
✅ Sent to ops_data.zerobus.measurements: {'id': '…', 'city': 'Munich', 'temperature': 21.5, 'comment': 'Hello Zerobus'}
```

### Step 1d — Verify your row landed

Pick **one** of the two surfaces below. The data is the same governed Delta table either way; the choice is whether you read it from the notebook (still attached to the serverless runtime that just wrote it) or from a SQL warehouse (the consumer view a dashboard would use).

Zerobus is **at-least-once** at the protocol level — durability ACKs come back per record, and a client that retries on transport errors may produce duplicates. Order isn't guaranteed across producers.

**Option A — in the notebook.** The last cell runs a `%sql` query against `ops_data.zerobus.measurements`, ordered by city and temperature. Find your row by the city you typed at the top — it appears within a few seconds.

**Option B — in Databricks SQL.** Read the table as a downstream consumer. This proves the row is a real row in a real governed table, queryable by anything that can talk to a SQL warehouse — a BI dashboard, a downstream pipeline, a JDBC client, `ai_query(...)`.

1. Workspace sidebar → **SQL Editor** → **New query**.
2. In the top-right warehouse picker, select the **course warehouse** — `<course_warehouse_name>` (ID `<course_warehouse_id>`). It was provisioned for you by the training materials, so it's already running; you don't need to start a warehouse of your own.
3. Paste and run:

```sql
SELECT id, city, temperature, comment
FROM ops_data.zerobus.measurements
ORDER BY city, temperature;
```

You should see every attendee's row, including your own. In a real production deployment this is the query a dashboard would run, refreshed on a schedule — same table, same grants, no separate serving tier.


### What to take away

- **Direct-to-Delta ingest** — one `ingest_record` call, one row, no intermediate bus. The SDK holds a gRPC stream with durability ACKs, so it scales from a single-record demo like this to high-throughput production producers.
- **Fine-grained OAuth — like a hotel keycard, not a master key.** The SDK mints tokens scoped via `authorization_details` to one table: even if the SP's client_secret leaked, the only thing it could do is append rows to `measurements`. No `SELECT *`, no `DELETE`, no `DROP`.
- **SDK over REST** — the `databricks-zerobus-ingest-sdk` uses gRPC with a persistent stream and durability ACKs (higher throughput, simpler retries) and handles all the OAuth + `authorization_details` plumbing internally. The Zerobus REST API is available too.

> Reference notebook: [`labs/01-Zerobus/send_city_iot_data.py`](./labs/01-Zerobus/send_city_iot_data.py).

---

## Lab 2 — Manually code a Lakeflow SDP pipeline and Job


In this lab you'll hand-code an end-to-end SDP pipeline: one **streaming table** in Python over the well-known Bakehouse sample dataset which is available in every Databricks workspace, and a **materialized view** implemented in SQL with three data quality constraints. One pipeline. Two files only. 

### Set up the pipeline in the Lakeflow Pipelines Editor

Before you write a single line, create the pipeline that hosts Steps 2a and 2b:

1. Workspace sidebar → **New** → **ETL pipeline**. The **Lakeflow Pipelines Editor** opens with a default name `New Pipeline <date> <time>`.

2. **Update pipeline name and root folder** Click the name of the pipeline, next to the pipeline symbol at the top of the editor → rename to `pipeline_SHORT_NAME`. Note you must have a unique pipeline name, this is why we use the SHORT_NAME here. If asked to rename the root folder that was automatically created under your home, confirm that too. 
3. **Update catalog/schema** Right of the pipeline name, click the catalog/schema selector (it opens the **Default location** dialog). Set it to the following values:
   - **Default catalog**: `de_workshop`
   - **Default schema**: copy your `SHORT_NAME` and click **Save**. Make sure to use your correct schema name, since it is writable for you but other schemas aren't writable. **So the pipeline will only run if you select the correct schema.** 
   
   The dropdown sometimes only offers *"Create schema"* even though your `SHORT_NAME` schema already exists — ignore that, the typed/copied literal is accepted. 


4. The default file `my_transformation.py` is already Python and Step 2a below uses Python. 

### Step 2a — streaming table (Python)

Use the **copy** button at the top-right of the code block to grab the snippet, then paste it into the editor. 

(If you ever see an `unexpected indent` error, it's because the editor auto-indented an empty leading line; use Genie's /fix to correct the code).

```python
from pyspark import pipelines as dp


@dp.table(
    name="sales_transactions",
    comment="Raw bakery transactions streamed from samples.bakehouse.sales_transactions",
)
def sales_transactions():
    # spark.readStream.table(...) inside @dp.table ⇒ streaming table
    return spark.readStream.table("samples.bakehouse.sales_transactions")
```

Rename the file to `sales_transactions.py` by clicking the file name in the **tab bar** (the title preceding the editor cell) and typing the new name.

Click **Run file**. Select pipeline graph at the bottom, and you will see one node `sales_transactions` (~3,333 rows) with its source Delta table. **Run file** only runs this transformation, not the whole pipeline, which typically has several transformations. 

### Step 2b — materialized view with data-quality expectations (SQL, copy and paste)

Click **Create pipeline asset** (the "+" in the asset browser) → **Transformation** → name it `sales_stats`, language **SQL** → **Create**. Paste the block below; it's the materialized view with three expectations wired in:

```sql
CREATE OR REFRESH MATERIALIZED VIEW sales_stats (
    --    Violations are counted in the event log; rows are still written.
    CONSTRAINT reasonable_avg_value
        EXPECT (avg_txn_value BETWEEN 1 AND 1000),

    --    Violating rows are excluded from the target; pipeline continues.
    CONSTRAINT nonneg_revenue
        EXPECT (gross_revenue >= 0) ON VIOLATION DROP ROW,

    --    Any violation aborts the whole pipeline update with the constraint name.
    CONSTRAINT known_product
        EXPECT (product IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Sales KPIs grouped by product, with data-quality expectations'
AS SELECT
    product,
    COUNT(*)                     AS txn_count,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(totalPrice), 2)    AS gross_revenue,
    ROUND(AVG(totalPrice), 2)    AS avg_txn_value,
    COUNT(DISTINCT customerID)   AS unique_customers,
    COUNT(DISTINCT franchiseID)  AS franchises_selling
FROM sales_transactions
GROUP BY product;
```

Click **Run pipeline**; this runs the entire pipeline. The DAG now shows `sales_transactions → sales_stats` (6 rows, one per product). Under Tables in the Expectations column you can see the data quality constraints and open the side panel.

SDP has **one** constraint syntax — `CONSTRAINT <name> EXPECT (<predicate>)` — and **three** violation behaviors: *log* (default), *drop row*, and *fail update*. For didactic reasons, we are wiring all three into one data set.

You might notice, that when running the pipeline the streaming table is not updated again (because it was run before) since it appends new data only once. You could run the pipeline with a full refresh to see this data loaded again or explicitly run that file again. 

**Key teaching points**
- Python for the streaming table 
- SQL for the materialized view. The relative name `sales_transactions` resolves against the pipeline's default catalog + schema.
- You can mix Python and SQL files — no special configuration needed.
- The bakehouse sample data is clean, so all three expectations pass and row counts match a constraint-free version.

**Is this MV incrementally maintained or fully recomputed?**

A materialized view is either *incrementally maintained* (only the rows that changed are reprocessed) or *fully recomputed* on refresh, depending on whether the SDP planner can rewrite the query as an incremental update. Simple projections, filters, and many aggregations qualify for incremental maintenance; `COUNT(DISTINCT …)` — which `sales_stats` uses twice — typically forces a **COMPLETE refresh** because distinct tracking isn't incrementally maintainable without a lot more state.

See Tables / Expectations for the details if a table is incrementalized or not. 

### Step 2c (Optional) — use Lakeflow Jobs to create a Workflow with SDP and a downstream action

Wrap the SDP pipeline and a downstream consumer notebook into a two-task job:

1. Workspace sidebar → **Jobs & Pipelines** → **Create** → **Job**. 
* Name it `workflow_SHORT_NAME`.
2. **Task 1**
* Select **Add another task type** and **ETL Pipeline**
    * Task name **my_pipeline**
    * Type **Pipeline**
    * For **Pipeline** select your `pipeline_SHORT_NAME` from Lab 2. 
    * Save Task
3. **Task 2**
    * Click on **Add task** select Notebook. 
    * Task name: `downstream`. 
    * Type **Notebook**
    * path `labs/02-SDP/downstream.py`  
    * Under **Depends on**, select `pipeline`.
4. Click **Run now**. 
* Verify the job executes. The pipeline runs first; on success the notebook fires and prints to its task log.
* Check out the possible triggers for a job


### Lab 2 take-away

In a few lines, you've built a streaming table ingest of bakehouse transactions, a materialized view that summarizes sales by product, and three data-quality expectations with different actions. 


The same shape, written without SDP, would be a streaming job, a batch job, and a scheduler — three separate systems to wire together and keep in sync. Here it lives in one pipeline, expressed as the *target table* you want, and the platform owns the rest.

Running the pipeline with an additional downstream action as a multi-step workflow gave you a production ready job that can be invoked by any Job trigger. 


![Lab 2 — completed pipeline run in the Lakeflow Pipelines Editor: streaming table sales_transactions (3.3K output records) feeds materialized view sales_stats (6 output records, 3 expectations, 100% written, 0% dropped)](https://raw.githubusercontent.com/apingledbx/lakeflow-dataeng-workshop/main/misc/images/lab1-ui.png)



---

## Lab 3 — learn how to use Genie Code as a data engineer

In Lab 2 you typed every line. Lab 3 you type *one*, the prompt. Genie Code is the AI-powered tooling that builds an entire SDP pipeline. It ingests from three different sources in SQL, creates JOINs and a gold table:
* AutoCDC on the `booking_updates` CDC feed
* Auto Loader on a JSON volume of fraud markers for certain bookings
* a plain streaming table on payments

You prompt, you review, you approve.

The skill this lab teaches isn't typing SQL. It's catching the draft that *looks* right and isn't.

### Set up a fresh pipeline

1. Workspace sidebar → **New** → **ETL pipeline**. Rename to `pipeline_SHORT_NAME_lab3`. The editor auto-creates a workspace folder of the same name under your home — Genie Code writes the five SQL files it generates there.
2. Set **Default catalog** to `de_workshop` and **Default schema** to `SHORT_NAME` (same as Lab 2).


### Open Genie Code

1. Upper-right of the workspace → click **Genie Code**. The side panel opens.
2. Genie Code always runs as an agent — there is no Agent/Chat toggle. The selector at the bottom of the pane sets response **quality** (**Auto** = highest quality, **Low** = lower cost); leave it on **Auto**.
3. Genie Code **auto-approves safe actions** (such as writing the SQL files) and only prompts before it *runs* code, showing **Allow** / **Skip** buttons plus an **Ask every time** dropdown. Reviewing the generated SQL *before* you let the pipeline run is the whole point of this lab — read each file, then **Allow** the run.

### The prompt

Paste the following into Genie Code Agent:

```text
build a SQL pipeline with SDP with catalog de_workshop and schema SHORT_NAME that answers the question:
"Is fraud risk related to party size and payment method?"

Inputs:
1. samples.wanderbricks.booking_updates — a CDC stream of booking state-update events 
2. samples.wanderbricks.payments streaming data 
3. /Volumes/ops_data/shared/landing/booking_fraud_flags/ with JSON files marking fraudulent bookings 

* mark all bookings with fraud in bookings_with_fraud.
* gold materialized view fraud_by_party_and_method: use bookings_with_fraud and payments 


Run the pipeline, report row counts, and answer the question about fraud and its correlation.
```

This prompt is deliberately higher-level — it states the *business question* and the *inputs*, and lets Genie Code plan the solution (table names, columns, join shape, aggregation form). This is the honest way to use an AI data engineering agent.

### Verify — the step that matters most

Because the prompt is rather high-level, Genie Code has room to make choices. **Don't worry if your solution looks slightly different, Genie Code is continuously improved.**

Before you approve the pipeline **run**, open each generated file and check it against the reference SQL below. Expected properties of a good generation:

- `bronze/bookings.sql` uses the **AutoCDC** pattern from `samples.wanderbricks.booking_updates`
- `bronze/fraud_flags.sql` uses Auto Loader with `STREAM(read_files(...))` on the JSON volume
- `bronze/payments.sql` is a plain stream over the Delta source
- `silver/bookings_with_fraud.sql` is a materialized view that joins bookings with fraud flags

- A gold table joins data from all three input sources

Note that AutoCDC with SCD1 is a time-lapse, not a scrapbook. Every update collapses into one current row per `booking_id` — the latest state wins, history fades.

**`SCD TYPE 1` vs `SCD TYPE 2`.** Type 1 keeps only the current row per `booking_id` — every update overwrites in place, no history. Type 2 keeps every historical version with `__START_AT` / `__END_AT` columns so you can query *as of* a past time. The lab uses Type 1 because the gold question asks about current state.

> **Two runs, two approvals.** Genie Code first does a **dry run** (validation only — it shows as completed but creates no tables), then prompts again to run the real **pipeline update** that materializes the tables and produces row counts. Approve **both**; if you stop after the dry run, your schema stays empty and there are no row counts to report.

### Ask Genie Code a follow-up

In the same Genie Code chat, ask a follow-up question:

```text
Explain the data flow in this pipeline end-to-end. Which node is incrementally maintained versus fully recomputed on refresh, and why?
```

### What you should see

![Lab 3 — Genie Code generated pipeline with bronze (bookings, fraud_flags, payments), silver (bookings_with_fraud) and gold (fraud_by_party_and_method) layers in the Lakeflow Pipelines Editor](https://raw.githubusercontent.com/apingledbx/lakeflow-dataeng-workshop/main/misc/images/lab2-dag.png)

The Lakeflow Pipelines Editor shows Genie Code's plan on the right, the generated SQL in the centre, and the resolved DAG with row counts at the bottom — three bronze streaming tables, one silver streaming table, and a gold materialized view. Use the row counts as your sanity check against the [Verify](#verify--the-step-that-matters-most) section.

---

## Lab 4 — Build a continuous medallion pipeline

Labs 2 and 3 ran **triggered**: you clicked Run, the pipeline processed the data once, and stopped. Lab 4 keeps the pipeline **running continuously**. **Two** independent sources produce rows non-stop, and you watch bronze, silver, and gold update live, with no clicking.

You'll build a medallion in one file, fed by **two sources**:

- **Two bronze streaming tables**, one per source, both append-only feeds from the built-in `rate` source:
  - `sensor_readings_bronze` — temperature readings (fast, ~10 rows/s).
  - `sensor_registry_bronze` — the sensor registry: which zone each sensor is in (slower, ~2 rows/s, and zones shift over time).
- **Two silver streaming tables via AutoCDC (SCD Type 1)**, one per feed, each collapsing its feed to *current state* per sensor and upserting in place:
  - `sensor_state_silver` — latest reading per sensor.
  - `sensor_zone_silver` — current zone per sensor.
- **One gold materialized view** that **joins the two silvers** on `sensor_id` and aggregates by zone. It recomputes as either silver changes.

This is the realistic shape: two systems emitting independently (a device feed and a registry feed), reconciled to current state, then joined for a business view.

### Set up the pipeline

1. Workspace sidebar → **New** → **ETL pipeline**. Rename it to `pipeline_SHORT_NAME_lab4`.
2. Set **Default catalog** to `de_workshop` and **Default schema** to your `SHORT_NAME` (same as Labs 2 and 3).

### Step 4a — paste the pipeline (Python, one file)

Create a transformation file named `continuous_medallion` (language **Python**) and paste the block below. It defines two bronze feeds, two AutoCDC silvers, and the gold join. Reference file: [`labs/04-Continuous/continuous_medallion.py`](./labs/04-Continuous/continuous_medallion.py).

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ---------- SOURCE 1 -> BRONZE: temperature readings feed ----------
@dp.table(
    name="sensor_readings_bronze",
    comment="Source 1: raw temperature readings generated continuously by a rate source.",
)
@dp.expect("valid_temperature", "temperature_c BETWEEN -20 AND 60")  # warn-only, like Lab 2
def sensor_readings_bronze():
    return (
        spark.readStream.format("rate").option("rowsPerSecond", "10").load()
        .withColumn("sensor_id", (col("value") % 25).cast("int"))
        .withColumn("temperature_c", expr("round(18 + rand() * 12, 2)"))
        .withColumn("status", expr("CASE WHEN temperature_c > 28 THEN 'HOT' ELSE 'OK' END"))
        .withColumnRenamed("value", "reading_seq")
        .withColumnRenamed("timestamp", "event_ts")
        .select("sensor_id", "temperature_c", "status", "event_ts", "reading_seq")
    )

# ---------- SOURCE 2 -> BRONZE: sensor registry feed (which zone each sensor is in) ----------
# A second, independent, slower rate source. The zone rotates slowly, so AutoCDC has real
# changes to apply.
@dp.table(
    name="sensor_registry_bronze",
    comment="Source 2: sensor->zone registry changes, generated continuously by a second rate source.",
)
def sensor_registry_bronze():
    return (
        spark.readStream.format("rate").option("rowsPerSecond", "2").load()
        .withColumn("sensor_id", (col("value") % 25).cast("int"))
        .withColumn(
            "zone",
            expr(
                "element_at(array('Zone-North','Zone-South','Zone-East','Zone-West'), "
                "CAST(((value % 25) + floor(value / 100)) % 4 AS INT) + 1)"
            ),
        )
        .withColumnRenamed("value", "registry_seq")
        .withColumnRenamed("timestamp", "updated_ts")
        .select("sensor_id", "zone", "updated_ts", "registry_seq")
    )

# ---------- SILVER 1: current reading per sensor via AUTO CDC (SCD Type 1) ----------
dp.create_streaming_table(
    name="sensor_state_silver",
    comment="Latest reading per sensor, kept current by AUTO CDC (SCD 1) off the readings feed.",
)
dp.create_auto_cdc_flow(
    target="sensor_state_silver",
    source="sensor_readings_bronze",
    keys=["sensor_id"],
    sequence_by="reading_seq",
    stored_as_scd_type=1,
)

# ---------- SILVER 2: current zone per sensor via AUTO CDC (SCD Type 1) ----------
dp.create_streaming_table(
    name="sensor_zone_silver",
    comment="Current zone per sensor, kept current by AUTO CDC (SCD 1) off the registry feed.",
)
dp.create_auto_cdc_flow(
    target="sensor_zone_silver",
    source="sensor_registry_bronze",
    keys=["sensor_id"],
    sequence_by="registry_seq",
    stored_as_scd_type=1,
)

# ---------- GOLD: join both silvers, aggregate by zone (materialized view) ----------
@dp.materialized_view(
    name="zone_stats_gold",
    comment="Live per-zone sensor counts and temperature stats, joining readings with the registry.",
)
def zone_stats_gold():
    readings = spark.read.table("sensor_state_silver")
    zones = spark.read.table("sensor_zone_silver")
    return (
        readings.join(zones, "sensor_id")
        .groupBy("zone")
        .agg(
            expr("COUNT(*) AS sensor_count"),
            expr("SUM(CASE WHEN status = 'HOT' THEN 1 ELSE 0 END) AS hot_sensors"),
            expr("ROUND(AVG(temperature_c), 2) AS avg_temp_c"),
            expr("ROUND(MIN(temperature_c), 2) AS min_temp_c"),
            expr("ROUND(MAX(temperature_c), 2) AS max_temp_c"),
        )
    )
```

### Step 4b — run once to see the DAG

Click **Run pipeline**. It runs one triggered update so you can see the shape: two bronze feeds (`sensor_readings_bronze`, `sensor_registry_bronze`) each feed a silver via AutoCDC (`sensor_state_silver`, `sensor_zone_silver`), and both silvers join into `zone_stats_gold`. Each silver shows **25** rows (one per sensor); gold shows one row per zone (four zones).

### Step 4c — switch to Continuous and watch it update

Open the pipeline **Settings** (gear icon) and set the **Pipeline mode** from **Triggered** to **Continuous**, then **Save** and start the pipeline. (In a Declarative Automation Bundle this is just `continuous: true` in the pipeline resource, the CI/CD way to ship the same thing.)

Now both rate sources run non-stop. Open a **SQL editor** on the course warehouse and run these a few times, ~20 seconds apart:

```sql
SELECT COUNT(*) AS readings_rows  FROM de_workshop.SHORT_NAME.sensor_readings_bronze;  -- climbs ~10/s
SELECT COUNT(*) AS registry_rows  FROM de_workshop.SHORT_NAME.sensor_registry_bronze;  -- climbs ~2/s
SELECT COUNT(*) AS state_sensors  FROM de_workshop.SHORT_NAME.sensor_state_silver;     -- stays at 25
SELECT COUNT(*) AS zone_sensors   FROM de_workshop.SHORT_NAME.sensor_zone_silver;      -- stays at 25
SELECT * FROM de_workshop.SHORT_NAME.zone_stats_gold ORDER BY zone;                    -- keeps moving
```

**What you should see** (numbers will differ):

- The two bronze feeds grow at different rates: `readings_rows` ~+10/s (e.g. 194 → 474 → 794), `registry_rows` ~+2/s (e.g. 49 → 105 → 169). Two independent sources.
- Both silvers hold steady at **25** (AutoCDC keeps one current row per sensor per feed).
- `zone_stats_gold` keeps recomputing as readings change and sensors shift zones, e.g.:

```
 zone       | sensor_count | hot_sensors | avg_temp_c | min_temp_c | max_temp_c
------------+--------------+-------------+------------+------------+-----------
 Zone-North |            7 |           3 |      25.51 |      20.03 |      29.94
 Zone-East  |            6 |           2 |      25.48 |      20.45 |      29.81
 Zone-South |            6 |           1 |      24.07 |      21.08 |      28.09
 Zone-West  |            6 |           1 |      21.55 |      18.66 |      28.73
```

### Step 4d — Stop the pipeline (do this)

The pipeline is **continuous and serverless**, so it keeps consuming compute until you stop it. In the Pipelines Editor with the pipeline open, click **Stop**. Always stop it once you've watched the tables update.

### Key teaching points

- **Two independent sources, reconciled then joined.** The readings feed and the registry feed arrive at different rates and are handled independently through their own bronze and silver, then combined once at gold. That's the everyday multi-source medallion shape.
- **AutoCDC gives you current-state for free.** Each silver never grows past 25 rows even though its bronze has thousands, because SCD Type 1 collapses the feed to the latest per key. Switch `stored_as_scd_type=1` to `2` and you'd keep full history with `__START_AT` / `__END_AT` instead.
- **Streaming tables append, materialized views recompute.** The four bronze/silver tables are streaming (incremental, append/upsert). Gold is an MV, so it re-derives its per-zone aggregate as either silver changes, which is why the numbers move.
- **Triggered vs continuous is a setting, not a rewrite.** The exact same dataset definitions run either way. Continuous just keeps the streams alive so downstream tables update on their own.

### Lab 4 take-away

One file, one pipeline, two live sources, and you have a self-maintaining bronze/silver/gold that reconciles each feed to current state and joins them for a business view. Streaming tables + AutoCDC + a gold MV is the everyday shape of a continuously updating multi-source medallion, and moving from triggered to continuous was a single setting.

---

## References and other demos

* [Get to know Genie Code: Lakeflow and Analytics](https://www.databricks.com/resources/demos/videos/get-know-genie-code)
* Complete [Lakeflow Demo: From messy sales data to AI insights](https://www.databricks.com/resources/demos/videos/lakeflow-action-gourmet-pipeline-demo-daiwt)
* Getting Started with [OSS Apache SDP, VS Code](https://github.com/databricks/tmm/tree/main/OSS-SDP-OpenSkyNetwork)
* Further watching: [Air Traffic Control with Apache Spark Structured Streaming, Real-Time Mode](https://www.databricks.com/resources/demos/videos/air-traffic-control-with-apache-spark-structured-streaming-real-time-mode)
* Looking for the next Data Engineering workshop, or other [Databricks workshops](https://www.databricks.com/events?event_type=workshop&region=all) for DBSQL, AI, Unity Catalog

## Credits

Adapted from the upstream Databricks Technical Marketing workshop: [databricks/tmm — Lakeflow-DataEng-Workshop](https://github.com/databricks/tmm/tree/main/Lakeflow-DataEng-Workshop). Original labs and material are by the Databricks TMM team. This fork reorders the labs (ingestion-first), removes the Real-Time Mode and CI/CD labs, replaces the Vocareum-based schema provisioning with a self-service model, adds the continuous two-source medallion lab, and makes the Zerobus storage setup cloud-agnostic.
