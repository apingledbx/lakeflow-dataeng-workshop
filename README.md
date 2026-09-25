# Lakeflow data engineering workshop

A focused, instructor-led Databricks data engineering workshop. A trimmed fork of the
Databricks TMM "Lakeflow-DataEng-Workshop": three core labs plus a continuous pipeline, no
Vocareum, and setup that works on AWS or Azure.

> **Based on / credit:** this is adapted from the upstream Databricks Technical Marketing
> workshop at [databricks/tmm — Lakeflow-DataEng-Workshop](https://github.com/databricks/tmm/tree/main/Lakeflow-DataEng-Workshop).
> Original labs and material are by the Databricks TMM team; this fork reorders the labs,
> removes the RTM and CI/CD labs, swaps the Vocareum flow for self-service schemas, adds a
> continuous two-source medallion lab, and makes the Zerobus setup cloud-agnostic.

## Technologies covered

- **Lakeflow Spark Declarative Pipelines (SDP)**: streaming tables, materialized views, and data-quality expectations, hand-coded in Python and SQL.
- **Genie Code**: AI-assisted pipeline authoring with human-in-the-loop verification.
- **Continuous pipelines + AutoCDC**: a continuous medallion fed by two `rate` sources, where bronze/silver are streaming tables (each silver via AutoCDC SCD Type 1) and a gold materialized view joins them and updates live.
- **Zerobus Ingest**: direct gRPC ingest into Delta tables via the official `databricks-zerobus-ingest-sdk`.

See [Labguide.ipynb](./Labguide.ipynb) for the step-by-step exercises (Lab 1 Zerobus ingestion, Lab 2 SDP, Lab 3 Genie Code, Lab 4 Continuous medallion).

> Note: the optional Real-Time Mode (RTM) and CI/CD DABs (Gourmet) labs from the upstream
> TMM workshop have been removed in this edition to keep it time-boxed.

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless enabled. Runs completely serverless.
- A running SQL warehouse for the SQL verification steps (share its name/id with attendees).
- **No Vocareum, no pre-assigned ids.** Each attendee creates their own schema
  `de_workshop.<short_name>` by running [`misc/create_my_schema.py`](./misc/create_my_schema.py)
  once. `short_name` is the part of their login email before `@`, with `.`/`-` turned into `_`
  (e.g. `jane.doe@example.com` becomes `jane_doe`).
- The [`misc/setup_workshop.py`](./misc/setup_workshop.py) notebook has been run once as a
  **workspace admin** to provision the two workshop catalogs:
  - `de_workshop` (attendee catalog; grants `account users` USE CATALOG + CREATE SCHEMA so attendees self-serve their own schema)
  - `ops_data` (shared assets: landing volume + seeded JSON fraud markers for Lab 3 (Genie Code); Zerobus target table, service principal, UC grants, and config table for Lab 1 (ingestion))

## Setup (admin, once)

Run `misc/setup_workshop.py` as a workspace admin. Widgets:

| Widget | Default | Notes |
|---|---|---|
| `catalog` | `de_workshop` | Attendee catalog. |
| `fraud_pct` | `3.0` | Percent of bookings seeded as fraud markers (Lab 3). |
| `num_files` | `5` | JSONL files to split the seed across. |
| `zerobus_region` | `us-west-2` | Region for the Zerobus endpoint (e.g. `us-west-2`, `eastus`). Blank skips Lab 1 (Zerobus) provisioning. |
| `zerobus_managed_location` | (blank) | See below. |

### Zerobus storage (Lab 1), cloud-agnostic

Zerobus writes directly into a Delta table and only accepts a table on **real customer cloud
storage** (S3 on AWS, ADLS on Azure), not workspace default storage. If your workspace's
catalogs land on default storage (common on serverless FEVM workspaces), setup will fail the
Zerobus preflight with a clear message. The fix:

1. Find a real external location: `databricks external-locations list` (use the workspace's own `*-ext-*` entry; the default managed catalog already uses it).
2. Set the `zerobus_managed_location` widget to a URL under it, for example
   `s3://<bucket>/ops_data_zerobus` (AWS) or
   `abfss://<container>@<account>.dfs.core.windows.net/ops_data_zerobus` (Azure).
3. Re-run setup. The `ops_data.zerobus` schema is pinned to that location and Lab 1 works.

If your workspace already defaults to real managed storage, leave the widget blank.

## Attendee flow

1. Clone this repo into the workspace (Workspace, Create, Git folder).
2. Run `misc/create_my_schema.py` once to create your `de_workshop.<short_name>` schema.
3. Open `Labguide.ipynb` as a notebook, using your `short_name` wherever it asks for it.
