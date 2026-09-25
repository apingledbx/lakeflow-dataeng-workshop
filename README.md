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

See `Labguide.py` for the step-by-step exercises (Lab 1 Zerobus ingestion, Lab 2 SDP, Lab 3 Genie Code, Lab 4 Continuous medallion).

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
| `zerobus_region` | `us-west-2` | Region for the Zerobus endpoint (e.g. `us-west-2`, `eastus`). Blank skips Lab 1 (Zerobus) provisioning. Must be a Zerobus-supported region. |
| `zerobus_managed_location` | (blank) | Optional fallback, see below. Leave blank in the common case. |

### Zerobus storage (Lab 1)

Zerobus writes directly into a Delta table. As of ~Sept 2026 it supports **default storage**
(Public Preview, AWS + Azure), so on most serverless / Free Edition workspaces you need **no
external storage** — leave `zerobus_managed_location` blank. Requirements:

- Enable the workspace preview **Settings → Previews → "Zerobus Ingest Default Storage"** (plus base Zerobus). On Free Edition the toggle occasionally isn't exposed; if so, contact the Zerobus team.
- Zerobus-supported region, AWS or Azure (not GCP). Free Edition also has a daily credit cap that can stop ingestion.
- The setup's B6 gRPC smoke test is the real check; if it returns HTTP 403, the default-storage preview isn't active on that workspace.

**Optional fallback** (workspace without the default-storage preview, where a bare default-storage
table gets a 403 at insert): set `zerobus_managed_location` to a real external-location URL
(`databricks external-locations list`), e.g. `s3://<bucket>/ops_data_zerobus` (AWS) or
`abfss://<container>@<account>.dfs.core.windows.net/ops_data_zerobus` (Azure), and re-run — the
`ops_data.zerobus` schema is pinned there instead. Free Edition cannot create external locations,
so it relies on the default-storage preview above.

## Attendee flow

1. Clone this repo into the workspace (Workspace, Create, Git folder).
2. Run `misc/create_my_schema.py` once to create your `de_workshop.<short_name>` schema.
3. Open `Labguide.py` (imports as a notebook), using your `short_name` wherever it asks for it.
