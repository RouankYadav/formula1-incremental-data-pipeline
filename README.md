# Formula 1 Incremental Data Pipeline (Azure Databricks)

A batch-based incremental data pipeline on Azure Databricks. It loads Formula 1 data through **Bronze → Silver → Gold** Delta Lake tables and processes only new data on each run, instead of rebuilding everything from scratch.

**Tech stack:** Azure Databricks · PySpark · Spark SQL · Delta Lake · Unity Catalog · ADLS Gen2 · Lakeflow Jobs

> Built as a hands-on learning project while following a Databricks data engineering course. The architecture and batch-control approach follow that course's design.

---

## Problem

A full refresh reprocesses all historical data on every run, which becomes slow and expensive as data grows. This pipeline tracks which batches have been processed and loads only the new one.

## Architecture

```text
 Source files (landing area in ADLS, organised by dataset and batch_id)
                          |
                          v
   Bronze  - raw Delta tables, schema enforced, load metadata added
                          |
                          v
   Silver  - cleaned and standardised (duplicates and null keys removed)
                          |
                          v
   Gold    - analytics-ready facts, dimensions and aggregates

   Orchestrated with Lakeflow Jobs | Governed with Unity Catalog
```

## How incremental loading works

A `batch_control` table records the state of every batch (`batch_id`, `batch_status`).

1. Identify the next batch to process.
2. Register it as `in_progress`.
3. Load only that batch through Bronze, Silver and Gold.
4. Mark it `complete`.

Because status is stored per batch, an interrupted run stays visible as `in_progress` instead of looking finished.

## Loading strategy by dataset

| Dataset | Source behaviour | Write pattern |
|---|---|---|
| circuits, races, constructors, drivers | Mostly stable reference data | Append + Overwrite |
| results, sprints | Records can change after first load | Append + Merge |

Reference data can be replaced safely, while records that may change need a merge so updates don't create duplicates.

## Layers

- **Bronze:** controlled entry point that keeps raw data, enforces schema and adds load metadata.
- **Silver:** standardises column names, removes duplicates and filters rows with null business keys.
- **Gold:** dimensional model for analytical questions such as driver performance by season and constructor performance over time.

## Governance and orchestration

- **Unity Catalog** organises data as catalog → schema → tables, with landing files kept separate from the Bronze, Silver and Gold tables.
- Landing files are stored in **ADLS Gen2** and exposed through an external location.
- A **Lakeflow Job** runs the notebooks in dependency order (batch control → Bronze → Silver → Gold → mark complete) with scheduling and run monitoring.

## Repository structure

```text
00-common/         shared helpers and configuration
01-setup/          catalog, schema and external location setup
02-bronze/         raw ingestion into Bronze tables
03-silver/         cleaning and standardisation
04-gold/           dimensional and analytics tables
05-analytics/      analytical queries
06-orchestration/  Lakeflow Job definition
```

## Possible improvements

- Add automated data quality checks and alerting on failed runs.
- Add CI/CD for notebook deployment.
- Add streaming ingestion (e.g. Auto Loader) for near-real-time loading.

## License

Apache-2.0
