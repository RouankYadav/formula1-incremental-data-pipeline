# Formula 1 Incremental Data Loading Pipeline

An Azure Databricks data engineering project that implements **batch-based incremental data loading** for Formula 1 datasets using the **Medallion Architecture**, **Delta Lake**, **Unity Catalog**, and **Lakeflow Jobs**.

## Project Overview

This project processes Formula 1 data incrementally rather than rebuilding the entire dataset on every execution.

The pipeline is designed around a simple batch-based approach:

- Identify the next available `batch_id`
- Create a new batch
- Process only the data belonging to that batch
- Load the data through the Bronze, Silver, and Gold layers
- Mark the batch as complete

The course describes this approach as processing **only new data**, instead of processing all historical data during every execution.

## Architecture

```text
                         Formula 1 Source Data
                                  |
                                  v
                         +------------------+
                         |     Landing      |
                         |  Batch Data      |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |      Bronze      |
                         | Raw Delta Tables |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |      Silver      |
                         | Cleaned /        |
                         | Standardized    |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |       Gold       |
                         | Analytics-ready  |
                         | Delta Tables     |
                         +--------+---------+
                                  |
                                  v
                            BI / Analytics

                    Orchestrated using
                         Lakeflow Jobs
```

The overall project follows the course's Bronze → Silver → Gold architecture, where Bronze is the controlled/raw entry point, Silver contains cleaned and standardized data, and Gold contains business-level analytical structures.

## Technology Stack

- **Azure Databricks**
- **Apache Spark / PySpark**
- **Spark SQL**
- **Delta Lake**
- **Unity Catalog**
- **Azure Data Lake Storage (ADLS)**
- **Lakeflow Jobs**
- Python
- SQL

## Dataset

The project uses Formula 1 datasets covering:

- `circuits`
- `races`
- `constructors`
- `drivers`
- `results`
- `sprints`

The incremental project also uses a `batch_id` to identify individual data batches.

Example:

```text
batch_id = 2025-01
```

## Incremental Loading Strategy

### Full Refresh

In a full-refresh pipeline, every execution processes all available data.

```text
Batch 1 + Batch 2
       |
       v
Bronze -> Silver -> Gold
```

This becomes inefficient as historical data grows.

### Incremental Processing

The project instead processes only the new batch.

```text
Batch 1
  |
  v
Bronze -> Silver -> Gold

Batch 2
  |
  v
Only Batch 2 is processed
```

This reduces unnecessary processing of historical data.

The course notes that there are multiple ways to build incremental pipelines and uses a **simple batch-based approach** for this project.

## Batch Control

A `batch_control` mechanism is used to manage the state of processing.

Conceptually:

```text
                 +-------------------+
                 |   batch_control   |
                 +---------+---------+
                           |
                           v
                    Identify Next Batch
                           |
                           v
                     Create New Batch
                           |
                           v
                      Process Batch
                           |
                           v
                    Mark Batch Complete
```

The batch control information includes:

- `batch_id`
- `batch_status`

Example status progression:

```text
2025-01 -> in_progress -> complete
```

This provides a simple way to determine which batch should be processed and whether a previous batch completed successfully.

## Snapshot Data vs Change Data

The incremental pipeline distinguishes between different types of source data.

### Snapshot Data

Snapshot-style datasets represent the current state of the source data.

The project handles these datasets differently from datasets where individual records can change over time.

### Change Data

Change data contains new or changed records that need to be incorporated into the target tables.

The project uses different loading patterns depending on the dataset's characteristics.

## Loading Patterns

The course's incremental-processing design uses different write strategies across the Formula 1 datasets.

### Circuits, Races, Constructors and Drivers

These datasets use a combination of:

- Append
- Overwrite

across the relevant layers.

### Results and Sprints

These datasets use:

- Append
- Merge

where changed records need to be incorporated.

The exact write strategy is therefore dependent on whether the source behaves like snapshot data, historical data, or change data.

## Medallion Architecture

### Bronze Layer

Purpose:

- Controlled entry point for source data
- Schema enforcement
- Metadata addition
- Raw data preservation

The Bronze layer stores the ingested data as Delta tables.

### Silver Layer

Purpose:

- Clean and standardize data
- Remove duplicates
- Remove invalid records
- Reshape and structure data
- Prepare data for analytics

The course demonstrates transformations such as standardizing column names, removing duplicates, filtering null business keys, and making column values more consistent.

### Gold Layer

Purpose:

- Business-level analytical structures
- Dimensional modelling
- Facts and dimensions
- Aggregations for analytical queries

The Gold layer supports Formula 1 analytical questions such as driver performance by season and constructor performance over time.

## Data Governance

The project uses **Unity Catalog** to organize and govern the data.

The architecture follows the Unity Catalog hierarchy:

```text
Metastore
   |
   +-- Catalog
         |
         +-- Schema
               |
               +-- Tables
               +-- Views
               +-- Functions
               +-- Volumes
```

The incremental project uses a Formula 1 catalog/schema structure and separates the landing files from the Bronze, Silver, and Gold Delta tables.

## Storage

The project uses Azure Data Lake Storage together with Unity Catalog.

The incremental project setup includes an external location and landing area for the Formula 1 batch files.

Conceptually:

```text
ADLS
 |
 +-- landing/
       |
       +-- circuits
       +-- races
       +-- constructors
       +-- drivers
       +-- results
       +-- sprints
       +-- batch_id
```

## Orchestration

**Lakeflow Jobs** is used to orchestrate the production pipeline.

The course describes Lakeflow Jobs as a mechanism for:

- Running pipelines as coordinated workflows
- Scheduling execution
- Defining dependencies
- Handling failures
- Monitoring pipeline health

A production workflow can therefore be structured around:

```text
Identify Batch
      |
      v
Create Batch
      |
      v
Ingest / Process
      |
      v
Bronze
      |
      v
Silver
      |
      v
Gold
      |
      v
Mark Batch Complete
```

## Why Incremental Loading?

Incremental loading provides several advantages over full refreshes:

- Processes less data per execution
- Avoids repeatedly rebuilding historical data
- Reduces unnecessary compute
- Makes scheduled pipelines more scalable
- Provides batch-level processing control
- Makes production workflows easier to track

The key principle used in this project is:

> **Process new data instead of reprocessing all historical data on every run.**

## Project Structure

A suggested repository structure for implementing the project is:

```text
formula1-incremental-pipeline/
|
├── README.md
|
├── notebooks/
│   ├── 01_batch_control
│   ├── 02_bronze_ingestion
│   ├── 03_silver_transformation
│   └── 04_gold_transformation
|
├── jobs/
│   └── formula1_incremental_job
|
├── sql/
│   └── validation_queries
|
└── docs/
    └── architecture.md
```

Adjust the notebook names to match the actual names used in your Databricks workspace.

## Key Data Engineering Concepts Demonstrated

This project demonstrates:

- Incremental data loading
- Batch-based processing
- Batch control
- Snapshot vs. change data
- Append loading
- Overwrite loading
- Merge operations
- Medallion Architecture
- Delta Lake
- ACID transactions
- Data transformation with Spark
- Unity Catalog
- Azure Data Lake Storage
- Lakeflow Jobs
- Production pipeline orchestration

## Learning Outcomes

After completing this project, the main concepts demonstrated are:

1. Designing a batch-based incremental pipeline.
2. Tracking processing state using `batch_id` and `batch_status`.
3. Loading new data without reprocessing all historical data.
4. Applying different loading strategies based on source-data behavior.
5. Building Bronze, Silver, and Gold Delta tables.
6. Transforming Formula 1 datasets with Spark/PySpark.
7. Organizing data using Unity Catalog.
8. Orchestrating production processing with Lakeflow Jobs.

## Source

This project README is based on the **Azure Databricks for Data Engineers – Hands-on Project using Spark, Delta Lake, Unity Catalog and Lakeflow Jobs** course material provided with this project.

The course specifically presents the Formula 1 project, its Bronze/Silver/Gold architecture, batch-based incremental processing, batch control, and incremental project setup.
