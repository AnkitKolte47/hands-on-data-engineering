# DataWarehouse — A 3-Day Data Engineering Project

> **Build a production-style analytics data warehouse using dbt — covering source definitions, staging models, dimensional modeling (Kimball), fact and dimension tables, dbt tests, and auto-generated documentation.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — dbt Setup, Sources & Staging Models](Day-1.md)
6. [Day 2 — Dimensional Modeling: Facts, Dims & Marts](Day-2.md)
7. [Day 3 — dbt Tests, Documentation & Incremental Models](Day-3.md)
8. [How to Run the Full Project](#how-to-run-the-full-project)
9. [Sample profiles.yml](#sample-profilesyml)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

You are still at **SwiftShip**, and the database from Project 02 is populated with raw shipment records.

The analytics team wants to build dashboards. They open the `shipments` table and find:

- City names in inconsistent case (`Mumbai`, `mumbai`, `MUMBAI`)
- No pre-calculated KPIs — every query must re-aggregate from scratch
- No date dimension — impossible to filter by week, quarter, or fiscal period
- No concept of "region" — origin_city and destination_city are unstructured strings
- No documentation — nobody knows what `status = 'RETURNED'` means in business terms

Your job: build a **data warehouse** using **dbt** (data build tool) that transforms the raw `shipments` table into analyst-ready models following the Kimball dimensional modeling methodology.

You will build:

- **Staging layer** — cleans and standardises raw data, one model per source
- **Dimension tables** — `dim_city`, `dim_date`, `dim_status` — descriptive context for facts
- **Fact table** — `fct_shipments` — one row per shipment, with foreign keys to dimensions
- **Mart layer** — `mart_daily_kpis`, `mart_regional_performance` — pre-aggregated for dashboards
- **dbt tests** — uniqueness, not-null, accepted values, referential integrity
- **dbt docs** — auto-generated documentation with data lineage graph

By the end of Day 3, an analyst can open the `mart_daily_kpis` model and get a time-series of delivery performance KPIs without writing a single line of transformation SQL.

---

## Why These Concepts Matter in Data Engineering

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **Staging models** | Standardise raw data from one source before anything depends on it | 12 downstream models all implement their own version of city name normalisation — inconsistent and unmaintainable |
| **Dimensional modeling** | Separate facts (measurements) from dimensions (context) for fast, intuitive querying | Analysts write enormous flat queries; joins are repeated everywhere; performance degrades on large tables |
| **Fact tables** | Store measurable events at the lowest grain — one row per shipment | Aggregations happen at query time from raw data — slow, inconsistent, hard to audit |
| **Dimension tables** | Store descriptive attributes — city metadata, date hierarchies, status definitions | Every report re-derives "which region is Mumbai in?" — inconsistent answers |
| **dbt tests** | Catch data quality issues at build time, before analysts see broken dashboards | Silent data quality issues surface in board-level reports — trust in data is destroyed |
| **dbt docs** | Auto-generate a documentation site with lineage graph from YAML descriptions | Nobody knows what a model does or where its data comes from — onboarding a new analyst takes weeks |
| **Incremental models** | Only process new/changed records on each run instead of reprocessing everything | Full table rebuilds take 4 hours on a 5-year history — daily runs miss their SLA window |

---

## Project Architecture Overview

```
shipments table (raw, from Project 02)
        │
        ▼
┌─────────────────────────────────────┐
│  STAGING LAYER  (Day 1)             │
│  stg_shipments                      │
│  Casts types, standardises names    │
│  One model per source table         │
└──────────────┬──────────────────────┘
               │ clean, typed records
               ▼
┌─────────────────────────────────────┐
│  DIMENSION LAYER  (Day 2)           │
│  dim_city     — city + region       │
│  dim_date     — date hierarchy      │
│  dim_status   — status + meaning    │
└──────────────┬──────────────────────┘
               │ surrogate keys
               ▼
┌─────────────────────────────────────┐
│  FACT LAYER  (Day 2)                │
│  fct_shipments                      │
│  One row per shipment               │
│  Foreign keys to all dimensions     │
└──────────────┬──────────────────────┘
               │ facts + dim keys
               ▼
┌─────────────────────────────────────┐
│  MART LAYER  (Day 2 + 3)            │
│  mart_daily_kpis                    │
│  mart_regional_performance          │
│  mart_on_time_delivery              │
└─────────────────────────────────────┘
```

---

## Folder Structure

```
dbt_warehouse/
│
├── dbt_project.yml                      ← Day 1: project config
├── profiles.yml                         ← Day 1: DB connection (not committed)
│
├── models/
│   ├── staging/
│   │   ├── _sources.yml                 ← Day 1: source definitions
│   │   ├── stg_shipments.sql            ← Day 1
│   │   └── stg_shipments.yml            ← Day 3: column-level tests + docs
│   │
│   ├── dimensions/
│   │   ├── dim_city.sql                 ← Day 2
│   │   ├── dim_date.sql                 ← Day 2
│   │   ├── dim_status.sql               ← Day 2
│   │   └── dimensions.yml               ← Day 3: tests + docs
│   │
│   ├── facts/
│   │   ├── fct_shipments.sql            ← Day 2
│   │   └── facts.yml                    ← Day 3: tests + docs
│   │
│   └── marts/
│       ├── mart_daily_kpis.sql          ← Day 2
│       ├── mart_regional_performance.sql ← Day 2
│       └── marts.yml                    ← Day 3: tests + docs
│
├── seeds/
│   ├── city_region_mapping.csv          ← Day 2: city → region lookup
│   └── status_descriptions.csv         ← Day 2: status → business meaning
│
├── macros/
│   └── generate_surrogate_key.sql       ← Day 2: reusable key generation
│
└── analyses/
    └── data_quality_check.sql           ← Day 3: ad-hoc analysis queries
```

---

## How to Run the Full Project

### Prerequisites

```bash
pip install dbt-core dbt-sqlite
# For PostgreSQL (from Project 02):
pip install dbt-postgres
```

### Run

```bash
cd dbt_warehouse

# Install dbt packages
dbt deps

# Seed lookup tables
dbt seed

# Run all models
dbt run

# Run all tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve  # opens browser at http://localhost:8080
```

### Selective runs

```bash
dbt run --select staging          # only staging models
dbt run --select +fct_shipments   # fct_shipments and all its upstream models
dbt run --select marts.*          # all mart models
dbt test --select fct_shipments   # only test fct_shipments
```

---

## Sample profiles.yml

Store this in `~/.dbt/profiles.yml` — never commit this file (it contains credentials).

```yaml
# ~/.dbt/profiles.yml

swiftship_dw:
  target: dev
  outputs:
    dev:
      type: sqlite
      threads: 1
      database: /path/to/swiftship.db
      schema: main
      schemas_and_paths:
        main: /path/to/swiftship.db

    prod:
      type: postgres
      host: localhost
      port: 5432
      dbname: swiftship
      user: postgres
      password: swiftship
      schema: analytics
      threads: 4
```

---

## Interview Cheat Sheet

**"What is dbt and what problem does it solve?"**
> dbt (data build tool) is a transformation framework that lets you write transformations as SELECT statements in SQL, then handles compilation, dependency management, testing, and documentation. It solves the problem of scattered, untested, undocumented SQL scripts that each analyst maintains independently. With dbt, transformations are version-controlled, tested, and documented like software.

**"What is the difference between a staging model and a mart model?"**
> Staging models sit directly on top of raw source tables. They do one thing: clean, rename, and cast data from one source. They should not join multiple sources and should not aggregate. Mart models are the top of the stack — they are purpose-built for a specific consumer (a dashboard, a report, a machine learning feature store). Marts often join multiple facts and dimensions and may include pre-calculated aggregations.

**"What is Kimball dimensional modeling?"**
> Kimball dimensional modeling is a data warehouse design methodology that organises data into fact tables and dimension tables. Fact tables store measurable events at the lowest grain (one row per shipment). Dimension tables store descriptive context (city names, date hierarchies, status meanings). This structure makes data intuitive for analysts and fast for databases because aggregations happen on narrow fact tables, not wide flat tables.

**"What is a surrogate key and why do you use it?"**
> A surrogate key is a synthetic unique identifier generated by the warehouse, typically a hash of the natural key columns. I use surrogate keys in dimension tables because natural keys (shipment_id, city name) can change in source systems — a surrogate key remains stable even if the source changes the natural key. They also provide a consistent join key type (always a hash, never a variable-length string from a legacy system).

**"How do dbt tests work?"**
> dbt ships with four built-in generic tests: `unique`, `not_null`, `accepted_values`, and `relationships`. You declare them in a `.yml` file alongside your model. When you run `dbt test`, dbt compiles each test into a SQL query that returns failing rows. If the query returns 0 rows, the test passes. If it returns any rows, the test fails and dbt reports which rows violated the constraint. You can also write custom tests as SQL files in the `tests/` directory.

**"What is an incremental model in dbt?"**
> An incremental model only processes new or changed records on each run, rather than rebuilding the entire table. You add `{{ config(materialized='incremental') }}` to the model and a `{% if is_incremental() %}` filter that limits the query to records newer than the latest record already in the table. On the first run, the full table is built. On subsequent runs, only new rows are appended. This is critical for fact tables with years of history where a full rebuild would take hours.

---

*dbt is now a standard tool in every modern data stack. Mastering it — especially the modeling methodology behind it — is what makes the difference between a DE who "writes SQL" and one who builds a data platform.*
