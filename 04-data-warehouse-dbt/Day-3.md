# Day 3 — dbt Tests, Documentation & Incremental Models

> **Goal: Add comprehensive tests across all model layers, generate and serve the dbt documentation site with a lineage graph, and convert fct_shipments to an incremental model so daily runs only process new records. By end of day, `dbt test` passes all tests and `dbt docs serve` shows the complete warehouse lineage.**

---

## What You Are Building Today

1. Write tests for all dimension and fact models — uniqueness, not-null, accepted values, referential integrity
2. Write a custom singular test to catch a business logic violation (delivery before dispatch)
3. Generate and navigate the dbt documentation site
4. Add model descriptions, column descriptions, and lineage annotations to YAML files
5. Convert `fct_shipments` to an incremental model — only new rows processed on each run
6. Understand when to use `table` vs `view` vs `incremental` vs `ephemeral`

---

## Step 1 — Add Tests for Dimension Models

Create `models/dimensions/dimensions.yml`:

```yaml
# models/dimensions/dimensions.yml

version: 2

models:

  - name: dim_city
    description: >
      One row per unique city observed in the shipments data.
      Enriched with region and tier data from the city_region_mapping seed.
      Cities not found in the mapping have region = 'Unknown'.

    columns:
      - name: city_key
        description: "Surrogate key — MD5 hash of city_name. Stable even if city_name is corrected."
        tests:
          - unique
          - not_null

      - name: city_name
        description: "Normalised city name — lowercase, trimmed."
        tests:
          - unique
          - not_null

      - name: region
        description: "Geographical region. 'Unknown' if city not in reference data."
        tests:
          - not_null
          - accepted_values:
              values: ['North', 'South', 'East', 'West', 'Central', 'Unknown']

      - name: city_tier
        description: "City tier (1=metro, 2=tier-2, 3=tier-3, 0=unknown)."
        tests:
          - accepted_values:
              values: [0, 1, 2, 3]

  - name: dim_date
    description: >
      Date spine from 2023-01-01 to 2026-12-31.
      One row per calendar date. All date hierarchy attributes pre-computed.

    columns:
      - name: date_key
        description: "Integer surrogate key in YYYYMMDD format. Fast to join."
        tests:
          - unique
          - not_null

      - name: full_date
        description: "Calendar date as DATE type."
        tests:
          - unique
          - not_null

      - name: quarter
        description: "Quarter number (1–4)."
        tests:
          - accepted_values:
              values: [1, 2, 3, 4]

      - name: is_weekend
        description: "1 if Saturday or Sunday, 0 otherwise."
        tests:
          - accepted_values:
              values: [0, 1]

  - name: dim_status
    description: >
      One row per valid shipment status code.
      Includes business definitions and whether the status is terminal (no further updates expected).

    columns:
      - name: status_key
        description: "Surrogate key — MD5 hash of status_code."
        tests:
          - unique
          - not_null

      - name: status_code
        description: "Status code as stored in the source system."
        tests:
          - unique
          - not_null
          - accepted_values:
              values: ['DELIVERED', 'IN_TRANSIT', 'PENDING', 'RETURNED', 'FAILED']
```

---

## Step 2 — Add Tests for the Fact Table

Create `models/facts/facts.yml`:

```yaml
# models/facts/facts.yml

version: 2

models:

  - name: fct_shipments
    description: >
      Central fact table. One row per shipment at the lowest available grain.
      All dimension lookups are pre-joined. Measures include weight, delivery days,
      and derived flags (is_delivered, is_on_time).

    columns:
      - name: shipment_id
        description: "Natural key from source system. Unique per shipment."
        tests:
          - unique
          - not_null

      - name: origin_city_key
        description: "Foreign key to dim_city."
        tests:
          - not_null
          - relationships:
              to: ref('dim_city')
              field: city_key

      - name: destination_city_key
        description: "Foreign key to dim_city."
        tests:
          - not_null
          - relationships:
              to: ref('dim_city')
              field: city_key

      - name: status_key
        description: "Foreign key to dim_status."
        tests:
          - not_null
          - relationships:
              to: ref('dim_status')
              field: status_key

      - name: weight_kg
        description: "Shipment weight in kg. Null if source was invalid or missing."

      - name: days_to_deliver
        description: "Calendar days from dispatch to delivery. Null for undelivered shipments."

      - name: is_delivered
        description: "1 if status = DELIVERED, else 0."
        tests:
          - accepted_values:
              values: [0, 1]

      - name: is_on_time
        description: "1 if delivered within 3 days, else 0. Null if not delivered."
        tests:
          - accepted_values:
              values: [0, 1]
```

**The `relationships` test is the most important test in a data warehouse.** It verifies referential integrity — every `status_key` in `fct_shipments` must exist in `dim_status`. Without this test, a new status code could appear in the facts with no matching dimension row, causing NULL joins in every downstream mart.

---

## Step 3 — Write a Custom Singular Test

Generic tests (unique, not_null, relationships) cover structural integrity. Custom tests encode business rules. Create `tests/assert_no_delivery_before_dispatch.sql`:

```sql
-- tests/assert_no_delivery_before_dispatch.sql
-- Business rule: delivery date must never be before dispatch date.
-- A shipment cannot arrive before it was sent.
-- This query returns FAILING rows. dbt test passes if this returns 0 rows.

select
    shipment_id,
    dispatch_date_key,
    delivery_date_key,
    days_to_deliver
from {{ ref('fct_shipments') }}
where
    -- Both dates present AND delivery is before dispatch
    dispatch_date_key is not null
    and delivery_date_key is not null
    and days_to_deliver < 0
```

When `dbt test` runs this, it executes the query. If any rows are returned, the test fails and dbt reports which shipment_ids have the violation.

Add more singular tests:

```sql
-- tests/assert_weight_positive.sql
-- All non-null weights must be positive.

select shipment_id, weight_kg
from {{ ref('fct_shipments') }}
where weight_kg is not null and weight_kg <= 0
```

```sql
-- tests/assert_on_time_requires_delivery.sql
-- is_on_time can only be 1 if is_delivered is also 1.
-- An undelivered shipment cannot be on-time.

select shipment_id, is_delivered, is_on_time
from {{ ref('fct_shipments') }}
where is_on_time = 1 and is_delivered = 0
```

---

## Step 4 — Run All Tests

```bash
dbt test

# Expected output structure:
# Completed with X warnings and 0 errors.
# Done. PASS=N WARN=0 ERROR=0 SKIP=0 TOTAL=N

# Run tests on a specific model only:
dbt test --select fct_shipments
dbt test --select dim_city

# Run only a specific type of test:
dbt test --select test_type:singular
dbt test --select test_type:generic
```

If a test fails, dbt outputs the compiled SQL. Copy it and run it directly in your database to see the failing rows. Fix the upstream model or the test expectation (if the test was wrong), then rerun.

---

## Step 5 — Generate dbt Documentation

dbt auto-generates a documentation website from your YAML descriptions and SQL models. No separate docs system needed.

```bash
# Generate the docs
dbt docs generate

# Serve locally (opens at http://localhost:8080)
dbt docs serve
```

**Navigate the documentation site:**

1. **Project tab** — tree view of all models, seeds, tests, and macros
2. **Database tab** — tree view by database schema
3. **Lineage graph** — click any model → click the lineage graph icon (bottom right) → see the full DAG visually

The lineage graph for `mart_daily_kpis` should show:
```
shipments (source) → stg_shipments → fct_shipments → mart_daily_kpis
                                  ↗
city_region_mapping (seed) → dim_city
status_descriptions (seed) → dim_status
                   dim_date ↗
```

Add a project-level description to `dbt_project.yml`:

```yaml
# dbt_project.yml — add at the top

name: 'swiftship_dw'
version: '1.0.0'
config-version: 2
profile: 'swiftship_dw'

# Project description — appears on the dbt docs homepage
description: >
  SwiftShip Data Warehouse — built with dbt.
  Transforms raw shipment records into analyst-ready dimensional models
  using Kimball methodology. Covers staging, dimensions, facts, and marts.
  Source: Project 02 database sink. Updated daily via Project 03 Airflow pipeline.
```

---

## Step 6 — Convert fct_shipments to an Incremental Model

Right now, every `dbt run` rebuilds `fct_shipments` from scratch — scanning all historical records. After a year of data, this full rebuild could take 20+ minutes. An incremental model only processes records newer than the last run.

Update `models/facts/fct_shipments.sql`:

```sql
-- models/facts/fct_shipments.sql  (incremental version)

{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        on_schema_change='append_new_columns'
    )
}}

-- The rest of the model is identical to Day 2 version.
-- The only addition is the is_incremental() filter below.

with stg as (
    select * from {{ ref('stg_shipments') }}

    -- INCREMENTAL FILTER: on the first run, is_incremental() = False → no filter.
    -- On subsequent runs, is_incremental() = True → only process rows
    -- that were loaded after the latest loaded_at already in the fact table.
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

-- ... (dim_city, dim_date, dim_status CTEs — identical to Day 2) ...
-- ... (final select — identical to Day 2) ...
```

**How incremental models work step by step:**

1. **First run** (`is_incremental() = False`): dbt builds the table from scratch using all records.
2. **Subsequent runs** (`is_incremental() = True`): dbt runs only the filtered query, then merges results into the existing table using `unique_key='shipment_id'` — update existing rows, insert new ones.
3. **`on_schema_change='append_new_columns'`**: if you add a column to the model, the incremental run adds it to the existing table rather than failing.

**Force a full refresh when needed:**

```bash
# Rebuild fct_shipments from scratch (ignores incremental filter)
dbt run --select fct_shipments --full-refresh
```

---

## Step 7 — Understand Materialisation Options

| Materialisation | What dbt does | When to use |
|---|---|---|
| `view` | Creates a SQL view — no data stored | Staging models queried only by dbt, not analysts |
| `table` | Drops and recreates the table on every run | Dims and marts — small enough to rebuild fast |
| `incremental` | Merges new/changed rows into existing table | Large fact tables where full rebuild is slow |
| `ephemeral` | Inlined as a CTE — never materialised in DB | Intermediate logic reused in one other model |

The right materialisation for each layer in SwiftShip:

| Layer | Materialisation | Reason |
|---|---|---|
| Staging | `view` | No storage needed; always fresh from source |
| Dimensions | `table` | Fast rebuilds (< 1000 rows); frequently queried |
| Fact table | `incremental` | Grows daily; full rebuild expensive at scale |
| Marts | `table` | Small (aggregated); analysts need consistent performance |

---

## Step 8 — Final End-to-End Run

```bash
# Complete project run with all options:
dbt seed &&           # load reference data
dbt run  &&           # build all models
dbt test &&           # run all tests
dbt docs generate &&  # generate documentation
dbt docs serve        # open docs at http://localhost:8080
```

---

## Day 3 Checklist

Before calling Project 05 complete, confirm:

- [ ] `dbt test` passes all generic tests across all 7 models
- [ ] All three singular tests (`assert_no_delivery_before_dispatch`, `assert_weight_positive`, `assert_on_time_requires_delivery`) pass or return 0 failing rows
- [ ] `dbt docs serve` opens a documentation site with model descriptions visible
- [ ] The lineage graph for `mart_daily_kpis` shows the full ancestry back to the `shipments` source
- [ ] `fct_shipments` is now an incremental model — `dbt run --select fct_shipments` a second time is faster than the first
- [ ] `dbt run --select fct_shipments --full-refresh` rebuilds from scratch without error
- [ ] All YAML description fields are populated — no model or column has an empty description
- [ ] `relationships` test on `fct_shipments.status_key` passes (all status keys exist in `dim_status`)

---

*With Project 05 complete, you have a dimensional data warehouse built with dbt — the same stack used at Airbnb, GitLab, Shopify, and thousands of other companies. Project 06 adds a data quality layer that validates data before it ever reaches this warehouse.*
