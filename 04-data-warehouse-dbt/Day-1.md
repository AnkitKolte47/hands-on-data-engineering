# Day 1 — dbt Setup, Sources & Staging Models

> **Goal: Install dbt, configure it against the SwiftShip database, define your raw source tables, and build the staging layer that cleans and standardises raw shipment data. By end of day, `dbt run --select staging` completes successfully and `dbt test --select staging` passes all tests.**

---

## What You Are Building Today

1. Install dbt and configure a connection to the SwiftShip database (SQLite or PostgreSQL)
2. Understand dbt's project structure and how models, sources, and tests relate
3. Define the raw `shipments` table as a dbt source
4. Build `stg_shipments` — the staging model that cleans, renames, and casts the raw data
5. Write column-level tests on the staging model
6. Run `dbt run` and `dbt test` for the first time and interpret the output

---

## Step 1 — Install and Initialise dbt

```bash
pip install dbt-core dbt-sqlite   # for SQLite (from Project 02)
# OR
pip install dbt-core dbt-postgres  # for PostgreSQL

# Initialise a new dbt project
dbt init dbt_warehouse
cd dbt_warehouse
```

dbt creates this structure:

```
dbt_warehouse/
├── dbt_project.yml     ← project name, model materialisations
├── models/             ← your SQL transformation files
├── seeds/              ← CSV files loaded into the DB as tables
├── tests/              ← custom SQL tests
├── macros/             ← reusable Jinja2 SQL functions
└── analyses/           ← ad-hoc queries (not built by dbt run)
```

---

## Step 2 — Configure profiles.yml

dbt uses `profiles.yml` to know which database to connect to. This file lives in `~/.dbt/` — never inside the project directory (it contains credentials).

```yaml
# ~/.dbt/profiles.yml

swiftship_dw:
  target: dev
  outputs:
    dev:
      type: sqlite
      threads: 1
      database: /absolute/path/to/swiftship.db
      schema: main
      schemas_and_paths:
        main: /absolute/path/to/swiftship.db

    prod:
      type: postgres
      host: localhost
      port: 5432
      dbname: swiftship
      user: postgres
      password: swiftship
      schema: analytics   # dbt creates models in this schema
      threads: 4
```

Verify the connection:

```bash
dbt debug
# Expected output: All checks passed!
```

---

## Step 3 — Configure dbt_project.yml

```yaml
# dbt_project.yml

name: 'swiftship_dw'
version: '1.0.0'
config-version: 2

profile: 'swiftship_dw'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]

# Default materialisations by layer
models:
  swiftship_dw:
    staging:
      +materialized: view        # staging models are views — no storage cost
      +schema: staging           # models go into the 'staging' schema
    dimensions:
      +materialized: table       # dims are tables — queried frequently
      +schema: analytics
    facts:
      +materialized: table
      +schema: analytics
    marts:
      +materialized: table
      +schema: analytics
```

**Why different materialisations per layer?**
- `view` for staging: no data is stored, the SQL runs on each query. Staging models are only queried by downstream dbt models, not by analysts, so performance is not critical.
- `table` for dims/facts/marts: data is stored. Analyst queries hit a pre-computed table — fast and consistent.

---

## Step 4 — Define Sources

A dbt **source** declares a raw table that exists in the database but was not created by dbt. Declaring it as a source lets you reference it with `{{ source('name', 'table') }}` instead of hardcoded table names, and enables source freshness checks.

Create `models/staging/_sources.yml`:

```yaml
# models/staging/_sources.yml

version: 2

sources:
  - name: swiftship_raw
    description: "Raw shipment data loaded by the Project 02 database sink."
    database: main        # SQLite: always 'main'. PostgreSQL: your dbname.
    schema: main          # SQLite: always 'main'. PostgreSQL: your schema.

    tables:
      - name: shipments
        description: "One row per shipment, loaded daily from 5 regional offices."
        loaded_at_field: loaded_at   # used for source freshness checks
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}

        columns:
          - name: shipment_id
            description: "Unique identifier for the shipment. Natural key from source systems."
            tests:
              - unique
              - not_null

          - name: status
            description: "Current status of the shipment."
            tests:
              - not_null
              - accepted_values:
                  values: ['DELIVERED', 'IN_TRANSIT', 'PENDING', 'RETURNED', 'FAILED']

          - name: weight_kg
            description: "Shipment weight in kilograms."

          - name: loaded_at
            description: "Timestamp when this row was loaded by the pipeline."
            tests:
              - not_null
```

Check source freshness:

```bash
dbt source freshness
```

---

## Step 5 — Build the Staging Model

Create `models/staging/stg_shipments.sql`. This model:

1. Selects from the raw `shipments` source (not the table name directly)
2. Casts all columns to the correct types
3. Standardises city names (lowercase, trimmed)
4. Renames columns to match the warehouse naming convention
5. Adds derived columns (e.g., `is_delivered`, `days_to_deliver`)

```sql
-- models/staging/stg_shipments.sql

with source as (
    -- Reference the source, not the raw table name.
    -- If the raw table is renamed, only this line changes.
    select * from {{ source('swiftship_raw', 'shipments') }}
),

renamed as (
    select
        -- Primary key
        shipment_id,

        -- Standardise city names: trim whitespace, lowercase
        trim(lower(origin_city))      as origin_city,
        trim(lower(destination_city)) as destination_city,

        -- Cast dates from TEXT to DATE
        -- SQLite stores dates as TEXT; PostgreSQL has native DATE type
        case
            when dispatch_date is not null and dispatch_date != ''
            then date(dispatch_date)
            else null
        end as dispatch_date,

        case
            when delivery_date is not null and delivery_date != ''
            then date(delivery_date)
            else null
        end as delivery_date,

        -- Standardise status: uppercase, trimmed
        upper(trim(status)) as status,

        -- Coerce weight: null if unparseable or out of range
        case
            when weight_kg > 0 and weight_kg <= 1000
            then cast(weight_kg as real)
            else null
        end as weight_kg,

        -- Source tracking
        source_file,
        loaded_at,

        -- Derived columns — computed once here, available everywhere downstream
        case
            when upper(trim(status)) = 'DELIVERED' then 1
            else 0
        end as is_delivered,

        case
            when delivery_date is not null and delivery_date != ''
             and dispatch_date is not null and dispatch_date != ''
            then cast(
                julianday(delivery_date) - julianday(dispatch_date)
                as integer
            )
            else null
        end as days_to_deliver

    from source
    where
        -- Exclude clearly invalid rows at the staging layer
        shipment_id is not null
        and shipment_id != ''
        and status is not null
        and status != ''
)

select * from renamed
```

**Why compute `is_delivered` and `days_to_deliver` in staging?**

Because 12 downstream models will all need these values. Computing them once in staging ensures the calculation is identical everywhere. If the business changes the definition of "delivered", you change one line in `stg_shipments.sql` — not 12 models.

---

## Step 6 — Write Staging Tests

Create `models/staging/stg_shipments.yml`:

```yaml
# models/staging/stg_shipments.yml

version: 2

models:
  - name: stg_shipments
    description: >
      Cleaned and standardised shipment records from the raw source.
      One row per shipment. City names are lowercase. Dates are cast to DATE.
      Invalid rows (null shipment_id, null status) are excluded.

    columns:
      - name: shipment_id
        description: "Unique shipment identifier. Natural key."
        tests:
          - unique
          - not_null

      - name: origin_city
        description: "Normalised origin city name (lowercase, trimmed)."
        tests:
          - not_null

      - name: destination_city
        description: "Normalised destination city name (lowercase, trimmed)."
        tests:
          - not_null

      - name: status
        description: "Shipment status. Always uppercase."
        tests:
          - not_null
          - accepted_values:
              values: ['DELIVERED', 'IN_TRANSIT', 'PENDING', 'RETURNED', 'FAILED']

      - name: is_delivered
        description: "1 if status = DELIVERED, 0 otherwise."
        tests:
          - accepted_values:
              values: [0, 1]

      - name: days_to_deliver
        description: >
          Days between dispatch and delivery. Null if either date is missing.
          Negative values indicate a data error (delivery before dispatch).

      - name: weight_kg
        description: "Weight in kilograms. Null if original value was invalid or out of range."
```

---

## Step 7 — Run and Test

```bash
# Run only the staging models
dbt run --select staging

# Expected output:
# 1 of 1 START sql view model staging.stg_shipments ............ [RUN]
# 1 of 1 OK created sql view model staging.stg_shipments ....... [OK in 0.23s]

# Run tests on staging
dbt test --select staging

# Expected output:
# 1 of 5 START test not_null_stg_shipments_shipment_id ......... [RUN]
# 1 of 5 PASS not_null_stg_shipments_shipment_id ............... [PASS in 0.09s]
# ... (all 5 tests pass)

# Query the staging model directly (SQLite)
sqlite3 /path/to/swiftship.db "SELECT * FROM stg_shipments LIMIT 5;"
# For PostgreSQL:
psql -U postgres -d swiftship -c "SELECT * FROM analytics.stg_shipments LIMIT 5;"
```

If any test fails, read the error message carefully. It shows the failing query — run that query directly in your database to see which rows are causing the failure.

---

## Day 1 Checklist

Before moving to Day 2, confirm:

- [ ] `dbt debug` reports: All checks passed!
- [ ] `dbt run --select staging` creates `stg_shipments` as a view with no errors
- [ ] `dbt test --select staging` passes all 5 column-level tests
- [ ] Querying `stg_shipments` directly returns clean, lowercase city names
- [ ] `is_delivered` is 0 or 1 for every row — no nulls, no other values
- [ ] `days_to_deliver` is positive for DELIVERED rows that have both dates
- [ ] `dbt source freshness` runs without error (even if data is old — that is expected in dev)
- [ ] No raw table names appear anywhere in `stg_shipments.sql` except inside `{{ source(...) }}`

---

*Day 2 builds on the clean staging layer to create dimension and fact tables using Kimball dimensional modeling. By end of Day 2, analysts can join `fct_shipments` with `dim_date` to answer "how many shipments were delivered each week in Q1?"*
