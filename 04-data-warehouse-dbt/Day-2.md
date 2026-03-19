# Day 2 — Dimensional Modeling: Facts, Dims & Marts

> **Goal: Build the dimension tables (dim_city, dim_date, dim_status), the central fact table (fct_shipments), and two mart models for dashboards. By end of day, an analyst can write `SELECT * FROM mart_daily_kpis` and get a complete, pre-calculated daily delivery performance summary.**

---

## What You Are Building Today

1. Load two seed CSVs — city-to-region mapping and status descriptions
2. Build `dim_city`, `dim_date`, `dim_status` — the three dimension tables
3. Build `fct_shipments` — one row per shipment, with surrogate keys to all dimensions
4. Build `mart_daily_kpis` — pre-aggregated daily KPIs ready for Tableau or Power BI
5. Build `mart_regional_performance` — delivery performance by region

---

## Step 1 — Create Seed Files

Seeds are CSV files in the `seeds/` directory that dbt loads into the database as tables. They are for small, static reference data — lookup tables that change infrequently.

**`seeds/city_region_mapping.csv`**

```csv
city_name,region,region_code,tier
mumbai,West,WE,1
pune,West,WE,2
ahmedabad,West,WE,2
surat,West,WE,3
delhi,North,NO,1
jaipur,North,NO,2
lucknow,North,NO,2
bangalore,South,SO,1
chennai,South,SO,1
hyderabad,South,SO,1
kolkata,East,EA,1
bhubaneswar,East,EA,2
nagpur,Central,CE,2
bhopal,Central,CE,2
indore,Central,CE,2
```

**`seeds/status_descriptions.csv`**

```csv
status_code,display_name,is_terminal,sla_breached,description
DELIVERED,Delivered,true,false,Shipment successfully delivered to recipient
IN_TRANSIT,In Transit,false,false,Shipment is currently moving between facilities
PENDING,Pending,false,false,Shipment accepted but not yet dispatched
RETURNED,Returned,true,false,Shipment returned to sender at recipient request
FAILED,Failed - Lost,true,true,Shipment lost or permanently undeliverable
```

Load seeds into the database:

```bash
dbt seed
# Expected:
# 1 of 2 START seed file analytics.city_region_mapping ........ [RUN]
# 1 of 2 OK loaded seed file analytics.city_region_mapping .... [INSERT 15 in 0.12s]
# 2 of 2 START seed file analytics.status_descriptions ........ [RUN]
# 2 of 2 OK loaded seed file analytics.status_descriptions .... [INSERT 5 in 0.08s]
```

---

## Step 2 — Build dim_status

Start with the simplest dimension — the one with the fewest rows and no joins.

Create `models/dimensions/dim_status.sql`:

```sql
-- models/dimensions/dim_status.sql
-- Dimension table for shipment status values.
-- Grain: one row per valid status code.

with status_seed as (
    select * from {{ ref('status_descriptions') }}
)

select
    -- Surrogate key: a hash of the natural key.
    -- Use dbt_utils.generate_surrogate_key macro (see Step 3 for macro).
    {{ dbt_utils.generate_surrogate_key(['status_code']) }} as status_key,

    status_code,
    display_name,
    cast(is_terminal as integer)    as is_terminal,
    cast(sla_breached as integer)   as is_sla_breached,
    description
from status_seed
```

---

## Step 3 — Install dbt-utils and Write the Surrogate Key Macro

dbt-utils is the standard utility package for dbt. Add it to `packages.yml`:

```yaml
# packages.yml  (create at project root)

packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Install it:

```bash
dbt deps
```

Now `{{ dbt_utils.generate_surrogate_key([...]) }}` is available in all models. It generates an MD5 hash of the input columns — a stable surrogate key that does not depend on source system IDs.

---

## Step 4 — Build dim_city

Create `models/dimensions/dim_city.sql`:

```sql
-- models/dimensions/dim_city.sql
-- Dimension table for cities.
-- Grain: one row per unique city name seen in the shipments data.
-- Enriched with region data from the seed.

with cities_from_shipments as (
    -- Get all unique cities (both origin and destination)
    select distinct origin_city as city_name
    from {{ ref('stg_shipments') }}
    where origin_city is not null

    union

    select distinct destination_city as city_name
    from {{ ref('stg_shipments') }}
    where destination_city is not null
),

enriched as (
    select
        c.city_name,
        coalesce(m.region,      'Unknown') as region,
        coalesce(m.region_code, 'UNK')     as region_code,
        coalesce(m.tier,        0)         as city_tier
    from cities_from_shipments c
    left join {{ ref('city_region_mapping') }} m
        on c.city_name = lower(trim(m.city_name))
)

select
    {{ dbt_utils.generate_surrogate_key(['city_name']) }} as city_key,
    city_name,
    initcap(city_name) as city_display_name,   -- "mumbai" → "Mumbai"
    region,
    region_code,
    city_tier
from enriched
```

**Why UNION (not UNION ALL) for cities?** A city may appear as both an origin and a destination. `UNION` deduplicates — `UNION ALL` would produce two rows for `mumbai` when it appears in both columns.

**Why LEFT JOIN to the seed?** Not every city in the data will be in the seed. `LEFT JOIN` ensures unknown cities still get a row in `dim_city`, with "Unknown" region, rather than being silently dropped.

---

## Step 5 — Build dim_date

The date dimension is one of the most important tables in any data warehouse. It enables filtering by week, month, quarter, financial year, and day-of-week without any date arithmetic in the dashboard tool.

Create `models/dimensions/dim_date.sql`:

```sql
-- models/dimensions/dim_date.sql
-- Date dimension covering the full range of dispatch dates in the data.
-- Grain: one row per calendar date.

with date_spine as (
    -- Generate one row per date between the earliest and latest dispatch date.
    -- dbt_utils.date_spine generates a sequence of dates.
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

final as (
    select
        -- Surrogate key: integer in YYYYMMDD format — fast to join, human-readable
        cast(strftime('%Y%m%d', date_day) as integer) as date_key,
        date_day                                       as full_date,

        -- Year
        cast(strftime('%Y', date_day) as integer)     as year,

        -- Quarter
        case cast(strftime('%m', date_day) as integer)
            when 1 then 1 when 2 then 1 when 3 then 1
            when 4 then 2 when 5 then 2 when 6 then 2
            when 7 then 3 when 8 then 3 when 9 then 3
            else 4
        end                                           as quarter,
        'Q' || case cast(strftime('%m', date_day) as integer)
            when 1 then '1' when 2 then '1' when 3 then '1'
            when 4 then '2' when 5 then '2' when 6 then '2'
            when 7 then '3' when 8 then '3' when 9 then '3'
            else '4'
        end                                           as quarter_name,

        -- Month
        cast(strftime('%m', date_day) as integer)     as month,
        strftime('%B', date_day)                      as month_name,
        strftime('%b', date_day)                      as month_short,

        -- Week
        cast(strftime('%W', date_day) as integer)     as week_of_year,

        -- Day
        cast(strftime('%d', date_day) as integer)     as day_of_month,
        cast(strftime('%w', date_day) as integer)     as day_of_week,
        case cast(strftime('%w', date_day) as integer)
            when 0 then 'Sunday'    when 1 then 'Monday'
            when 2 then 'Tuesday'   when 3 then 'Wednesday'
            when 4 then 'Thursday'  when 5 then 'Friday'
            else 'Saturday'
        end                                           as day_name,

        -- Flags
        case cast(strftime('%w', date_day) as integer)
            when 0 then 1 when 6 then 1 else 0
        end                                           as is_weekend

    from date_spine
)

select * from final
```

---

## Step 6 — Build fct_shipments

The fact table is the core of the data warehouse. It stores one row per shipment with surrogate keys to all dimension tables.

Create `models/facts/fct_shipments.sql`:

```sql
-- models/facts/fct_shipments.sql
-- Central fact table for shipments.
-- Grain: one row per shipment (lowest grain available).
-- All dimension lookups are pre-joined via surrogate keys.

with stg as (
    select * from {{ ref('stg_shipments') }}
),

dim_city as (
    select * from {{ ref('dim_city') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_status as (
    select * from {{ ref('dim_status') }}
),

final as (
    select
        -- Natural key (preserved for lineage / debugging)
        s.shipment_id,

        -- Dimension foreign keys (surrogate keys)
        o.city_key                                    as origin_city_key,
        d.city_key                                    as destination_city_key,
        ds.date_key                                   as dispatch_date_key,
        dd.date_key                                   as delivery_date_key,
        st.status_key                                 as status_key,

        -- Measures (the actual facts)
        s.weight_kg,
        s.days_to_deliver,

        -- Derived measures
        s.is_delivered,
        case
            when s.is_delivered = 1 and s.days_to_deliver <= 3 then 1
            else 0
        end                                           as is_on_time,

        -- Degenerate dimensions (low cardinality, kept on fact)
        s.source_file,
        s.loaded_at

    from stg s

    -- Join dimension tables using surrogate keys derived from natural keys
    left join dim_city  o  on o.city_name  = s.origin_city
    left join dim_city  d  on d.city_name  = s.destination_city
    left join dim_date  ds on ds.date_key  = cast(replace(s.dispatch_date, '-', '') as integer)
    left join dim_date  dd on dd.date_key  = cast(replace(s.delivery_date, '-', '') as integer)
    left join dim_status st on st.status_code = s.status
)

select * from final
```

---

## Step 7 — Build the Mart Models

Marts are pre-aggregated views for specific business use cases. They are the layer analysts and BI tools query.

**`models/marts/mart_daily_kpis.sql`**

```sql
-- models/marts/mart_daily_kpis.sql
-- Daily shipment KPI summary.
-- Consumer: operations dashboard, C-suite daily briefing.
-- Grain: one row per dispatch date.

with facts as (
    select * from {{ ref('fct_shipments') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily as (
    select
        f.dispatch_date_key,
        d.full_date                                   as dispatch_date,
        d.year,
        d.quarter_name,
        d.month_name,
        d.week_of_year,
        d.is_weekend,

        count(f.shipment_id)                          as total_shipments,
        sum(f.is_delivered)                           as delivered_count,
        sum(f.is_on_time)                             as on_time_count,
        sum(case when f.is_delivered = 0 then 1 end)  as pending_or_transit,
        round(avg(f.weight_kg), 2)                    as avg_weight_kg,
        round(sum(f.weight_kg), 2)                    as total_weight_kg,
        round(avg(f.days_to_deliver), 1)              as avg_days_to_deliver,

        -- KPI ratios (calculated once here, not in every dashboard)
        round(
            100.0 * sum(f.is_delivered) / nullif(count(f.shipment_id), 0),
            2
        )                                             as delivery_rate_pct,
        round(
            100.0 * sum(f.is_on_time) / nullif(sum(f.is_delivered), 0),
            2
        )                                             as on_time_rate_pct

    from facts f
    left join dates d on d.date_key = f.dispatch_date_key
    where f.dispatch_date_key is not null
    group by
        f.dispatch_date_key, d.full_date, d.year,
        d.quarter_name, d.month_name, d.week_of_year, d.is_weekend
)

select * from daily
order by dispatch_date desc
```

**`models/marts/mart_regional_performance.sql`**

```sql
-- models/marts/mart_regional_performance.sql
-- Regional delivery performance summary.
-- Consumer: regional manager dashboard, route optimisation analysis.
-- Grain: one row per (origin_region, destination_region, status).

with facts as (
    select * from {{ ref('fct_shipments') }}
),

origin_cities as (
    select city_key, city_name, region as origin_region
    from {{ ref('dim_city') }}
),

dest_cities as (
    select city_key, city_name, region as destination_region
    from {{ ref('dim_city') }}
),

statuses as (
    select status_key, status_code, display_name as status_display
    from {{ ref('dim_status') }}
),

final as (
    select
        o.origin_region,
        d.destination_region,
        s.status_display,

        count(f.shipment_id)            as shipment_count,
        round(avg(f.weight_kg), 2)      as avg_weight_kg,
        round(avg(f.days_to_deliver), 1) as avg_days_to_deliver,
        sum(f.is_on_time)               as on_time_deliveries,
        round(
            100.0 * sum(f.is_on_time) / nullif(sum(f.is_delivered), 0),
            2
        )                               as on_time_rate_pct

    from facts f
    left join origin_cities o on o.city_key = f.origin_city_key
    left join dest_cities   d on d.city_key = f.destination_city_key
    left join statuses      s on s.status_key = f.status_key
    group by o.origin_region, d.destination_region, s.status_display
)

select * from final
order by shipment_count desc
```

---

## Step 8 — Run the Full Model Graph

```bash
# Run everything in dependency order (dbt figures out the order automatically)
dbt run

# Expected output (in order, based on DAG):
# city_region_mapping  (seed)
# status_descriptions  (seed)
# stg_shipments
# dim_city      ← depends on stg_shipments + city_region_mapping
# dim_date      ← no dependencies
# dim_status    ← depends on status_descriptions
# fct_shipments ← depends on stg_shipments + dim_city + dim_date + dim_status
# mart_daily_kpis           ← depends on fct_shipments + dim_date
# mart_regional_performance ← depends on fct_shipments + dim_city + dim_status
```

Verify in SQLite:

```bash
sqlite3 /path/to/swiftship.db ".tables"
sqlite3 /path/to/swiftship.db "SELECT * FROM mart_daily_kpis LIMIT 5;"
sqlite3 /path/to/swiftship.db "
  SELECT origin_region, destination_region, shipment_count, on_time_rate_pct
  FROM mart_regional_performance
  ORDER BY shipment_count DESC
  LIMIT 10;
"
```

---

## Day 2 Checklist

Before moving to Day 3, confirm:

- [ ] `dbt seed` loads both CSV seeds successfully
- [ ] `dbt run` completes all 9 models with no errors (2 seeds + 7 SQL models)
- [ ] `dim_city` has one row per unique city, with `region = 'Unknown'` for unmapped cities
- [ ] `dim_date` has rows from 2023-01-01 to 2026-12-31 (verify: `SELECT COUNT(*) FROM dim_date`)
- [ ] `fct_shipments` row count matches `stg_shipments` row count
- [ ] `mart_daily_kpis` has `delivery_rate_pct` and `on_time_rate_pct` as numeric values (not null)
- [ ] `mart_regional_performance` has rows for every region combination present in the data
- [ ] Running `dbt run` twice produces identical results (idempotent)

---

*Day 3 adds column-level tests across all models, auto-generated documentation with the lineage graph, and incremental materialisation for the fact table.*
