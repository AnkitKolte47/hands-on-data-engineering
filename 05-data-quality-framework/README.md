# DataQuality — A 3-Day Data Engineering Project

> **Build a production-style data quality framework from scratch — covering expectation suites, schema validation, range checks, freshness SLAs, HTML quality reports, quality score gating, and trend tracking across pipeline runs.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — Rule Engine Design, Schema & Null Validation](Day-1.md)
6. [Day 2 — Range Checks, Freshness Checks & Referential Integrity](Day-2.md)
7. [Day 3 — HTML Reports, Quality Score Gating & Trend Tracking](Day-3.md)
8. [How to Run the Full Project](#how-to-run-the-full-project)
9. [Sample Expectation Config](#sample-expectation-config)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

You are still at **SwiftShip**, and the pipeline has been running for two weeks.

The CEO opens the daily logistics dashboard and sees 47 shipments listed as delivered to the city `"nan"`. The CFO's report shows the average delivery weight is 9,843 kg — clearly a decimal formatting bug. The operations team notices that Monday's data loaded on Friday — three days early, which is impossible.

All of these bugs passed through your pipeline silently. The ETL ran, reported success, loaded the data — and nobody knew the data was wrong until a stakeholder saw it.

Your manager sits you down and says: **"We need data quality checks. Before any data reaches the database, we need to know if it is trustworthy."**

You will build a **data quality framework** that:

- **Validates schema** — every column must have the right name and type
- **Validates nullability** — required columns must never be null
- **Validates ranges** — weight must be between 0.1 and 500 kg; dates must be in the past
- **Validates accepted values** — status must be one of the known values, not free-text garbage
- **Validates freshness** — dispatch_date must be within the last 30 days
- **Validates referential integrity** — destination_city must exist in the known cities table
- **Generates an HTML quality report** on every run — viewable in any browser
- **Gates the pipeline** — if the quality score drops below 95%, the load step is blocked
- **Tracks trends** — quality scores are stored per run so you can spot degrading data sources over time

---

## Why These Concepts Matter in Data Engineering

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **Schema validation** | Upstream systems change column names or types without warning | Pipeline loads data into wrong columns silently; analysts query `None` values for months |
| **Null validation** | Required fields arrive empty due to upstream bugs | Fact tables have null foreign keys; GROUP BY on city returns a null group that confuses analysts |
| **Range checks** | Weight sensors malfunction and send 9999; date systems have timezone bugs | Averages and totals are wildly wrong; dashboards show impossible values |
| **Accepted value checks** | Free-text status fields get new undocumented values from ops teams | Unknown statuses are silently dropped or included, breaking SLA calculations |
| **Freshness checks** | Pipelines replay old data, duplicate-detect fails, data is stale | Analysts query yesterday's data thinking it is today's — wrong decisions get made |
| **Quality score gating** | A source file with 80% bad records should not reach the database | Bad data is loaded, corrupts the warehouse, and requires an emergency rollback |
| **HTML reports** | Stakeholders need a human-readable quality dashboard, not just logs | Quality issues are invisible until a dashboard breaks — reactive instead of proactive |
| **Trend tracking** | Quality can degrade slowly — 98%, 97%, 95%, 90% over two weeks | A slow degradation is invisible run-by-run; only the trend view reveals the problem |

---

## Project Architecture Overview

```
consolidated.csv  (from pipeline)
        │
        ▼
┌─────────────────────────────────────┐
│  EXPECTATION SUITE LOADER  (Day 1)  │
│  Reads expectations from YAML config│
│  Builds RuleSet: list of Rule objs  │
└──────────────┬──────────────────────┘
               │ ordered rules
               ▼
┌─────────────────────────────────────┐
│  VALIDATION ENGINE  (Day 1 + 2)     │
│  For each record, runs all rules    │
│  Collects: passed, failed, warnings │
│  SchemaCheck → NullCheck →          │
│  RangeCheck → AcceptedValues →      │
│  FreshnessCheck → ReferentialCheck  │
└──────────────┬──────────────────────┘
               │ ValidationResult per record
               ▼
┌─────────────────────────────────────┐
│  QUALITY SCORER  (Day 3)            │
│  Computes quality score (0–100)     │
│  Compares to threshold (default 95) │
│  Blocks load if score < threshold   │
└──────────────┬──────────────────────┘
               │ score + pass/fail
               ▼
┌─────────────────────────────────────┐
│  REPORTER  (Day 3)                  │
│  HTML report with per-rule stats    │
│  Trend DB: stores score per run     │
│  Trend chart: last 30 runs          │
└─────────────────────────────────────┘
```

---

## Folder Structure

```
data_quality/
│
├── config/
│   └── expectations.yaml            ← Day 1: all rules defined here
│
├── reference_data/
│   └── valid_cities.csv             ← Day 2: referential integrity lookup
│
├── quality/
│   ├── __init__.py
│   ├── rules/
│   │   ├── __init__.py
│   │   ├── base_rule.py             ← Day 1: abstract Rule class
│   │   ├── schema_rule.py           ← Day 1: column existence + type check
│   │   ├── null_rule.py             ← Day 1: required field not-null check
│   │   ├── range_rule.py            ← Day 2: numeric and date range checks
│   │   ├── accepted_values_rule.py  ← Day 2: enum / whitelist check
│   │   ├── freshness_rule.py        ← Day 2: date recency check
│   │   └── referential_rule.py      ← Day 2: foreign key existence check
│   │
│   ├── engine.py                    ← Day 1: ValidationEngine orchestrator
│   ├── expectation_loader.py        ← Day 1: loads rules from YAML config
│   ├── scorer.py                    ← Day 3: quality score calculation + gate
│   ├── reporter.py                  ← Day 3: HTML report generator
│   └── trend_tracker.py             ← Day 3: SQLite trend database
│
├── data/
│   ├── inputs/
│   │   └── consolidated.csv
│   └── outputs/
│       ├── quality_report.html      ← generated per run
│       ├── failed_records.csv       ← records that failed validation
│       └── quality_trends.db        ← SQLite DB of historical quality scores
│
└── main.py                          ← Day 3
```

---

## How to Run the Full Project

### Prerequisites

```bash
pip install pyyaml jinja2
```

No external services required — everything runs locally.

### Run

```bash
cd data_quality

# Standard quality check run
python main.py

# Run with strict threshold (fail if any record rejected)
python main.py --threshold 100

# Run with custom expectations file
python main.py --expectations config/expectations_strict.yaml

# View the HTML report
open data/outputs/quality_report.html   # Mac
xdg-open data/outputs/quality_report.html  # Linux
```

### Expected outputs

| Output | Description |
|---|---|
| `data/outputs/quality_report.html` | Human-readable quality report with per-rule pass/fail counts |
| `data/outputs/failed_records.csv` | All records that failed one or more rules, with failure reasons |
| `data/outputs/quality_trends.db` | SQLite database of quality scores per run, for trend analysis |
| Console | Quality score, pass/fail status, and gate decision |

---

## Sample Expectation Config

```yaml
# config/expectations.yaml

dataset: consolidated_shipments
version: "1.0"

rules:

  - type: schema
    name: "Required columns present"
    required_columns:
      - shipment_id
      - origin_city
      - destination_city
      - status
      - dispatch_date
    severity: critical   # critical = fail immediately if violated

  - type: not_null
    name: "shipment_id must never be null"
    column: shipment_id
    severity: critical

  - type: not_null
    name: "status must never be null"
    column: status
    severity: critical

  - type: accepted_values
    name: "status must be a known value"
    column: status
    values:
      - DELIVERED
      - IN_TRANSIT
      - PENDING
      - RETURNED
      - FAILED
    severity: error

  - type: range
    name: "weight_kg must be between 0.1 and 500"
    column: weight_kg
    min: 0.1
    max: 500.0
    allow_null: true   # null weight is allowed (pending shipments may lack weight)
    severity: error

  - type: freshness
    name: "dispatch_date must be within last 60 days"
    column: dispatch_date
    max_age_days: 60
    severity: warning  # warning = counted but does not affect quality score

  - type: referential
    name: "origin_city must be in known cities list"
    column: origin_city
    reference_file: reference_data/valid_cities.csv
    reference_column: city_name
    severity: warning
```

---

## Interview Cheat Sheet

**"What is a data quality framework and why does it matter more than pipeline monitoring?"**
> Pipeline monitoring tells you the pipeline ran — it measures process health. A data quality framework tells you whether the data produced by the pipeline is correct — it measures data health. A pipeline can run successfully and still produce wrong data. Quality frameworks catch the silent failures that monitoring misses.

**"What is an expectation suite?"**
> An expectation suite is a collection of assertions about data — rules that describe what the data should look like when it is correct. Each expectation describes one condition: "shipment_id must never be null", "weight_kg must be between 0.1 and 500". A dataset passes the suite when all expectations pass. The term comes from Great Expectations, the most widely used open-source data quality library.

**"What is quality score gating and when would you lower the threshold?"**
> Quality score gating blocks the load step if the percentage of records passing all critical rules falls below a threshold — I use 95% by default. You would lower the threshold if a source system is known to send noisy data during a transition period, and the business has accepted the temporary degradation. Lowering it should be a deliberate, documented decision — not a lazy workaround.

**"How do you handle a situation where quality degrades slowly over two weeks?"**
> This is why trend tracking exists. Run-by-run, a drop from 98% to 97% looks like noise. But stored in a trend database and graphed over 30 runs, a consistent downward trend is immediately visible. I store the quality score, total records, failed record count, and timestamp for every run. A simple threshold alert on the trend slope (quality decreasing by more than 0.5% per day) would catch slow degradation before it becomes a crisis.

**"What is the difference between a critical rule and a warning rule?"**
> A critical rule violation means the data is fundamentally unusable — a missing required column, a null primary key. These violations contribute to the quality score and can trigger the gate. A warning rule flags a potential issue that may or may not indicate a problem — a dispatch date that is 45 days old might be a legitimate historical correction, or it might be a bug. Warnings are recorded in the report for human review but do not block the pipeline.

---

*Data quality is the most under-appreciated skill in data engineering. Every company has a horror story about bad data in a production dashboard. Engineers who can build quality frameworks are rare and extremely valuable.*
