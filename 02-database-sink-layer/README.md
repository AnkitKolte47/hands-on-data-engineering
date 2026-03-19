# DatabaseSink — A 3-Day Data Engineering Project

> **Build a production-style database loading layer in Python, covering the Strategy Pattern, upsert logic, bulk loading, connection pooling, and schema migration — all wired to a real relational database.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — Strategy Pattern, Connection Management & Schema Design](Day-1.md)
6. [Day 2 — Upsert Logic, Bulk Loading & Benchmarking](Day-2.md)
7. [Day 3 — Schema Migrations, Connection Pooling & Pipeline Integration](Day-3.md)
8. [How to Run the Full Project](#how-to-run-the-full-project)
9. [Sample Configuration](#sample-configuration)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

You are still a data engineer at **SwiftShip**.

The ETL pipeline from Project 01 produces a `consolidated.csv` file every night. The analytics team wants to query shipment data using SQL — but a CSV file is not a database. Your manager asks you to build a **database sink layer** that loads the pipeline output into both **SQLite** (for local dev) and **PostgreSQL** (for production), using a single, swappable interface.

Your requirements:

- **One interface, two databases** — the pipeline should not care whether it is writing to SQLite or PostgreSQL. Swapping databases must require changing one line of config, not rewriting the loader.
- **Idempotent loads** — running the pipeline twice must not duplicate records. If a shipment already exists, update it. If it is new, insert it. This is called an **upsert**.
- **Efficient bulk loading** — inserting 100,000 records one row at a time is 50x slower than bulk loading. You will implement both and benchmark the difference.
- **Schema versioning** — the database schema will change over time (new columns, renamed fields). You will build a simple migration runner that applies versioned SQL scripts in order, never re-applying one that has already run.
- **Connection pooling** — opening a new database connection per record is catastrophically slow. You will use a connection pool so connections are reused across the pipeline run.

This project is what separates a junior engineer who "loads data into a database" from one who builds a reliable, maintainable data layer.

---

## Why These Concepts Matter in Data Engineering

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **Strategy Pattern** | You need to swap databases (dev SQLite → prod PostgreSQL) without changing pipeline code | Loader code is coupled to one database; migrating to a new DB requires rewriting the entire load layer |
| **Upsert (INSERT OR UPDATE)** | Pipeline re-runs daily; records get updated in source systems | Every re-run creates duplicate rows; analysts query stale or duplicate data |
| **Bulk Loading** | Production tables have millions of rows loaded daily | Row-by-row inserts take hours instead of minutes; pipelines miss SLA windows |
| **Connection Pooling** | Database connections are expensive to open — each takes 50–200ms | One connection per insert kills performance; connection limit exhausted under load |
| **Schema Migrations** | Database schemas evolve — new business fields, renamed columns, added indexes | Manual schema changes on prod with no history; impossible to reproduce the schema on a new environment |
| **Benchmarking** | You must prove your loading strategy is fast enough before going to production | You ship a slow loader and discover it on the first prod run at 2 AM |

---

## Project Architecture Overview

```
consolidated.csv  (from Project 01 output)
        │
        ▼
┌─────────────────────────────────────┐
│  MIGRATION RUNNER  (Day 3)          │
│  Reads /migrations/*.sql            │
│  Applies unapplied versions in order│
│  Tracks applied migrations in DB    │
└──────────────┬──────────────────────┘
               │ schema is ready
               ▼
┌─────────────────────────────────────┐
│  LOADER INTERFACE  (Day 1)          │
│  BaseLoader (abstract)              │
│  SQLiteLoader / PostgreSQLLoader    │
│  Selected via config — Strategy     │
└──────────────┬──────────────────────┘
               │ loader is wired
               ▼
┌─────────────────────────────────────┐
│  LOAD ENGINE  (Day 2)               │
│  parse_csv() → row dicts            │
│  upsert_one() vs bulk_upsert()      │
│  ConnectionPool wraps both DBs      │
└──────────────┬──────────────────────┘
               │ data is in DB
               ▼
┌─────────────────────────────────────┐
│  BENCHMARK REPORT  (Day 2)          │
│  row-by-row vs bulk time comparison │
│  Written to benchmark_report.txt    │
└─────────────────────────────────────┘
```

---

## Folder Structure

Create this structure before you begin Day 1:

```
database_sink/
│
├── data/
│   ├── inputs/
│   │   └── consolidated.csv        ← output from Project 01 (or use sample)
│   └── outputs/
│       └── benchmark_report.txt    ← pipeline creates this
│
├── migrations/
│   ├── V001__create_shipments.sql  ← Day 3
│   ├── V002__add_weight_index.sql  ← Day 3
│   └── V003__add_region_column.sql ← Day 3
│
├── sink/
│   ├── __init__.py
│   ├── config.py                   ← Day 1
│   ├── base_loader.py              ← Day 1
│   ├── sqlite_loader.py            ← Day 1
│   ├── postgres_loader.py          ← Day 1
│   ├── pool.py                     ← Day 3
│   ├── upsert.py                   ← Day 2
│   ├── bulk.py                     ← Day 2
│   ├── benchmark.py                ← Day 2
│   └── migration_runner.py         ← Day 3
│
└── main.py                         ← Day 3
```

---

## How to Run the Full Project

### Prerequisites

```bash
pip install psycopg2-binary
```

For PostgreSQL you also need a running instance. The easiest way during development:

```bash
docker run --name swiftship-pg -e POSTGRES_PASSWORD=swiftship -e POSTGRES_DB=swiftship -p 5432:5432 -d postgres:15
```

SQLite requires no installation — it is built into Python.

### Run

```bash
cd database_sink
python main.py --db sqlite          # development run
python main.py --db postgres        # production run
python main.py --db sqlite --bench  # run with benchmark comparison
```

### Expected outputs

| Output | Description |
|---|---|
| `swiftship.db` (SQLite) or `swiftship` DB (Postgres) | Loaded shipments table, queryable via SQL |
| `data/outputs/benchmark_report.txt` | Row-by-row vs bulk insert time comparison |
| Console | Migration status, row count loaded, upsert vs insert breakdown |

---

## Sample Configuration

### `sink/config.py`

```python
DB_CONFIG = {
    "sqlite": {
        "db_path": "swiftship.db"
    },
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "dbname": "swiftship",
        "user": "postgres",
        "password": "swiftship"
    }
}

ACTIVE_DB = "sqlite"   # change to "postgres" for production
INPUT_CSV = "data/inputs/consolidated.csv"
BENCHMARK_OUTPUT = "data/outputs/benchmark_report.txt"
```

---

## Interview Cheat Sheet

**"Why did you use the Strategy Pattern for your loaders?"**
> Because the pipeline code should not know or care which database it is writing to. The `BaseLoader` defines the contract — `connect()`, `upsert()`, `bulk_load()`, `close()` — and both `SQLiteLoader` and `PostgreSQLLoader` implement it. Switching databases is a one-line config change. Without this pattern, the pipeline code would be full of `if db == "sqlite": ... else: ...` branches that get messier every time a new database is added.

**"What is an upsert and why does your pipeline need it?"**
> An upsert is an atomic operation: INSERT the record if it does not exist, UPDATE it if it does — based on a unique key (shipment_id). My pipeline needs it because it runs daily, and source systems sometimes resend corrected records for previous days. Without upsert, every re-run would create duplicate rows. In SQLite it is `INSERT OR REPLACE`, in PostgreSQL it is `INSERT ... ON CONFLICT (shipment_id) DO UPDATE SET ...`.

**"How much faster is bulk loading vs row-by-row?"**
> In my benchmarks, bulk loading 10,000 rows took roughly 80ms compared to 4,200ms for row-by-row — about 50x faster. The reason is transaction overhead: in row-by-row mode, each insert is a separate transaction with a disk flush. Bulk loading wraps all rows in one transaction and uses the database's native batch copy mechanism.

**"How does your migration runner work?"**
> It reads all `.sql` files from the `migrations/` folder, orders them by version prefix (V001, V002, etc.), and checks a `schema_migrations` table in the database to see which ones have already been applied. It only runs unapplied migrations, in order, each in its own transaction. If a migration fails, the transaction rolls back and the runner stops — it never leaves the schema in a half-applied state.

**"What is connection pooling and why does it matter here?"**
> A database connection involves a TCP handshake, authentication, and session setup — typically 50–200ms per connection. If the pipeline opens a new connection for every row or every batch, that overhead dominates total runtime. A connection pool maintains a fixed set of open connections and hands them out on request. My pool wraps both SQLite and PostgreSQL behind the same `acquire()` / `release()` interface, keeping two connections alive for the duration of the pipeline run.

---

*Every pattern in this project — strategy, upsert, bulk load, pooling, migrations — appears daily in production data layers at real companies.*
