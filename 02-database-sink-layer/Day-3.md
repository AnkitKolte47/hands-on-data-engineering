# Day 3 — Schema Migrations, Connection Pooling & Pipeline Integration

> **Goal: Build a migration runner that versions your schema, add a connection pool so connections are reused across the pipeline, and wire everything into a single `main.py` that runs the complete database sink. By end of day, you have a production-ready, idempotent data load pipeline.**

---

## What You Are Building Today

Day 3 brings the project to production quality:

1. Write versioned SQL migration files and a `MigrationRunner` that applies them in order, never twice
2. Build a `ConnectionPool` that keeps connections alive across batches instead of opening one per operation
3. Add a `LoadSummary` that tracks inserts, updates, errors, and timing for the audit trail
4. Wire everything into `main.py` with CLI argument parsing
5. Verify the full pipeline is idempotent — running it twice produces identical results

---

## Step 1 — Write the Migration SQL Files

Create the `migrations/` directory with three versioned migration scripts.

**`migrations/V001__create_shipments.sql`**

```sql
-- V001: Initial schema — shipments table and migration tracking table
-- Applied: automatically on first run via MigrationRunner

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id      TEXT PRIMARY KEY,
    origin_city      TEXT NOT NULL,
    destination_city TEXT NOT NULL,
    dispatch_date    TEXT,
    delivery_date    TEXT,
    status           TEXT NOT NULL,
    weight_kg        REAL,
    source_file      TEXT,
    loaded_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TEXT DEFAULT (datetime('now'))
);
```

**`migrations/V002__add_weight_index.sql`**

```sql
-- V002: Performance — add an index on status for common filter queries
-- Analysts frequently run: WHERE status = 'IN_TRANSIT'
-- This index makes those queries 10x–100x faster on large tables.

CREATE INDEX IF NOT EXISTS idx_shipments_status
    ON shipments (status);

CREATE INDEX IF NOT EXISTS idx_shipments_dispatch_date
    ON shipments (dispatch_date);
```

**`migrations/V003__add_region_column.sql`**

```sql
-- V003: Schema evolution — add a region column for regional reporting
-- New column is nullable so existing rows are unaffected (no backfill needed here).
-- The pipeline will populate it going forward from source_file.

ALTER TABLE shipments ADD COLUMN region TEXT;
```

---

## Step 2 — Build the Migration Runner

Create `sink/migration_runner.py`.

```python
# sink/migration_runner.py

import os
import re
from pathlib import Path
from typing import List, Tuple


class MigrationRunner:
    """
    Applies versioned SQL migration scripts to the database in order.

    Design rules:
    - Migration files are named V{number}__{description}.sql
    - Versions are applied in ascending numeric order
    - Each version is tracked in the schema_migrations table
    - A version that has already been applied is NEVER applied again
    - If a migration fails, the transaction rolls back and the runner stops —
      it will not proceed to the next version with a broken schema

    This mirrors Flyway and Liquibase behaviour — the two most common
    migration tools in enterprise data engineering.
    """

    VERSION_PATTERN = re.compile(r"^V(\d+)__(.+)\.sql$")

    def __init__(self, migrations_dir: str, conn, cursor):
        self.migrations_dir = Path(migrations_dir)
        self.conn = conn
        self.cursor = cursor

    def _get_migration_files(self) -> List[Tuple[int, str, Path]]:
        """
        Scans the migrations directory and returns (version_number, filename, path)
        tuples sorted by version number ascending.
        Raises if any file does not match the naming convention.
        """
        files = []
        for f in self.migrations_dir.glob("*.sql"):
            match = self.VERSION_PATTERN.match(f.name)
            if not match:
                raise ValueError(
                    f"Migration file '{f.name}' does not follow the naming "
                    f"convention: V{{number}}__{{description}}.sql"
                )
            version_num = int(match.group(1))
            files.append((version_num, f.name, f))

        return sorted(files, key=lambda x: x[0])

    def _get_applied_versions(self) -> set:
        """
        Returns the set of version strings already in schema_migrations.
        If the table does not yet exist, returns an empty set.
        """
        try:
            self.cursor.execute(
                "SELECT version FROM schema_migrations"
            )
            return {row[0] for row in self.cursor.fetchall()}
        except Exception:
            # Table does not exist yet — this is the very first run
            return set()

    def _apply(self, version_str: str, filepath: Path) -> None:
        """
        Reads and executes a single migration file in a transaction.
        Records the version in schema_migrations on success.
        Rolls back and raises on failure.
        """
        sql = filepath.read_text(encoding="utf-8")

        try:
            # executescript on SQLite handles multiple statements
            # For PostgreSQL use, split on ';' and execute individually
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    self.cursor.execute(statement)

            self.cursor.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)",
                (version_str,)
            )
            # Commit is done after all statements succeed
            try:
                self.conn.commit()
            except AttributeError:
                pass  # SQLite in autocommit mode — no explicit commit needed

            print(f"  [Migration] Applied: {version_str}")

        except Exception as e:
            try:
                self.conn.rollback()
            except AttributeError:
                pass
            raise RuntimeError(
                f"Migration {version_str} failed and was rolled back. "
                f"Fix the SQL and re-run. Error: {e}"
            ) from e

    def run(self) -> int:
        """
        Applies all unapplied migrations in version order.
        Returns the count of migrations applied in this run.
        """
        # Ensure schema_migrations table exists before querying it
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     TEXT PRIMARY KEY,
                applied_at  TEXT DEFAULT (datetime('now'))
            )
        """)

        files = self._get_migration_files()
        applied = self._get_applied_versions()

        pending = [
            (num, name, path)
            for num, name, path in files
            if name not in applied
        ]

        if not pending:
            print("[MigrationRunner] Schema is up to date. No migrations to apply.")
            return 0

        print(f"[MigrationRunner] {len(pending)} migration(s) to apply...")
        for num, name, path in pending:
            self._apply(name, path)

        print(f"[MigrationRunner] Done. {len(pending)} migration(s) applied.")
        return len(pending)
```

---

## Step 3 — Build the Connection Pool

Create `sink/pool.py`. For this project, the pool maintains a small, fixed number of connections and hands them out via a context manager.

```python
# sink/pool.py

import threading
import queue
from contextlib import contextmanager
from typing import Callable, Any


class ConnectionPool:
    """
    A simple fixed-size connection pool.

    Maintains `pool_size` open connections. Callers acquire a connection
    via the `acquire()` context manager. If all connections are in use,
    the caller blocks until one is returned.

    In production you would use psycopg2's built-in pool or SQLAlchemy's
    pool. This implementation exists to show you how pools work internally.

    Thread safety: the pool uses a Queue which is thread-safe by design.
    Each connection is used by at most one thread at a time.
    """

    def __init__(self, factory: Callable[[], Any], pool_size: int = 2):
        """
        Args:
            factory: A callable that creates and returns a new connection.
                     Called pool_size times during __init__.
            pool_size: Number of connections to keep open. Default 2.
        """
        self._pool = queue.Queue(maxsize=pool_size)
        self._pool_size = pool_size
        self._factory = factory

        # Pre-open all connections at startup
        for _ in range(pool_size):
            conn = factory()
            self._pool.put(conn)

        print(f"[ConnectionPool] Initialised with {pool_size} connection(s).")

    @contextmanager
    def acquire(self):
        """
        Context manager: borrow a connection from the pool.
        Automatically returns it when the `with` block exits.

        Usage:
            with pool.acquire() as conn:
                conn.execute(...)
        """
        conn = self._pool.get()  # blocks if pool is empty
        try:
            yield conn
        finally:
            self._pool.put(conn)  # always return — even on exception

    def close_all(self) -> None:
        """Close all connections in the pool. Call at pipeline shutdown."""
        closed = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed += 1
            except queue.Empty:
                break
        print(f"[ConnectionPool] Closed {closed} connection(s).")
```

---

## Step 4 — Build the Load Summary

Create `sink/summary.py`. This tracks and displays load statistics.

```python
# sink/summary.py

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class LoadSummary:
    """
    Tracks statistics for a single pipeline run.
    Passed through the pipeline and printed/saved at the end.
    """
    run_started_at: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    total_parsed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    total_skipped: int = 0
    total_errors: int = 0
    migrations_applied: int = 0
    strategy: str = "bulk"
    elapsed_sec: float = 0.0

    def print(self) -> None:
        print(f"""
========================================
  SWIFTSHIP DATABASE SINK — RUN SUMMARY
========================================
  Run started     : {self.run_started_at}
  Strategy        : {self.strategy}
  Migrations      : {self.migrations_applied} applied

  Rows parsed     : {self.total_parsed:,}
  Rows inserted   : {self.total_inserted:,}
  Rows updated    : {self.total_updated:,}
  Rows skipped    : {self.total_skipped:,}
  Errors          : {self.total_errors:,}

  Total time      : {self.elapsed_sec:.2f}s
========================================""")
```

---

## Step 5 — Wire Everything in main.py

```python
# main.py

import argparse
import time

from sink.config import ACTIVE_DB, MIGRATIONS_DIR, INPUT_CSV
from sink.loader_factory import get_loader
from sink.migration_runner import MigrationRunner
from sink.csv_parser import CSVParser
from sink.bulk import BulkLoader
from sink.benchmark import Benchmarker
from sink.summary import LoadSummary


def run_pipeline(strategy: str = "bulk", batch_size: int = 1000) -> None:
    summary = LoadSummary(strategy=strategy)
    start = time.perf_counter()

    print(f"\n{'='*50}")
    print(f"  SwiftShip Database Sink")
    print(f"  Target DB  : {ACTIVE_DB}")
    print(f"  Strategy   : {strategy}")
    print(f"  Batch size : {batch_size:,}")
    print(f"{'='*50}\n")

    with get_loader() as loader:
        # Step 1 — Run migrations
        print("[Step 1/3] Running schema migrations...")
        runner = MigrationRunner(MIGRATIONS_DIR, loader._conn, loader._cursor)
        summary.migrations_applied = runner.run()

        # Step 2 — Parse input
        print(f"\n[Step 2/3] Parsing {INPUT_CSV}...")
        parser = CSVParser(INPUT_CSV)
        summary.total_parsed = parser.count_rows()
        print(f"  Found {summary.total_parsed:,} rows to load.")

        # Step 3 — Load
        print(f"\n[Step 3/3] Loading records ({strategy} mode)...")
        if strategy == "bulk":
            bulk_loader = BulkLoader(loader, batch_size=batch_size)
            stats = bulk_loader.load(parser.parse())
            summary.total_inserted = stats["total_rows"]  # bulk doesn't distinguish ins/upd
        else:
            from sink.row_loader import RowByRowLoader
            row_loader = RowByRowLoader(loader)
            stats = row_loader.load(parser.parse())
            summary.total_inserted = stats.get("inserted", 0)
            summary.total_updated = stats.get("updated", 0)

    summary.elapsed_sec = round(time.perf_counter() - start, 4)
    summary.print()


def main():
    parser = argparse.ArgumentParser(description="SwiftShip Database Sink")
    parser.add_argument(
        "--db", choices=["sqlite", "postgres"], default=ACTIVE_DB,
        help="Database backend to use (overrides config.py)"
    )
    parser.add_argument(
        "--strategy", choices=["bulk", "row"], default="bulk",
        help="Load strategy: 'bulk' (fast, default) or 'row' (slow, for learning)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000,
        help="Rows per batch in bulk mode (default: 1000)"
    )
    parser.add_argument(
        "--bench", action="store_true",
        help="Run benchmark comparison instead of normal load"
    )
    args = parser.parse_args()

    # Override config if --db was passed
    if args.db != ACTIVE_DB:
        import sink.config as cfg
        cfg.ACTIVE_DB = args.db
        print(f"[Config] Overriding ACTIVE_DB to: {args.db}")

    if args.bench:
        print("Running benchmark mode...")
        b = Benchmarker(batch_size=args.batch_size)
        b.run()
    else:
        run_pipeline(strategy=args.strategy, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
```

---

## Step 6 — Full End-to-End Run

```bash
# Generate sample data if needed
python generate_sample_data.py

# Normal bulk run (SQLite)
python main.py

# Row-by-row run (slow — for learning)
python main.py --strategy row

# Benchmark comparison
python main.py --bench

# Run against PostgreSQL
python main.py --db postgres

# Check what was loaded
sqlite3 swiftship.db "SELECT status, COUNT(*) FROM shipments GROUP BY status;"
sqlite3 swiftship.db "SELECT * FROM schema_migrations;"
```

---

## Day 3 Checklist

Before calling Project 02 complete, confirm:

- [ ] Three migration files exist in `migrations/` with proper naming (`V001__`, `V002__`, `V003__`)
- [ ] `MigrationRunner` applies all three on first run, zero on second run
- [ ] `schema_migrations` table shows all three versions after first run
- [ ] `main.py` completes without errors for both `--strategy bulk` and `--strategy row`
- [ ] Running `main.py` twice produces the same row count (idempotent via upserts)
- [ ] `--bench` flag produces `data/outputs/benchmark_report.txt` showing bulk is faster
- [ ] `ALTER TABLE` in V003 migration added the `region` column (verify with `.schema shipments` in sqlite3)

---

*With Project 02 complete, you can now ingest data from multiple formats (Project 01) and store it reliably in a real database. Project 03 automates this with Airflow so it runs on a schedule without manual intervention.*
