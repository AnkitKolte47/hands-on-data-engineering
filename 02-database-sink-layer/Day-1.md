# Day 1 — Strategy Pattern, Connection Management & Schema Design

> **Goal: Build the loader interface and connect to both SQLite and PostgreSQL using the Strategy Pattern. By end of day, both loaders can open a connection, create the schema, and close cleanly.**

---

## What You Are Building Today

Today is about design before code. You will:

1. Define a `BaseLoader` abstract class that sets the contract every database loader must follow
2. Implement `SQLiteLoader` — connects to a local `.db` file
3. Implement `PostgreSQLLoader` — connects to a running Postgres instance
4. Write a `config.py` that controls which loader is active without changing pipeline code
5. Verify both loaders connect, create the shipments table, and close — with no errors

By the end of today, you can swap the database by changing one line in `config.py`. Nothing else changes.

---

## Step 1 — Create the Config File

Create `sink/config.py`. This is the single source of truth for all database settings.

```python
# sink/config.py

DB_CONFIG = {
    "sqlite": {
        "db_path": "swiftship.db"
    },
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "dbname": "swiftship",
        "user": "postgres",
        "password": "swiftship",
        "pool_size": 2
    }
}

# Change this one line to switch databases. Nothing else changes.
ACTIVE_DB = "sqlite"

INPUT_CSV = "data/inputs/consolidated.csv"
BENCHMARK_OUTPUT = "data/outputs/benchmark_report.txt"
MIGRATIONS_DIR = "migrations"
```

**Why a single config file?** In production, this value comes from an environment variable (`os.environ.get("ACTIVE_DB", "sqlite")`). The config file is the local stand-in. Every other module imports from here — no hardcoded strings anywhere else.

---

## Step 2 — Define the Base Loader (Abstract Class)

Create `sink/base_loader.py`.

```python
# sink/base_loader.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLoader(ABC):
    """
    Abstract base class for all database loaders.

    Every concrete loader (SQLite, PostgreSQL, etc.) must implement
    all abstract methods defined here. This is the Strategy Pattern:
    the pipeline code depends only on BaseLoader, never on a specific
    database implementation.
    """

    @abstractmethod
    def connect(self) -> None:
        """Open the database connection. Must be called before any other method."""
        pass

    @abstractmethod
    def create_schema(self) -> None:
        """
        Create the shipments table if it does not exist.
        Must be idempotent — safe to call on every run.
        """
        pass

    @abstractmethod
    def upsert_one(self, record: Dict[str, Any]) -> str:
        """
        Insert a single record, or update it if shipment_id already exists.
        Returns 'inserted' or 'updated' to allow the caller to track counts.
        """
        pass

    @abstractmethod
    def bulk_upsert(self, records: List[Dict[str, Any]]) -> int:
        """
        Load a list of records in a single transaction.
        Returns the number of rows affected.
        """
        pass

    @abstractmethod
    def row_count(self) -> int:
        """Return the current number of rows in the shipments table."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection cleanly."""
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # do not suppress exceptions
```

**Why `__enter__` and `__exit__` on the base class?** So every loader automatically supports `with SQLiteLoader(...) as loader:` — the connection is always closed, even if an exception occurs mid-load. You write this once on the base class, not once per subclass.

**Why return `str` from `upsert_one`?** The pipeline needs to know how many records were new versus updated for the audit log and summary report. Returning `'inserted'` or `'updated'` lets the caller increment the right counter without querying the database again.

---

## Step 3 — Implement SQLiteLoader

Create `sink/sqlite_loader.py`.

```python
# sink/sqlite_loader.py

import sqlite3
from typing import List, Dict, Any

from sink.base_loader import BaseLoader


class SQLiteLoader(BaseLoader):
    """
    Loads shipment records into a local SQLite database.

    SQLite uses INSERT OR REPLACE for upserts. This replaces the entire
    row when a conflict on shipment_id is detected — appropriate for
    our use case because we always have all fields available.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._cursor = None

    def connect(self) -> None:
        """
        Open a connection to the SQLite file.
        isolation_level=None enables autocommit mode — we manage
        transactions explicitly using BEGIN/COMMIT in bulk operations.
        """
        self._conn = sqlite3.connect(self.db_path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row  # rows accessible by column name
        self._cursor = self._conn.cursor()
        print(f"[SQLiteLoader] Connected to {self.db_path}")

    def create_schema(self) -> None:
        """
        Create the shipments table. IF NOT EXISTS makes this idempotent.
        Running this on every pipeline start is safe and correct.
        """
        self._cursor.execute("""
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
            )
        """)
        # Track schema migrations in a separate table
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     TEXT PRIMARY KEY,
                applied_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        print("[SQLiteLoader] Schema verified.")

    def upsert_one(self, record: Dict[str, Any]) -> str:
        """
        SQLite upsert: INSERT OR REPLACE.
        'OR REPLACE' deletes the conflicting row and inserts a new one.
        The PRIMARY KEY constraint on shipment_id triggers the replace.
        """
        # Check if this shipment_id exists before insert, so we can report
        self._cursor.execute(
            "SELECT 1 FROM shipments WHERE shipment_id = ?",
            (record["shipment_id"],)
        )
        exists = self._cursor.fetchone() is not None

        self._cursor.execute("""
            INSERT OR REPLACE INTO shipments
                (shipment_id, origin_city, destination_city,
                 dispatch_date, delivery_date, status, weight_kg, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get("shipment_id"),
            record.get("origin_city"),
            record.get("destination_city"),
            record.get("dispatch_date"),
            record.get("delivery_date"),
            record.get("status"),
            record.get("weight_kg"),
            record.get("source_file")
        ))

        return "updated" if exists else "inserted"

    def bulk_upsert(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk load using executemany inside a single transaction.
        One BEGIN + one COMMIT for all rows = massively faster than
        one transaction per row.
        """
        rows = [
            (
                r.get("shipment_id"),
                r.get("origin_city"),
                r.get("destination_city"),
                r.get("dispatch_date"),
                r.get("delivery_date"),
                r.get("status"),
                r.get("weight_kg"),
                r.get("source_file")
            )
            for r in records
        ]

        self._cursor.execute("BEGIN")
        try:
            self._cursor.executemany("""
                INSERT OR REPLACE INTO shipments
                    (shipment_id, origin_city, destination_city,
                     dispatch_date, delivery_date, status, weight_kg, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            self._cursor.execute("COMMIT")
        except Exception:
            self._cursor.execute("ROLLBACK")
            raise

        return len(rows)

    def row_count(self) -> int:
        self._cursor.execute("SELECT COUNT(*) FROM shipments")
        return self._cursor.fetchone()[0]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            print(f"[SQLiteLoader] Connection to {self.db_path} closed.")
```

---

## Step 4 — Implement PostgreSQLLoader

Create `sink/postgres_loader.py`.

```python
# sink/postgres_loader.py

import psycopg2
import psycopg2.extras
from typing import List, Dict, Any

from sink.base_loader import BaseLoader


class PostgreSQLLoader(BaseLoader):
    """
    Loads shipment records into a PostgreSQL database.

    PostgreSQL upserts use INSERT ... ON CONFLICT DO UPDATE (standard SQL).
    This is more precise than SQLite's INSERT OR REPLACE — it updates only
    specified columns, preserving metadata columns like loaded_at.
    """

    def __init__(self, host: str, port: int, dbname: str,
                 user: str, password: str):
        self.conn_params = {
            "host": host, "port": port, "dbname": dbname,
            "user": user, "password": password
        }
        self._conn = None
        self._cursor = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(**self.conn_params)
        self._conn.autocommit = False  # we manage transactions explicitly
        self._cursor = self._conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        print(f"[PostgreSQLLoader] Connected to {self.conn_params['dbname']} "
              f"at {self.conn_params['host']}")

    def create_schema(self) -> None:
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS shipments (
                shipment_id      TEXT PRIMARY KEY,
                origin_city      TEXT NOT NULL,
                destination_city TEXT NOT NULL,
                dispatch_date    DATE,
                delivery_date    DATE,
                status           TEXT NOT NULL,
                weight_kg        NUMERIC(10, 2),
                source_file      TEXT,
                loaded_at        TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     TEXT PRIMARY KEY,
                applied_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        self._conn.commit()
        print("[PostgreSQLLoader] Schema verified.")

    def upsert_one(self, record: Dict[str, Any]) -> str:
        """
        PostgreSQL ON CONFLICT: if shipment_id already exists, update
        only the data columns — do NOT update loaded_at (preserve first-seen time).
        """
        self._cursor.execute(
            "SELECT 1 FROM shipments WHERE shipment_id = %s",
            (record["shipment_id"],)
        )
        exists = self._cursor.fetchone() is not None

        self._cursor.execute("""
            INSERT INTO shipments
                (shipment_id, origin_city, destination_city,
                 dispatch_date, delivery_date, status, weight_kg, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (shipment_id) DO UPDATE SET
                origin_city      = EXCLUDED.origin_city,
                destination_city = EXCLUDED.destination_city,
                dispatch_date    = EXCLUDED.dispatch_date,
                delivery_date    = EXCLUDED.delivery_date,
                status           = EXCLUDED.status,
                weight_kg        = EXCLUDED.weight_kg,
                source_file      = EXCLUDED.source_file
        """, (
            record.get("shipment_id"),
            record.get("origin_city"),
            record.get("destination_city"),
            record.get("dispatch_date") or None,
            record.get("delivery_date") or None,
            record.get("status"),
            record.get("weight_kg"),
            record.get("source_file")
        ))
        self._conn.commit()
        return "updated" if exists else "inserted"

    def bulk_upsert(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk upsert using execute_values — PostgreSQL's most efficient
        multi-row insert method. Generates a single parameterized SQL
        statement for all rows, not N separate statements.
        """
        rows = [
            (
                r.get("shipment_id"),
                r.get("origin_city"),
                r.get("destination_city"),
                r.get("dispatch_date") or None,
                r.get("delivery_date") or None,
                r.get("status"),
                r.get("weight_kg"),
                r.get("source_file")
            )
            for r in records
        ]

        psycopg2.extras.execute_values(self._cursor, """
            INSERT INTO shipments
                (shipment_id, origin_city, destination_city,
                 dispatch_date, delivery_date, status, weight_kg, source_file)
            VALUES %s
            ON CONFLICT (shipment_id) DO UPDATE SET
                origin_city      = EXCLUDED.origin_city,
                destination_city = EXCLUDED.destination_city,
                dispatch_date    = EXCLUDED.dispatch_date,
                delivery_date    = EXCLUDED.delivery_date,
                status           = EXCLUDED.status,
                weight_kg        = EXCLUDED.weight_kg,
                source_file      = EXCLUDED.source_file
        """, rows, page_size=1000)

        self._conn.commit()
        return len(rows)

    def row_count(self) -> int:
        self._cursor.execute("SELECT COUNT(*) FROM shipments")
        return self._cursor.fetchone()["count"]

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
            print(f"[PostgreSQLLoader] Connection closed.")
```

---

## Step 5 — Write a Loader Factory

Add `sink/loader_factory.py` so the rest of the code never imports a specific loader class directly.

```python
# sink/loader_factory.py

from sink.config import DB_CONFIG, ACTIVE_DB
from sink.base_loader import BaseLoader
from sink.sqlite_loader import SQLiteLoader
from sink.postgres_loader import PostgreSQLLoader


def get_loader() -> BaseLoader:
    """
    Returns the configured loader based on ACTIVE_DB in config.
    This is the Factory function — it hides which concrete class is used.
    """
    db = ACTIVE_DB
    cfg = DB_CONFIG[db]

    if db == "sqlite":
        return SQLiteLoader(db_path=cfg["db_path"])
    elif db == "postgres":
        return PostgreSQLLoader(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"]
        )
    else:
        raise ValueError(f"Unknown database type: {db!r}. "
                         f"Valid options: 'sqlite', 'postgres'")
```

---

## Step 6 — Smoke Test Both Loaders

Create a quick test script at the project root. Run it to verify Day 1 is complete.

```python
# test_day1.py  (delete after verification)

from sink.loader_factory import get_loader
from sink.config import ACTIVE_DB

print(f"Testing loader: {ACTIVE_DB}")

with get_loader() as loader:
    loader.create_schema()
    print(f"Row count before: {loader.row_count()}")

    result = loader.upsert_one({
        "shipment_id": "TEST001",
        "origin_city": "mumbai",
        "destination_city": "delhi",
        "dispatch_date": "2024-01-10",
        "delivery_date": "2024-01-13",
        "status": "DELIVERED",
        "weight_kg": 12.5,
        "source_file": "test"
    })
    print(f"First upsert: {result}")   # expect: inserted

    result = loader.upsert_one({
        "shipment_id": "TEST001",      # same ID — should update
        "origin_city": "mumbai",
        "destination_city": "delhi",
        "dispatch_date": "2024-01-10",
        "delivery_date": "2024-01-13",
        "status": "RETURNED",          # changed status
        "weight_kg": 12.5,
        "source_file": "test"
    })
    print(f"Second upsert: {result}")  # expect: updated

    print(f"Row count after: {loader.row_count()}")  # expect: 1 (not 2)

print("Day 1 complete. Schema and both loaders are working.")
```

Run it:

```bash
# Test SQLite (default)
python test_day1.py

# Test PostgreSQL
# Edit config.py: ACTIVE_DB = "postgres"
python test_day1.py
```

---

## Day 1 Checklist

Before moving to Day 2, confirm:

- [ ] `BaseLoader` defines all abstract methods and `__enter__`/`__exit__`
- [ ] `SQLiteLoader` connects, creates schema, upserts, and closes
- [ ] `PostgreSQLLoader` connects, creates schema, upserts, and closes
- [ ] `loader_factory.get_loader()` returns the right loader based on config
- [ ] Running `test_day1.py` with `ACTIVE_DB = "sqlite"` produces `inserted` then `updated` with row count = 1
- [ ] Switching to `"postgres"` and rerunning produces the same result

---

*The Strategy Pattern you built today is the backbone of the project. Day 2 focuses on making the load fast — bulk loading and benchmarking.*
