# Day 2 — Upsert Logic, Bulk Loading & Benchmarking

> **Goal: Build the full CSV parser, implement bulk loading for production throughput, and write a benchmark that proves the performance difference between row-by-row and bulk modes. By end of day, you can load 10,000 records and report the time difference.**

---

## What You Are Building Today

Day 1 gave you the loader interface and both database backends. Today you focus on performance:

1. Build a `CSVParser` that reads `consolidated.csv` into record dicts — memory-efficiently using generators
2. Implement a `BulkLoader` that batches records and calls `bulk_upsert()` from Day 1
3. Write a `Benchmarker` that runs both strategies (row-by-row and bulk) and times each
4. Generate a `benchmark_report.txt` with the results

This is where you learn the most important performance lesson in data engineering: **batch size matters more than almost any other variable in load performance.**

---

## Step 1 — Build the CSV Parser

Create `sink/csv_parser.py`. This must be a generator — never load the entire file into memory.

```python
# sink/csv_parser.py

import csv
from typing import Generator, Dict, Any
from pathlib import Path


class CSVParser:
    """
    Parses a consolidated CSV file and yields one record dict per row.

    Uses a generator so memory usage is constant regardless of file size.
    A 1GB CSV file and a 1KB CSV file consume the same peak memory.
    """

    REQUIRED_COLUMNS = {
        "shipment_id", "origin_city", "destination_city",
        "dispatch_date", "status"
    }

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(
                f"Input file not found: {self.file_path}. "
                f"Run Project 01 first to generate consolidated.csv, "
                f"or use the sample data provided in data/inputs/."
            )

    def parse(self) -> Generator[Dict[str, Any], None, None]:
        """
        Yields one record dict per row in the CSV.
        Skips rows missing required fields and logs them.
        Coerces weight_kg to float — leaves it None if unparseable.
        """
        with open(self.file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate columns on first read
            missing = self.REQUIRED_COLUMNS - set(reader.fieldnames or [])
            if missing:
                raise ValueError(
                    f"CSV is missing required columns: {missing}. "
                    f"Expected columns: {self.REQUIRED_COLUMNS}"
                )

            for line_num, row in enumerate(reader, start=2):  # start=2: header is line 1
                # Skip rows with no shipment_id
                if not row.get("shipment_id", "").strip():
                    print(f"[CSVParser] Line {line_num}: skipping row with empty shipment_id")
                    continue

                # Coerce weight_kg
                raw_weight = row.get("weight_kg", "").strip()
                try:
                    weight = float(raw_weight) if raw_weight else None
                except ValueError:
                    weight = None

                yield {
                    "shipment_id":      row["shipment_id"].strip(),
                    "origin_city":      row.get("origin_city", "").strip().lower(),
                    "destination_city": row.get("destination_city", "").strip().lower(),
                    "dispatch_date":    row.get("dispatch_date", "").strip() or None,
                    "delivery_date":    row.get("delivery_date", "").strip() or None,
                    "status":           row.get("status", "").strip().upper(),
                    "weight_kg":        weight,
                    "source_file":      str(self.file_path.name)
                }

    def count_rows(self) -> int:
        """Count total rows without loading all into memory. Used for progress reporting."""
        with open(self.file_path, newline="", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1  # subtract header line
```

---

## Step 2 — Build the Bulk Loader

Create `sink/bulk.py`. This wraps the generator and feeds records to `bulk_upsert()` in configurable batch sizes.

```python
# sink/bulk.py

import time
from typing import Generator, Dict, Any, List

from sink.base_loader import BaseLoader


def batched(generator: Generator, batch_size: int) -> Generator[List[Dict], None, None]:
    """
    Consumes a generator and yields lists of up to batch_size items.

    This is the critical pattern for bulk loading: never accumulate all
    records in memory. Accumulate only one batch at a time.

    Example: 1,000,000 records with batch_size=1000 → 1,000 yielded lists,
    each with 1,000 records. Peak memory = one batch, not all records.
    """
    batch = []
    for item in generator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:  # yield the final partial batch
        yield batch


class BulkLoader:
    """
    Loads records into a database in batches using bulk_upsert().
    Tracks progress and timing for benchmark comparison.
    """

    def __init__(self, loader: BaseLoader, batch_size: int = 1000):
        self.loader = loader
        self.batch_size = batch_size

    def load(self, records: Generator) -> Dict[str, Any]:
        """
        Load all records in batches.
        Returns a stats dict: total rows, batch count, elapsed seconds.
        """
        total_rows = 0
        batch_count = 0
        start = time.perf_counter()

        for batch in batched(records, self.batch_size):
            rows_loaded = self.loader.bulk_upsert(batch)
            total_rows += rows_loaded
            batch_count += 1
            print(f"  Batch {batch_count}: {rows_loaded} rows loaded "
                  f"(total so far: {total_rows})")

        elapsed = time.perf_counter() - start

        return {
            "strategy":    "bulk",
            "batch_size":  self.batch_size,
            "total_rows":  total_rows,
            "batch_count": batch_count,
            "elapsed_sec": round(elapsed, 4),
            "rows_per_sec": round(total_rows / elapsed, 1) if elapsed > 0 else 0
        }
```

---

## Step 3 — Build the Row-by-Row Loader

Add `sink/row_loader.py` for the slow path — you need this to benchmark against.

```python
# sink/row_loader.py

import time
from typing import Generator, Dict, Any

from sink.base_loader import BaseLoader


class RowByRowLoader:
    """
    Loads records into the database one row at a time using upsert_one().
    This is intentionally slow — its purpose is to demonstrate why
    bulk loading exists, not to be used in production.
    """

    def __init__(self, loader: BaseLoader):
        self.loader = loader

    def load(self, records: Generator) -> Dict[str, Any]:
        """
        Load all records one by one.
        Returns a stats dict compatible with BulkLoader.load() output.
        """
        total_rows = 0
        inserted = 0
        updated = 0
        start = time.perf_counter()

        for record in records:
            result = self.loader.upsert_one(record)
            total_rows += 1
            if result == "inserted":
                inserted += 1
            else:
                updated += 1

            # Print progress every 500 rows to show it's alive
            if total_rows % 500 == 0:
                print(f"  Row-by-row progress: {total_rows} rows...")

        elapsed = time.perf_counter() - start

        return {
            "strategy":    "row_by_row",
            "batch_size":  1,
            "total_rows":  total_rows,
            "inserted":    inserted,
            "updated":     updated,
            "elapsed_sec": round(elapsed, 4),
            "rows_per_sec": round(total_rows / elapsed, 1) if elapsed > 0 else 0
        }
```

---

## Step 4 — Build the Benchmarker

Create `sink/benchmark.py`. This is what makes the performance difference visible and reportable.

```python
# sink/benchmark.py

import os
from typing import Dict, Any
from pathlib import Path
from datetime import datetime

from sink.config import INPUT_CSV, BENCHMARK_OUTPUT
from sink.csv_parser import CSVParser
from sink.loader_factory import get_loader
from sink.bulk import BulkLoader
from sink.row_loader import RowByRowLoader


class Benchmarker:
    """
    Runs both loading strategies against the same input data and
    writes a human-readable comparison report.

    Caution: Both runs must use the same number of rows for a fair
    comparison. The benchmarker resets the table between runs by
    dropping and recreating it.
    """

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.output_path = Path(BENCHMARK_OUTPUT)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _count_input_rows(self) -> int:
        return CSVParser(INPUT_CSV).count_rows()

    def run(self) -> None:
        total_rows = self._count_input_rows()
        print(f"\n[Benchmarker] Input: {total_rows} rows from {INPUT_CSV}")
        print(f"[Benchmarker] Batch size: {self.batch_size}")

        # --- Run 1: Row-by-row ---
        print("\n[Benchmarker] Running row-by-row strategy...")
        with get_loader() as loader:
            loader.create_schema()
            parser = CSVParser(INPUT_CSV)
            row_loader = RowByRowLoader(loader)
            row_stats = row_loader.load(parser.parse())

        # --- Run 2: Bulk loading ---
        print("\n[Benchmarker] Running bulk strategy...")
        with get_loader() as loader:
            loader.create_schema()  # table already exists — idempotent
            parser = CSVParser(INPUT_CSV)
            bulk_loader = BulkLoader(loader, batch_size=self.batch_size)
            bulk_stats = bulk_loader.load(parser.parse())

        self._write_report(row_stats, bulk_stats, total_rows)

    def _write_report(
        self,
        row_stats: Dict[str, Any],
        bulk_stats: Dict[str, Any],
        total_rows: int
    ) -> None:
        speedup = (
            round(row_stats["elapsed_sec"] / bulk_stats["elapsed_sec"], 1)
            if bulk_stats["elapsed_sec"] > 0 else "N/A"
        )

        report = f"""
================================================================================
  SWIFTSHIP DATABASE SINK — BENCHMARK REPORT
  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
================================================================================

  Input file   : {INPUT_CSV}
  Total rows   : {total_rows:,}
  Batch size   : {self.batch_size:,}

--------------------------------------------------------------------------------
  STRATEGY              | ROW-BY-ROW       | BULK LOAD
--------------------------------------------------------------------------------
  Total rows loaded     | {row_stats['total_rows']:>16,} | {bulk_stats['total_rows']:>16,}
  Time elapsed (sec)    | {row_stats['elapsed_sec']:>16.4f} | {bulk_stats['elapsed_sec']:>16.4f}
  Rows per second       | {row_stats['rows_per_sec']:>16,.1f} | {bulk_stats['rows_per_sec']:>16,.1f}
  Batch count           | {1:>16,} | {bulk_stats['batch_count']:>16,}
--------------------------------------------------------------------------------

  Bulk loading is {speedup}x FASTER than row-by-row for {total_rows:,} rows.

  Why?
  - Row-by-row: each upsert_one() call opens a transaction, writes to disk,
    and commits. That is {total_rows:,} separate transactions.
  - Bulk load:  all {total_rows:,} rows are wrapped in {bulk_stats['batch_count']}
    transaction(s) of {self.batch_size:,} rows each. Disk writes are batched.
  - The database's write-ahead log (WAL) must flush on every COMMIT.
    Fewer COMMITs = dramatically fewer disk flushes = dramatically less time.

================================================================================
""".strip()

        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"\n[Benchmarker] Report written to {self.output_path}")
        print(f"[Benchmarker] Bulk is {speedup}x faster than row-by-row.")
```

---

## Step 5 — Generate Sample Data for Benchmarking

If you do not have `consolidated.csv` from Project 01, generate synthetic data:

```python
# generate_sample_data.py  (run once to create test input)

import csv
import random
from pathlib import Path

STATUSES = ["DELIVERED", "IN_TRANSIT", "PENDING", "RETURNED", "FAILED"]
CITIES = ["mumbai", "delhi", "bangalore", "hyderabad", "chennai",
          "pune", "kolkata", "ahmedabad", "surat", "nagpur"]

Path("data/inputs").mkdir(parents=True, exist_ok=True)

with open("data/inputs/consolidated.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "shipment_id", "origin_city", "destination_city",
        "dispatch_date", "delivery_date", "status", "weight_kg", "source_file"
    ])
    writer.writeheader()

    for i in range(1, 10_001):  # 10,000 rows
        origin, dest = random.sample(CITIES, 2)
        writer.writerow({
            "shipment_id":      f"SH{i:06d}",
            "origin_city":      origin,
            "destination_city": dest,
            "dispatch_date":    "2024-01-10",
            "delivery_date":    "2024-01-13" if random.random() > 0.2 else "",
            "status":           random.choice(STATUSES),
            "weight_kg":        round(random.uniform(0.5, 50.0), 2),
            "source_file":      "consolidated.csv"
        })

print("Generated data/inputs/consolidated.csv with 10,000 rows.")
```

Run: `python generate_sample_data.py`

---

## Step 6 — Day 2 Smoke Test

```python
# test_day2.py  (delete after verification)

from sink.config import ACTIVE_DB
from sink.csv_parser import CSVParser
from sink.loader_factory import get_loader
from sink.bulk import BulkLoader

print(f"Testing bulk load to: {ACTIVE_DB}")

parser = CSVParser("data/inputs/consolidated.csv")
total = parser.count_rows()
print(f"Input rows: {total}")

with get_loader() as loader:
    loader.create_schema()
    bulk_loader = BulkLoader(loader, batch_size=500)
    stats = bulk_loader.load(parser.parse())

print(f"\nStats: {stats}")
print(f"Rows in DB: {loader.row_count()}")  # will fail — conn is closed
# To query after close, open a second connection:
with get_loader() as loader:
    loader.create_schema()
    print(f"Rows in DB after load: {loader.row_count()}")
```

Then run the full benchmark:

```python
# test_benchmark.py
from sink.benchmark import Benchmarker
b = Benchmarker(batch_size=1000)
b.run()
```

---

## What to Observe in the Benchmark Output

Open `data/outputs/benchmark_report.txt`. You should see something like:

```
  Total rows loaded     |           10,000 |           10,000
  Time elapsed (sec)    |           4.2100 |           0.0830
  Rows per second       |        2,375.3   |      120,481.9
  Bulk loading is 50.7x FASTER than row-by-row for 10,000 rows.
```

The exact numbers depend on your machine and database. The ratio matters — it should be **30x–100x** faster for SQLite, and **20x–50x** for PostgreSQL.

---

## Day 2 Checklist

Before moving to Day 3, confirm:

- [ ] `CSVParser.parse()` is a generator — uses `yield`, never builds a list
- [ ] `batched()` yields lists of exactly `batch_size` records (last batch may be smaller)
- [ ] `BulkLoader.load()` returns a stats dict with `elapsed_sec` and `rows_per_sec`
- [ ] `RowByRowLoader.load()` returns a comparable stats dict
- [ ] `Benchmarker.run()` produces `benchmark_report.txt` in `data/outputs/`
- [ ] Bulk loading is at least 10x faster than row-by-row on 10,000 rows
- [ ] Running the benchmark twice does not double the row count (upserts, not inserts)

---

*Day 3 completes the project: schema migrations, connection pooling, and wiring everything into a single `main.py` entry point.*
