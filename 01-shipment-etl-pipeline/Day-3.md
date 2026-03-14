---
## Day 3

# Day 3 — Resilience & Output: Exception Handling, Audit & Reporting

**Goal:** Make the pipeline production-grade. By end of Day 3, the pipeline handles every error gracefully, quarantines bad records, writes all outputs atomically, and produces a full audit log of every run.

**Concepts practiced:** Custom exception hierarchies, `try/except/else/finally`, exception chaining (`raise from`), context managers, file writing (append vs overwrite), JSON report generation

**Estimated time:** 5–6 hours

---

### Step 3.1 — Build the custom exception hierarchy (`pipeline/exceptions.py`)

Never use built-in exceptions like `Exception` or `ValueError` for domain errors. Custom exceptions make your code self-documenting and allow callers to catch specific error types.

```python
# pipeline/exceptions.py

class PipelineException(Exception):
    """
    Base class for all ShipmentETL exceptions.
    Catching PipelineException catches any pipeline error.
    Catching a subclass catches only that specific stage's errors.
    """
    def __init__(self, message: str, source: str = "", record_id: str = ""):
        super().__init__(message)
        self.message = message
        self.source = source         # which file caused this error
        self.record_id = record_id   # which record, if known

    def __str__(self):
        parts = [self.message]
        if self.source:
            parts.append(f"source='{self.source}'")
        if self.record_id:
            parts.append(f"record_id='{self.record_id}'")
        return " | ".join(parts)


class IngestionError(PipelineException):
    """Raised when a source file cannot be opened or parsed at all."""
    pass


class ValidationError(PipelineException):
    """Raised when a record fails business rule validation."""
    pass


class TransformError(PipelineException):
    """Raised when a transformation (type conversion, enrichment) fails."""
    pass


class LoadError(PipelineException):
    """Raised when writing output files fails."""
    pass


class UnsupportedFormatError(IngestionError):
    """Raised when a file extension has no registered ingester."""
    pass
```

**Why a hierarchy?** Consider this calling code:

```python
try:
    run_pipeline()
except IngestionError:
    # handle source file problems
except TransformError:
    # handle data transformation problems
except PipelineException:
    # catch-all for any other pipeline error
```

Each layer can catch exactly the errors it knows how to handle, and let others bubble up. This is how real production systems handle errors.

---

### Step 3.2 — Build the Audit Logger (`pipeline/audit.py`)

The audit logger is a class because it must maintain state across the entire pipeline run. It collects statistics and writes a summary when the run completes.

```python
# pipeline/audit.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict
import json, os

@dataclass
class SourceStats:
    records_ingested: int = 0
    records_loaded: int = 0
    records_rejected: int = 0
    status: str = "pending"
    error: str = ""

class AuditLogger:
    """
    Tracks pipeline run statistics and writes an audit log entry.

    Implemented as a context manager so it always writes the audit entry,
    even if the pipeline crashes mid-run.

    Usage:
        with AuditLogger(config) as audit:
            audit.record_ingested("north.csv")
            audit.record_loaded("north.csv")
    """

    def __init__(self, config):
        self.config = config
        self.run_id = config.run_id
        self.started_at = datetime.now().isoformat()
        self.finished_at = None
        self.source_stats: Dict[str, SourceStats] = {}
        self.total_delay_days = 0
        self.delay_count = 0
        self._city_origins = {}
        self._city_destinations = {}

    def __enter__(self):
        self._log(f"=== Pipeline Run Started: {self.run_id} ===")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # This runs whether the pipeline succeeded or crashed
        self.finished_at = datetime.now().isoformat()
        if exc_type:
            self._log(f"PIPELINE FAILED: {exc_type.__name__}: {exc_val}")
        else:
            self._log(f"Pipeline completed successfully.")
        self._write_summary()
        return False   # False means: do not suppress the exception

    def init_source(self, source_name: str):
        self.source_stats[source_name] = SourceStats()

    def record_ingested(self, source_name: str, count: int = 1):
        self.source_stats[source_name].records_ingested += count

    def record_loaded(self, source_name: str, count: int = 1):
        self.source_stats[source_name].records_loaded += count

    def record_rejected(self, source_name: str, count: int = 1):
        self.source_stats[source_name].records_rejected += count

    def record_source_success(self, source_name: str):
        self.source_stats[source_name].status = "success"

    def record_source_failure(self, source_name: str, error: str):
        self.source_stats[source_name].status = "failed"
        self.source_stats[source_name].error = error

    def record_enrichment(self, record: dict):
        """Accumulates stats for the summary report."""
        delay = record.get("delivery_delay_days")
        if delay is not None:
            self.total_delay_days += delay
            self.delay_count += 1

        origin = record.get("origin_city", "")
        if origin:
            self._city_origins[origin] = self._city_origins.get(origin, 0) + 1

        dest = record.get("destination_city", "")
        if dest:
            self._city_destinations[dest] = self._city_destinations.get(dest, 0) + 1

    def _log(self, message: str):
        """Appends a timestamped line to the audit log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}\n"
        # 'a' mode = append — never overwrites previous runs
        with open(self.config.audit_log_path, mode='a', encoding='utf-8') as f:
            f.write(line)

    def _write_summary(self):
        """Writes summary.json with aggregated statistics."""
        total_ingested = sum(s.records_ingested for s in self.source_stats.values())
        total_loaded   = sum(s.records_loaded   for s in self.source_stats.values())
        total_rejected = sum(s.records_rejected for s in self.source_stats.values())
        rejection_rate = round((total_rejected / total_ingested * 100), 2) if total_ingested else 0
        avg_delay = round(self.total_delay_days / self.delay_count, 1) if self.delay_count else None
        top_origin = max(self._city_origins, key=self._city_origins.get, default=None)
        top_dest   = max(self._city_destinations, key=self._city_destinations.get, default=None)

        summary = {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total_records_ingested": total_ingested,
            "total_records_loaded": total_loaded,
            "total_records_rejected": total_rejected,
            "rejection_rate_pct": rejection_rate,
            "avg_delivery_delay_days": avg_delay,
            "top_origin_city": top_origin,
            "top_destination_city": top_dest,
            "sources": {
                name: {
                    "records_ingested": s.records_ingested,
                    "records_loaded": s.records_loaded,
                    "records_rejected": s.records_rejected,
                    "status": s.status,
                    **({"error": s.error} if s.error else {})
                }
                for name, s in self.source_stats.items()
            }
        }

        with open(self.config.summary_json_path, mode='w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        self._log(f"Summary written to {self.config.summary_json_path}")
        self._log(f"Total: {total_ingested} ingested | {total_loaded} loaded | {total_rejected} rejected")
```

---

### Step 3.3 — Build the Loader (`pipeline/loaders.py`)

The loader handles writing output files. Notice the careful use of file modes and the pattern of writing headers only once.

```python
# pipeline/loaders.py
import csv, os
from typing import List
from pipeline.exceptions import LoadError

# These are the fields we write to the output CSV — in this order
OUTPUT_FIELDS = [
    "shipment_id", "origin_city", "destination_city",
    "dispatch_date", "delivery_date", "status",
    "weight_kg", "weight_category",
    "delivery_delay_days", "is_delayed",
    "source_file", "ingested_at"
]

class DataLoader:
    """
    Writes cleaned records to the consolidated CSV and
    rejected records to the rejection CSV.

    Uses append mode ('a') so multiple sources accumulate
    into the same output file within one pipeline run.
    Writes CSV headers only when the file is new (empty).
    """

    def __init__(self, config):
        self.config = config
        self._consolidated_initialized = False
        self._rejected_initialized = False

    def write_batch(self, batch: List[dict], source_name: str, audit) -> int:
        """
        Writes one batch of records to consolidated.csv.
        Returns the number of records actually written.
        """
        if not batch:
            return 0

        try:
            file_exists = (
                os.path.exists(self.config.consolidated_csv_path) and
                os.path.getsize(self.config.consolidated_csv_path) > 0
            )
            # 'a' = append mode — adds to existing content
            with open(self.config.consolidated_csv_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction='ignore')
                # Write header only if the file is new
                if not file_exists and not self._consolidated_initialized:
                    writer.writeheader()
                    self._consolidated_initialized = True
                writer.writerows(batch)

            # Update audit stats for each record
            for record in batch:
                audit.record_enrichment(record)

            return len(batch)

        except (OSError, PermissionError) as e:
            raise LoadError(
                f"Failed to write batch to consolidated CSV: {e}",
                source=source_name
            ) from e   # exception chaining — preserves original OSError

    def write_rejected(self, rejected_records: List[dict]):
        """
        Writes invalid records to rejected_records.csv.
        The rejection reason is stored in the '_rejection_reasons' field.
        """
        if not rejected_records:
            return

        rejection_fields = OUTPUT_FIELDS + ["_rejection_reasons"]
        file_exists = (
            os.path.exists(self.config.rejected_csv_path) and
            os.path.getsize(self.config.rejected_csv_path) > 0
        )
        with open(self.config.rejected_csv_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rejection_fields, extrasaction='ignore')
            if not file_exists and not self._rejected_initialized:
                writer.writeheader()
                self._rejected_initialized = True
            writer.writerows(rejected_records)
```

---

### Step 3.4 — Update transformers to use custom exceptions

Go back to `pipeline/transformers.py` and replace the `REJECTION_LOG` approach with proper exception-aware code. Update the `validate_records` function's rejection line to:

```python
# In validate_records(), replace the rejection block with:
if errors:
    from pipeline.exceptions import ValidationError
    record["_rejection_reasons"] = " | ".join(errors)
    # We don't raise — we collect. Raising would stop the pipeline.
    # Instead we yield to a side channel (the rejection list).
    # The caller (main.py) is responsible for routing rejected records.
    record["_is_rejected"] = True
    yield record   # yield ALL records; loader decides what to do with rejected ones
```

This is a subtle but important design decision: the transformer does not decide where rejected records go — it just tags them. The loader routes them to the right file. This separation of concerns is a hallmark of good pipeline design.

---

### Step 3.5 — Wire everything together in `main.py`

```python
# main.py
import os
import glob
from pipeline.config import PipelineConfig
from pipeline.ingesters import get_ingester
from pipeline.transformers import build_pipeline
from pipeline.iterators import ShipmentIterator
from pipeline.loaders import DataLoader
from pipeline.audit import AuditLogger
from pipeline.exceptions import (
    IngestionError, UnsupportedFormatError, LoadError, PipelineException
)

def run_pipeline(input_dir: str, output_dir: str, batch_size: int = 50):
    config = PipelineConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        batch_size=batch_size
    )
    loader = DataLoader(config)

    # Use AuditLogger as a context manager — audit entry always written
    with AuditLogger(config) as audit:
        source_files = glob.glob(os.path.join(input_dir, "*"))

        if not source_files:
            print(f"No files found in {input_dir}")
            return

        for file_path in source_files:
            source_name = os.path.basename(file_path)
            audit.init_source(source_name)
            print(f"\nProcessing: {source_name}")

            try:
                # Step 1: Get the right ingester for this file format
                ingester = get_ingester(file_path)

            except UnsupportedFormatError as e:
                # Unsupported format — skip this file, continue with others
                audit.record_source_failure(source_name, str(e))
                print(f"  SKIPPED: {e}")
                continue   # move to next file

            try:
                # Step 2: Build the lazy pipeline
                raw_records = ingester.ingest()
                pipeline = build_pipeline(raw_records, batch_size=batch_size)
                iterator = ShipmentIterator(pipeline, source_name, file_path)

                total_loaded = 0

                # Step 3: Consume the pipeline batch by batch
                for batch in iterator:
                    # Separate clean records from rejected ones
                    clean = [r for r in batch if not r.get("_is_rejected")]
                    rejected = [r for r in batch if r.get("_is_rejected")]

                    # Write clean records to consolidated CSV
                    if clean:
                        try:
                            written = loader.write_batch(clean, source_name, audit)
                            total_loaded += written
                            audit.record_loaded(source_name, written)
                        except LoadError as e:
                            # Load failure is serious — log it but keep going
                            print(f"  LOAD ERROR: {e}")
                            audit.record_source_failure(source_name, str(e))

                    # Write rejected records to rejection CSV
                    if rejected:
                        loader.write_rejected(rejected)
                        audit.record_rejected(source_name, len(rejected))

                    audit.record_ingested(source_name, len(batch))

                audit.record_source_success(source_name)
                print(f"  Done: {total_loaded} records loaded | {iterator}")

            except IngestionError as e:
                # The file exists but could not be parsed
                audit.record_source_failure(source_name, str(e))
                print(f"  INGESTION ERROR: {e}")
                continue   # skip to the next source file

            except Exception as e:
                # Unexpected error — log it with the original traceback preserved
                audit.record_source_failure(source_name, repr(e))
                print(f"  UNEXPECTED ERROR in {source_name}: {e}")
                raise PipelineException(
                    f"Unexpected error processing {source_name}"
                ) from e   # exception chaining — 'from e' preserves the original

    print(f"\nPipeline complete. Check {output_dir} for outputs.")


if __name__ == "__main__":
    run_pipeline(
        input_dir="data/inputs",
        output_dir="data/outputs",
        batch_size=50
    )
```

---

### Day 3 Summary — What You Learned and Why It Matters

**Custom exception hierarchies are how professionals communicate errors.** When a pipeline crashes at 2 AM and pages an on-call engineer, the first thing they look at is the exception type and message. `IngestionError: malformed XML on line 34 | source='east_region.xml'` tells you exactly what happened and where in seconds. A raw `Exception: 'NoneType' object has no attribute 'strip'` tells you nothing.

**`try/except/else/finally` is not just error handling — it is program structure.** The `finally` block in the `AuditLogger.__exit__` method guarantees the audit log is written whether the pipeline succeeds or crashes. This is the difference between a pipeline that leaves breadcrumbs and one that disappears silently.

**Exception chaining with `raise X from Y` is underused and undervalued.** When you catch a low-level `OSError` and raise a `LoadError`, the `from` keyword attaches the original exception as the cause. This means your stack trace shows both the high-level pipeline context AND the low-level OS reason. Senior engineers look for this pattern.

**The context manager pattern (`__enter__`/`__exit__`) is how Python handles resource lifecycles.** The same pattern behind `with open(...)` is behind database connections, locks, temp files, and your `AuditLogger`. When you understand this pattern, you can make any resource in Python safe to use.

**The key mental model from Day 3:** Failure is not an exception — it is a feature. A production pipeline that cannot handle bad data is not production-grade. Design your pipeline to handle every failure case explicitly, and your 2 AM will be much quieter.

---