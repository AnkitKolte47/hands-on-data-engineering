## Day 2

# Day 2 — The Pipeline Engine: Generators & Iterators

**Goal:** Build the streaming transformation engine. By end of Day 2, records from Day 1's ingesters will flow lazily through a chain of generator functions — validated, normalised, enriched, and batched — without ever loading the full dataset into memory.

**Concepts practiced:** Generators (`yield`), generator pipelines (chaining), custom Iterator class (`__iter__`, `__next__`), `itertools`

**Estimated time:** 4–5 hours

---

### Step 2.1 — Understand lazy evaluation first (read before coding)

Here is the most important concept of Day 2. Consider two approaches to reading 1 million records:

**Approach A — Eager (bad for large data):**
```python
def get_all_records(file):
    records = []              # allocates memory for ALL records at once
    for row in file:
        records.append(row)
    return records            # returns only when EVERYTHING is loaded
```

**Approach B — Lazy with generators (correct for data engineering):**
```python
def stream_records(file):
    for row in file:
        yield row             # produces ONE record, pauses, waits for next()
                              # memory usage stays constant regardless of file size
```

A generator function returns a generator object. It does not run the body of the function when called — it runs the body one step at a time, each time `next()` is called on it. This is called **lazy evaluation**.

In data engineering, datasets can be hundreds of gigabytes. Generators are not optional — they are the standard.

---

### Step 2.2 — Build the transformer chain (`pipeline/transformers.py`)

Each transformer is a generator function that accepts an iterable of records and yields transformed records. They chain together like a Unix pipe.

```python
# pipeline/transformers.py
from datetime import datetime
import re
from typing import Iterator

REQUIRED_FIELDS = {"shipment_id", "status", "origin_city", "destination_city"}
ALLOWED_STATUSES = {"DELIVERED", "IN_TRANSIT", "PENDING", "RETURNED", "FAILED"}
REJECTION_LOG = []   # Day 3 will replace this with a proper file writer

# ─────────────────────────────────────────────
# TRANSFORMER 1: Validate
# ─────────────────────────────────────────────
def validate_records(records: Iterator[dict]) -> Iterator[dict]:
    """
    Checks that each record has required fields and valid values.
    Valid records are yielded. Invalid records are collected for rejection log.
    This generator NEVER raises an exception — it quarantines bad records.

    Why: In production, you never want the pipeline to crash because one
    office sent a file with missing columns. Quarantine and continue.
    """
    for record in records:
        errors = []

        # Check required fields exist and are non-empty
        for field in REQUIRED_FIELDS:
            if field not in record or not record[field] or str(record[field]).strip() == "":
                errors.append(f"Missing required field: '{field}'")

        # Check status is a known value
        status = str(record.get("status", "")).strip().upper()
        if status and status not in ALLOWED_STATUSES:
            errors.append(f"Unknown status value: '{status}'")

        if errors:
            # Tag the record with rejection reasons and hand it to the rejection collector
            record["_rejection_reasons"] = " | ".join(errors)
            REJECTION_LOG.append(record)
        else:
            yield record  # only valid records continue downstream


# ─────────────────────────────────────────────
# TRANSFORMER 2: Normalize
# ─────────────────────────────────────────────
def normalize_records(records: Iterator[dict]) -> Iterator[dict]:
    """
    Standardises field formats across all source files.
    Different offices use different date formats, casings, and weight units.
    This transformer makes everything consistent before enrichment.
    """
    def parse_date(raw: str) -> str:
        """Try multiple date formats and return ISO YYYY-MM-DD."""
        if not raw:
            return ""
        formats = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d-%b-%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return str(raw).strip()  # return as-is if no format matches

    def parse_weight(raw: str) -> float:
        """Strip units like 'kg', 'lbs' and return a float."""
        if not raw:
            return 0.0
        cleaned = re.sub(r'[^\d.]', '', str(raw))
        return float(cleaned) if cleaned else 0.0

    for record in records:
        record["shipment_id"]       = str(record.get("shipment_id", "")).strip().upper()
        record["status"]            = str(record.get("status", "")).strip().upper()
        record["origin_city"]       = str(record.get("origin_city", "")).strip().title()
        record["destination_city"]  = str(record.get("destination_city", "")).strip().title()
        record["dispatch_date"]     = parse_date(record.get("dispatch_date", ""))
        record["delivery_date"]     = parse_date(record.get("delivery_date", ""))
        record["weight_kg"]         = parse_weight(record.get("weight_kg", record.get("weight", "")))
        yield record


# ─────────────────────────────────────────────
# TRANSFORMER 3: Enrich
# ─────────────────────────────────────────────
def enrich_records(records: Iterator[dict]) -> Iterator[dict]:
    """
    Adds derived fields that do not exist in the raw source data.
    This is where business logic lives — turning raw data into insights.

    Derived fields:
    - delivery_delay_days: how many days after dispatch was it delivered
    - is_delayed: boolean flag for quick filtering
    - weight_category: Light / Medium / Heavy based on weight_kg
    """
    for record in records:
        # Calculate delivery delay
        try:
            dispatch = datetime.strptime(record["dispatch_date"], "%Y-%m-%d")
            delivery = datetime.strptime(record["delivery_date"], "%Y-%m-%d")
            delay = (delivery - dispatch).days
            record["delivery_delay_days"] = delay
            record["is_delayed"] = delay > 3   # business rule: > 3 days is a delay
        except (ValueError, KeyError):
            record["delivery_delay_days"] = None
            record["is_delayed"] = None

        # Categorise weight
        weight = record.get("weight_kg", 0)
        if weight < 5:
            record["weight_category"] = "Light"
        elif weight < 20:
            record["weight_category"] = "Medium"
        else:
            record["weight_category"] = "Heavy"

        yield record


# ─────────────────────────────────────────────
# TRANSFORMER 4: Batch
# ─────────────────────────────────────────────
def batch_records(records: Iterator[dict], size: int = 50) -> Iterator[list]:
    """
    Groups records into lists of `size` for bulk writing.
    Bulk writes are dramatically faster than writing one row at a time —
    this is a standard pattern in data loading.

    Note: this generator yields LISTS, not individual dicts.
    The downstream loader receives a list of records and writes them all at once.
    """
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) >= size:
            yield batch
            batch = []     # reset for next batch
    if batch:
        yield batch        # yield the final partial batch


# ─────────────────────────────────────────────
# Pipeline composer
# ─────────────────────────────────────────────
def build_pipeline(raw_records: Iterator[dict], batch_size: int = 50) -> Iterator[list]:
    """
    Composes all transformers into a single pipeline.
    No data moves until something iterates over the returned generator.
    This is the power of lazy evaluation.
    """
    validated  = validate_records(raw_records)
    normalised = normalize_records(validated)
    enriched   = enrich_records(normalised)
    batched    = batch_records(enriched, size=batch_size)
    return batched
```

---

### Step 2.3 — Build the custom Iterator class (`pipeline/iterators.py`)

Generators are powerful, but sometimes you need a class-based iterator — for instance, when you need to add state, support indexing, or expose metadata alongside the data stream.

**Generators vs Iterators — the key distinction:**
- A **generator function** uses `yield` and is the easiest way to create an iterator
- An **iterator class** implements `__iter__` and `__next__` manually — giving you full control

Both produce the same result when used in a `for` loop. Knowing both is an interview staple.

```python
# pipeline/iterators.py
from typing import Iterator
import os

class ShipmentIterator:
    """
    A class-based iterator that wraps the batched pipeline output.

    Implements the full iterator protocol:
    - __iter__: makes the object usable in a for loop
    - __next__: returns the next batch, raises StopIteration when done
    - __len__:  estimates total record count from file size
    - __repr__: gives a useful string representation for debugging

    Why a class here instead of just a generator?
    Because we want to attach metadata (source, estimated count)
    to the iterator itself — a generator function cannot hold state.
    """

    def __init__(self, pipeline: Iterator, source_name: str, file_path: str = ""):
        self._pipeline = pipeline
        self.source_name = source_name
        self.file_path = file_path
        self.batches_yielded = 0
        self.records_yielded = 0
        self._exhausted = False

    def __iter__(self):
        # __iter__ must return an iterator — 'self' is its own iterator
        return self

    def __next__(self):
        if self._exhausted:
            raise StopIteration

        try:
            batch = next(self._pipeline)
            self.batches_yielded += 1
            self.records_yielded += len(batch)
            return batch
        except StopIteration:
            self._exhausted = True
            raise   # re-raise StopIteration to signal end of iteration

    def __len__(self):
        """
        Estimates record count from file size.
        This is an approximation — useful for progress reporting.
        In real pipelines, this can be a COUNT query to a database.
        """
        if not self.file_path or not os.path.exists(self.file_path):
            return 0
        size_bytes = os.path.getsize(self.file_path)
        avg_record_size = 200   # rough estimate: 200 bytes per record
        return max(1, size_bytes // avg_record_size)

    def __repr__(self):
        return (
            f"ShipmentIterator("
            f"source='{self.source_name}', "
            f"batches_yielded={self.batches_yielded}, "
            f"records_yielded={self.records_yielded})"
        )
```

---

### Step 2.4 — Verify Day 2 works

```python
# test_day2.py  (delete after verification)
from pipeline.ingesters import get_ingester
from pipeline.transformers import build_pipeline
from pipeline.iterators import ShipmentIterator

file_path = "data/inputs/north_region.csv"
ingester = get_ingester(file_path)
raw = ingester.ingest()
pipeline = build_pipeline(raw, batch_size=5)
iterator = ShipmentIterator(pipeline, source_name="north_region.csv", file_path=file_path)

print(f"Iterator: {iterator}")
print(f"Estimated records: {len(iterator)}\n")

for batch in iterator:
    print(f"Received batch of {len(batch)} records")
    print(f"First record keys: {list(batch[0].keys())}")
    break  # just check the first batch
```

---

### Day 2 Summary — What You Learned and Why It Matters

**Generators solve the biggest practical problem in data engineering: memory.** A list loads everything into RAM before you can process anything. A generator produces one item at a time — it does not matter if the file has 100 rows or 100 million. Your pipeline uses the same amount of memory either way. Every major data framework (Spark, Pandas chunking, Kafka consumers) is built on this same principle.

**Chained generators are a pipeline.** `batch_records(enrich_records(normalize_records(validate_records(raw))))` — this reads exactly like a Unix pipe and works the same way. No record is processed until the loader asks for the next batch. This is called **lazy evaluation** and it is one of the most powerful ideas in computer science.

**Class-based iterators give you control.** When you need to attach state (how many records yielded?), expose metadata (what source is this from?), or support `len()`, you need a class. Generators are convenient. Iterators are flexible. Knowing when to use each one is a sign of engineering maturity.

**The key mental model from Day 2:** Data should flow like a stream, not be collected like a lake. Never load data you don't need. Process it as it arrives.

---