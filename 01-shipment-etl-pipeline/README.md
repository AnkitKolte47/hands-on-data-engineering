# ShipmentETL — A 3-Day Data Engineering Project for Freshers

> **Build a production-style multi-source ETL pipeline in Python, covering OOP, Exception Handling, Generators, Iterators, File Handling, and multi-format Parsing — all in one cohesive project.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — Foundations: OOP, File Handling & Multi-Format Parsing](#Day-1.md)
6. [Day 2 — The Pipeline Engine: Generators & Iterators](#Day-2.md)
7. [Day 3 — Resilience & Output: Exception Handling, Audit & Reporting](#Day-3.md)
8. [How to Run the Full Pipeline](#how-to-run-the-full-pipeline)
9. [Sample Data Files](#sample-data-files)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

You are a junior data engineer at a logistics company called **SwiftShip**.

Regional offices across the country send daily shipment data dumps in whatever file format their legacy systems support — CSV, JSON, XML, YAML, and plain-text logs. Your job is to build a **daily ETL (Extract, Transform, Load) pipeline** that:

- **Extracts** shipment records from 5 different file formats
- **Transforms** them — validates, cleans, normalises, and enriches each record
- **Loads** them into a single consolidated CSV and a JSON summary report
- **Logs** every run with a detailed audit trail
- **Never crashes** — bad data is quarantined, not ignored or thrown away

This is not a toy project. Every design decision in it mirrors what real data engineering teams do at companies like Swiggy, Flipkart, or any logistics startup. By Day 3, you will have a portfolio piece you can demo and explain confidently in any fresher interview.

---

## Why These Concepts Matter in Data Engineering

Before writing a single line of code, understand *why* each concept exists. A senior engineer will always ask you this.

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **OOP** | Different data sources need different parsing logic — classes let you swap sources without rewriting the pipeline | A single 800-line script that breaks if one format changes |
| **File Handling** | You must read gigabytes of data safely, handle missing files, and write output atomically | Files get corrupted, partial writes happen, errors are silent |
| **Multi-format Parsing** | Real-world data never arrives in one format | You write one-off scripts per format, duplicating logic |
| **Generators** | Shipment files can have millions of records — you cannot load them all into RAM | Your pipeline crashes on large files with `MemoryError` |
| **Iterators** | You need a clean, reusable interface to walk through records without caring about the source | Tight coupling between ingestion and transformation code |
| **Exception Handling** | One corrupt file from one office should not stop the entire pipeline | A single bad record kills the night run and you get paged at 2 AM |

---

## Project Architecture Overview

```
5 Source Files (CSV, JSON, XML, YAML, TXT)
        │
        ▼
┌─────────────────────────────────────┐
│  INGESTION LAYER  (Day 1)           │
│  BaseIngester → 5 subclasses        │
│  File Handling + Multi-format Parse │
└──────────────┬──────────────────────┘
               │ raw records (dicts)
               ▼
┌─────────────────────────────────────┐
│  TRANSFORM PIPELINE  (Day 2)        │
│  validate() → normalize()           │
│  → enrich() → batch()               │
│  Generator chain — 100% lazy        │
└──────────────┬──────────────────────┘
               │ clean batches
               ▼
┌─────────────────────────────────────┐
│  LOAD + AUDIT LAYER  (Day 3)        │
│  Write consolidated.csv             │
│  Write summary.json                 │
│  Write rejected_records.csv         │
│  Append to audit.log                │
└─────────────────────────────────────┘
```

---

## Folder Structure

Create this structure before you begin Day 1:

```
shipment_etl/
│
├── data/
│   ├── inputs/
│   │   ├── north_region.csv
│   │   ├── south_region.json
│   │   ├── east_region.xml
│   │   ├── west_region.yaml
│   │   └── central_region.txt
│   └── outputs/               ← pipeline creates this
│       ├── consolidated.csv
│       ├── summary.json
│       ├── rejected_records.csv
│       └── audit.log
│
├── pipeline/
│   ├── __init__.py
│   ├── config.py              ← Day 1
│   ├── ingesters.py           ← Day 1
│   ├── transformers.py        ← Day 2
│   ├── iterators.py           ← Day 2
│   ├── exceptions.py          ← Day 3
│   ├── loaders.py             ← Day 3
│   └── audit.py               ← Day 3
│
└── main.py                    ← Day 3
```
---

## How to Run the Full Pipeline

### Prerequisites

```bash
pip install pyyaml
```

### Run

```bash
cd shipment_etl
python main.py
```

### Expected outputs in `data/outputs/`

| File | Description |
|---|---|
| `consolidated.csv` | All valid records from all sources, cleaned and enriched |
| `summary.json` | KPIs and per-source stats for this run |
| `rejected_records.csv` | All invalid records with rejection reasons |
| `audit.log` | Timestamped log of every pipeline run (appended, never overwritten) |

---

## Sample Data Files

Use these to test your pipeline.

### `data/inputs/north_region.csv`
```
shipment_id,origin_city,destination_city,dispatch_date,delivery_date,status,weight_kg
SH001,mumbai,delhi,2024-01-10,2024-01-13,DELIVERED,12.5
SH002,pune,bangalore,2024-01-11,2024-01-15,IN_TRANSIT,8.2
SH003,chennai,hyderabad,2024-01-12,,PENDING,
```

### `data/inputs/south_region.json`
```json
[
  {"shipment_id": "SH101", "origin_city": "Bangalore", "destination_city": "Chennai",
   "dispatch_date": "12/01/2024", "delivery_date": "15/01/2024",
   "status": "DELIVERED", "weight_kg": "5.3kg"},
  {"shipment_id": "SH102", "origin_city": "Hyderabad", "destination_city": "Pune",
   "dispatch_date": "13/01/2024", "delivery_date": "17/01/2024",
   "status": "RETURNED", "weight_kg": "22.0"}
]
```

### `data/inputs/east_region.xml`
```xml
<shipments>
  <shipment>
    <shipment_id>SH201</shipment_id>
    <origin_city>Kolkata</origin_city>
    <destination_city>Delhi</destination_city>
    <dispatch_date>2024-01-10</dispatch_date>
    <delivery_date>2024-01-14</delivery_date>
    <status>DELIVERED</status>
    <weight_kg>9.1</weight_kg>
  </shipment>
</shipments>
```

### `data/inputs/west_region.yaml`
```yaml
- shipment_id: SH301
  origin_city: Ahmedabad
  destination_city: Mumbai
  dispatch_date: "2024-01-11"
  delivery_date: "2024-01-12"
  status: DELIVERED
  weight_kg: 3.5
- shipment_id: SH302
  origin_city: Surat
  destination_city: Pune
  dispatch_date: "2024-01-12"
  delivery_date: "2024-01-16"
  status: IN_TRANSIT
  weight_kg: 18.0
```

### `data/inputs/central_region.txt`
```
# Central region daily shipment log
[2024-01-10] ORDER_ID=SH401 STATUS=DELIVERED WEIGHT=7.2kg ORIGIN=Nagpur DEST=Mumbai
[2024-01-11] ORDER_ID=SH402 STATUS=IN_TRANSIT WEIGHT=14.5kg ORIGIN=Bhopal DEST=Delhi
[2024-01-12] ORDER_ID=SH403 STATUS=PENDING WEIGHT=2.1kg ORIGIN=Indore DEST=Pune
```

---

## Interview Cheat Sheet

Use these when an interviewer asks you to explain your project.

**"Why did you use an abstract class for BaseIngester?"**
> Because I wanted to enforce a contract. Any new ingester subclass must implement `ingest()` — Python raises a TypeError at class definition time if it doesn't. This prevents future developers from accidentally creating an ingester that silently does nothing.

**"What is a generator and why did you use them?"**
> A generator is a function that uses `yield` instead of `return`. It produces one value at a time and pauses between values. I used them because shipment files can be very large — loading all records into a list would consume too much memory. With generators, memory usage is constant regardless of file size.

**"How is a generator different from an iterator?"**
> A generator function is the easy way to create an iterator. An iterator is any object with `__iter__` and `__next__` methods. My `ShipmentIterator` class is an iterator — I wrote it as a class because I needed to attach state (how many records yielded, source name) alongside the data stream.

**"What happens when a file is corrupted in your pipeline?"**
> The ingester raises an `IngestionError`. In `main.py`, that error is caught in the `except IngestionError` block — the source file is logged as failed in the audit, and the loop continues to the next source file. The pipeline never crashes because of one bad file.

**"Why did you use `raise LoadError(...) from e` instead of just `raise LoadError(...)`?"**
> Exception chaining. The `from e` attaches the original `OSError` as the cause of the `LoadError`. When someone reads the stack trace, they see both the high-level context (LoadError: failed to write consolidated CSV) AND the low-level reason (OSError: No space left on device). Without `from e`, the original cause is lost.

**"Why does `AuditLogger` use `__enter__` and `__exit__`?"**
> To guarantee the audit entry is always written — even if the pipeline crashes. The `__exit__` method is called by the `with` statement whether the block exits normally or via an exception. It's the same pattern as `with open(...)` — the file is always closed, the audit is always written.

---

*Built for freshers entering data engineering. Every pattern in this project appears daily in production pipelines at real companies.*