# Day 1 — Airflow Setup, DAG Structure & Basic Task Definitions

> **Goal: Stand up Airflow locally using Docker, write your first DAG with four tasks, confirm it appears in the UI with no import errors, and run it manually end-to-end. By end of day, the SwiftShip pipeline runs on a scheduler.**

---

## What You Are Building Today

1. Stand up Airflow locally using `docker-compose`
2. Understand Airflow's core concepts: DAG, Operator, Task, TaskInstance, DagRun
3. Write the `swiftship_daily_pipeline` DAG with four tasks: ingest → transform → load → report
4. Write the Python callable for each task in the `scripts/` directory
5. Run the DAG manually and inspect the logs in the Airflow UI

By end of today, you have a working scheduled pipeline visible in the Airflow web UI.

---

## Step 1 — Stand Up Airflow with Docker

Create `docker-compose.yml` at the project root. This runs Airflow's scheduler, webserver, and metadata database in containers — no local installation needed.

```yaml
# docker-compose.yml

version: '3.8'

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.9.0
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    AIRFLOW__WEBSERVER__SECRET_KEY: 'swiftship-secret-key'
    _PIP_ADDITIONAL_REQUIREMENTS: ''
  volumes:
    - ./dags:/opt/airflow/dags
    - ./plugins:/opt/airflow/plugins
    - ./data:/opt/airflow/data
    - ./scripts:/opt/airflow/scripts
    - ./config:/opt/airflow/config
    - airflow-logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
    volumes:
      - postgres-data:/var/lib/postgresql/data

  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "
        airflow db init &&
        airflow users create
          --username admin
          --password admin
          --firstname Admin
          --lastname User
          --role Admin
          --email admin@swiftship.com
      "

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-data:
  airflow-logs:
```

Start everything:

```bash
docker-compose up -d
# Wait ~60 seconds for init to complete, then visit http://localhost:8080
```

---

## Step 2 — Understand Airflow's Core Concepts

Before writing code, understand these five terms. An interviewer will ask about them.

| Term | What It Is | Analogy |
|---|---|---|
| **DAG** | The workflow definition — tasks and their dependencies. A Python file. | A recipe that lists steps and their order |
| **Operator** | The class that defines what a task does. `PythonOperator`, `BashOperator`, `SqlOperator`. | The type of action (stir, bake, chop) |
| **Task** | An instance of an Operator inside a DAG with a specific `task_id`. | One specific step in the recipe |
| **TaskInstance** | A Task at a specific `execution_date`. Same task run on Jan 10 and Jan 11 are two TaskInstances. | One actual execution of a step on a specific day |
| **DagRun** | One execution of the entire DAG at a specific `execution_date`. Contains all TaskInstances. | One full run of the recipe on a specific day |

**The most important concept:** `execution_date` is NOT when the task runs. It is the logical date the DAG run represents. A DAG scheduled for midnight Jan 11 runs at midnight and processes data logically belonging to Jan 10. This is the `execution_date`. This distinction matters enormously for backfill.

---

## Step 3 — Write the Pipeline Config

```python
# config/pipeline_config.py

from datetime import datetime, timedelta

DAG_CONFIG = {
    "dag_id":            "swiftship_daily_pipeline",
    "schedule_interval": "0 6 * * *",
    "start_date":        datetime(2024, 1, 1),
    "catchup":           True,
    "max_active_runs":   1,
    "tags":              ["swiftship", "etl", "production"],
}

DEFAULT_ARGS = {
    "owner":                    "data-engineering",
    "depends_on_past":          False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure":         False,
    "sla":                      timedelta(hours=2),
}

PATHS = {
    "inputs_dir":  "/opt/airflow/data/inputs",
    "outputs_dir": "/opt/airflow/data/outputs",
    "input_files": [
        "north_region.csv",
        "south_region.json",
        "east_region.xml",
        "west_region.yaml",
        "central_region.txt",
    ]
}
```

---

## Step 4 — Write the Task Callables

Each task in the DAG calls one Python function. Keep these in `scripts/` so they can be tested independently of Airflow.

**`scripts/ingest.py`**

```python
# scripts/ingest.py

import os
import csv
import json
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path


def run_ingestion(inputs_dir: str, outputs_dir: str, execution_date: date) -> dict:
    """
    Ingestion task callable.
    Reads all 5 regional source files and writes a single raw_combined.csv.
    Returns a stats dict that gets pushed to XCom.

    execution_date: the logical date this run represents.
    """
    Path(outputs_dir).mkdir(parents=True, exist_ok=True)
    raw_output = Path(outputs_dir) / "raw_combined.csv"

    records = []
    sources_read = 0
    sources_failed = 0

    # Read CSV
    csv_path = Path(inputs_dir) / "north_region.csv"
    if csv_path.exists():
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["source"] = "north"
                records.append(row)
        sources_read += 1
    else:
        print(f"[Ingest] WARNING: {csv_path} not found. Skipping.")
        sources_failed += 1

    # Read JSON
    json_path = Path(inputs_dir) / "south_region.json"
    if json_path.exists():
        with open(json_path) as f:
            data = json.load(f)
        for row in data:
            row["source"] = "south"
            records.append(row)
        sources_read += 1
    else:
        sources_failed += 1

    # Read XML
    xml_path = Path(inputs_dir) / "east_region.xml"
    if xml_path.exists():
        tree = ET.parse(xml_path)
        for shipment in tree.getroot().findall("shipment"):
            row = {child.tag: child.text for child in shipment}
            row["source"] = "east"
            records.append(row)
        sources_read += 1
    else:
        sources_failed += 1

    # Write combined raw output
    if records:
        fieldnames = list(records[0].keys())
        with open(raw_output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)

    stats = {
        "execution_date": str(execution_date),
        "sources_read":   sources_read,
        "sources_failed": sources_failed,
        "total_records":  len(records),
        "raw_output":     str(raw_output),
    }

    print(f"[Ingest] Completed: {stats}")
    return stats
```

**`scripts/transform.py`**

```python
# scripts/transform.py

import csv
from pathlib import Path
from datetime import date


VALID_STATUSES = {"DELIVERED", "IN_TRANSIT", "PENDING", "RETURNED", "FAILED"}


def run_transform(outputs_dir: str, execution_date: date) -> dict:
    """
    Transform task callable.
    Reads raw_combined.csv, validates and normalises records,
    writes clean_shipments.csv and rejected_records.csv.
    Returns stats dict for XCom.
    """
    raw_path = Path(outputs_dir) / "raw_combined.csv"
    clean_path = Path(outputs_dir) / "clean_shipments.csv"
    rejected_path = Path(outputs_dir) / "rejected_records.csv"

    if not raw_path.exists():
        raise FileNotFoundError(
            f"raw_combined.csv not found at {raw_path}. "
            "Did the ingestion task complete successfully?"
        )

    clean_records = []
    rejected_records = []

    with open(raw_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rejection_reason = None

            # Validate shipment_id
            if not row.get("shipment_id", "").strip():
                rejection_reason = "missing shipment_id"

            # Validate status
            elif row.get("status", "").upper() not in VALID_STATUSES:
                rejection_reason = f"invalid status: {row.get('status')}"

            # Validate weight
            elif row.get("weight_kg"):
                try:
                    w = float(str(row["weight_kg"]).replace("kg", "").strip())
                    if w <= 0 or w > 1000:
                        rejection_reason = f"weight out of range: {w}"
                except ValueError:
                    rejection_reason = f"unparseable weight: {row['weight_kg']}"

            if rejection_reason:
                row["rejection_reason"] = rejection_reason
                rejected_records.append(row)
            else:
                # Normalise
                row["origin_city"] = row.get("origin_city", "").strip().lower()
                row["destination_city"] = row.get("destination_city", "").strip().lower()
                row["status"] = row.get("status", "").strip().upper()
                try:
                    w_str = str(row.get("weight_kg", "")).replace("kg", "").strip()
                    row["weight_kg"] = float(w_str) if w_str else None
                except ValueError:
                    row["weight_kg"] = None
                clean_records.append(row)

    # Write clean
    if clean_records:
        with open(clean_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(clean_records[0].keys()))
            writer.writeheader()
            writer.writerows(clean_records)

    # Write rejected
    if rejected_records:
        with open(rejected_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rejected_records[0].keys()))
            writer.writeheader()
            writer.writerows(rejected_records)

    stats = {
        "execution_date": str(execution_date),
        "clean_count":    len(clean_records),
        "rejected_count": len(rejected_records),
        "clean_output":   str(clean_path),
    }
    print(f"[Transform] Completed: {stats}")
    return stats
```

**`scripts/load.py`** (simplified — calls Project 02 logic)

```python
# scripts/load.py

import csv
import sqlite3
from pathlib import Path
from datetime import date


def run_load(outputs_dir: str, db_path: str, execution_date: date) -> dict:
    """
    Load task callable.
    Reads clean_shipments.csv and upserts into SQLite.
    Returns stats dict for XCom.
    """
    clean_path = Path(outputs_dir) / "clean_shipments.csv"
    if not clean_path.exists():
        raise FileNotFoundError(f"clean_shipments.csv not found at {clean_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id TEXT PRIMARY KEY,
            origin_city TEXT,
            destination_city TEXT,
            dispatch_date TEXT,
            delivery_date TEXT,
            status TEXT,
            weight_kg REAL,
            source TEXT,
            execution_date TEXT,
            loaded_at TEXT DEFAULT (datetime('now'))
        )
    """)

    loaded = 0
    with open(clean_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = [(
            r.get("shipment_id"), r.get("origin_city"), r.get("destination_city"),
            r.get("dispatch_date"), r.get("delivery_date"), r.get("status"),
            r.get("weight_kg"), r.get("source"), str(execution_date)
        ) for r in reader]

    cursor.executemany("""
        INSERT OR REPLACE INTO shipments
            (shipment_id, origin_city, destination_city, dispatch_date,
             delivery_date, status, weight_kg, source, execution_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    loaded = len(rows)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shipments")
    total_in_db = cursor.fetchone()[0]
    conn.close()

    stats = {
        "execution_date": str(execution_date),
        "rows_loaded":    loaded,
        "total_in_db":    total_in_db,
        "db_path":        db_path,
    }
    print(f"[Load] Completed: {stats}")
    return stats
```

**`scripts/report.py`**

```python
# scripts/report.py

import json
from pathlib import Path
from datetime import date, datetime


def run_report(outputs_dir: str, execution_date: date,
               ingest_stats: dict, transform_stats: dict, load_stats: dict) -> None:
    """
    Report task callable.
    Pulls XCom stats from all previous tasks and writes a JSON summary.
    """
    report = {
        "pipeline":        "swiftship_daily_pipeline",
        "execution_date":  str(execution_date),
        "generated_at":    datetime.now().isoformat(),
        "ingestion":       ingest_stats,
        "transformation":  transform_stats,
        "load":            load_stats,
        "data_quality": {
            "total_ingested":  ingest_stats.get("total_records", 0),
            "clean_records":   transform_stats.get("clean_count", 0),
            "rejected_records": transform_stats.get("rejected_count", 0),
            "rejection_rate_pct": round(
                transform_stats.get("rejected_count", 0) /
                max(ingest_stats.get("total_records", 1), 1) * 100, 2
            )
        }
    }

    report_path = Path(outputs_dir) / f"pipeline_report_{execution_date}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[Report] Written to {report_path}")
    print(f"[Report] Quality: {report['data_quality']['rejection_rate_pct']}% rejected")
```

---

## Step 5 — Write the DAG File

```python
# dags/swiftship_pipeline.py

import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from config.pipeline_config import DAG_CONFIG, DEFAULT_ARGS, PATHS
from scripts.ingest import run_ingestion
from scripts.transform import run_transform
from scripts.load import run_load
from scripts.report import run_report

DB_PATH = "/opt/airflow/data/swiftship.db"

# ── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_CONFIG["schedule_interval"],
    start_date=DAG_CONFIG["start_date"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
    description="SwiftShip daily ETL pipeline — ingest, transform, load, report",
) as dag:

    # Task 1 — Ingestion
    ingest_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=lambda **ctx: run_ingestion(
            inputs_dir=PATHS["inputs_dir"],
            outputs_dir=PATHS["outputs_dir"],
            execution_date=ctx["execution_date"].date()
        ),
    )

    # Task 2 — Transform
    transform_task = PythonOperator(
        task_id="run_transform",
        python_callable=lambda **ctx: run_transform(
            outputs_dir=PATHS["outputs_dir"],
            execution_date=ctx["execution_date"].date()
        ),
    )

    # Task 3 — Load
    load_task = PythonOperator(
        task_id="run_load",
        python_callable=lambda **ctx: run_load(
            outputs_dir=PATHS["outputs_dir"],
            db_path=DB_PATH,
            execution_date=ctx["execution_date"].date()
        ),
    )

    # Task 4 — Report
    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=lambda **ctx: run_report(
            outputs_dir=PATHS["outputs_dir"],
            execution_date=ctx["execution_date"].date(),
            ingest_stats=ctx["ti"].xcom_pull(task_ids="run_ingestion"),
            transform_stats=ctx["ti"].xcom_pull(task_ids="run_transform"),
            load_stats=ctx["ti"].xcom_pull(task_ids="run_load"),
        ),
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    ingest_task >> transform_task >> load_task >> report_task
```

---

## Step 6 — Test DAG Integrity

Create `tests/test_dag_integrity.py`. Run this before every commit to catch import errors.

```python
# tests/test_dag_integrity.py

import pytest
from airflow.models import DagBag


def test_dag_loads_without_errors():
    """DAG file must import without syntax errors or missing imports."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, (
        f"DAG import errors: {dag_bag.import_errors}"
    )


def test_dag_exists():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert "swiftship_daily_pipeline" in dag_bag.dags


def test_dag_has_no_cycles():
    """Airflow validates acyclicity on import — if it loaded, it has no cycles."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.dags["swiftship_daily_pipeline"]
    assert dag is not None


def test_task_count():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.dags["swiftship_daily_pipeline"]
    assert len(dag.tasks) == 4


def test_task_order():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.dags["swiftship_daily_pipeline"]

    ingest = dag.get_task("run_ingestion")
    transform = dag.get_task("run_transform")
    load = dag.get_task("run_load")
    report = dag.get_task("generate_report")

    assert "run_transform" in [t.task_id for t in ingest.downstream_list]
    assert "run_load" in [t.task_id for t in transform.downstream_list]
    assert "generate_report" in [t.task_id for t in load.downstream_list]
```

Run: `pytest tests/test_dag_integrity.py -v`

---

## Day 1 Checklist

Before moving to Day 2, confirm:

- [ ] `docker-compose up -d` starts without errors
- [ ] Airflow UI is accessible at `http://localhost:8080`
- [ ] `swiftship_daily_pipeline` appears in the DAGs list with no import errors (red icon = error)
- [ ] Manually triggering the DAG (Trigger DAG button) runs all 4 tasks to success (green)
- [ ] Logs for each task show the printed stats from the callable functions
- [ ] `pytest tests/test_dag_integrity.py` passes all 5 tests
- [ ] `data/outputs/` contains `raw_combined.csv`, `clean_shipments.csv`, and a `pipeline_report_*.json`

---

*Day 2 adds FileSensors to wait for inputs, XComs to pass data between tasks cleanly, and retry logic with exponential backoff.*
