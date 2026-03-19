# Day 2 — Task Dependencies, XComs, Sensors & Retry Logic

> **Goal: Add FileSensors that wait for input files before starting, wire XComs explicitly to pass data between tasks, configure retries with exponential backoff, and simulate a failure to verify the retry mechanism works. By end of day, the pipeline handles late data and transient failures without human intervention.**

---

## What You Are Building Today

Day 1 gave you a working DAG. Today you make it production-hardened:

1. Add a `FileSensor` for each regional input file — the pipeline waits until all files exist
2. Group sensors to run in parallel using `TaskGroup`
3. Explicitly use XCom `push` and `pull` instead of relying on return value auto-push
4. Add a retry simulation task to verify exponential backoff
5. Understand and configure `depends_on_past`, `wait_for_downstream`, and `trigger_rule`

---

## Step 1 — Understand FileSensors

A `FileSensor` polls a file path at a regular interval. If the file exists, the sensor succeeds and the downstream tasks proceed. If it does not exist by the timeout, the sensor fails.

Why does the SwiftShip pipeline need this? Regional offices send files by 5 AM, but occasionally a feed is 30–90 minutes late. Without a sensor, Airflow starts ingestion at 6 AM, finds no files, and marks the run as failed. With sensors, it waits — up to a configured timeout — and proceeds as soon as files arrive.

```python
# Add to dags/swiftship_pipeline.py

from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

# Inside the `with DAG(...) as dag:` block, before ingest_task:

with TaskGroup(group_id="wait_for_files") as wait_group:
    sensors = []
    for filename in PATHS["input_files"]:
        sensor = FileSensor(
            task_id=f"wait_for_{filename.replace('.', '_').replace('-', '_')}",
            filepath=f"{PATHS['inputs_dir']}/{filename}",
            poke_interval=60,       # check every 60 seconds
            timeout=60 * 90,        # fail if file not found within 90 minutes
            mode="reschedule",      # release the worker slot while waiting
            soft_fail=False,        # hard fail if timeout exceeded
        )
        sensors.append(sensor)
```

**`mode="reschedule"` vs `mode="poke"` — critical difference:**

| Mode | Behaviour | When to use |
|---|---|---|
| `poke` | Worker slot stays occupied while waiting | Short waits (< 5 min). Wastes a slot. |
| `reschedule` | Releases the worker slot between pokes | Long waits (> 5 min). Correct for production file waits. |

Always use `reschedule` for file sensors. `poke` mode on a 90-minute timeout ties up a worker for 90 minutes.

---

## Step 2 — Wire Sensors Into the DAG

Update `dags/swiftship_pipeline.py` with the full sensor → pipeline chain:

```python
# dags/swiftship_pipeline.py  (full updated version)

import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from config.pipeline_config import DAG_CONFIG, DEFAULT_ARGS, PATHS
from scripts.ingest import run_ingestion
from scripts.transform import run_transform
from scripts.load import run_load
from scripts.report import run_report

DB_PATH = "/opt/airflow/data/swiftship.db"

with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_CONFIG["schedule_interval"],
    start_date=DAG_CONFIG["start_date"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
    description="SwiftShip daily ETL pipeline with sensors and XComs",
) as dag:

    # ── Sensors: run in parallel, all must succeed before ingestion ──────────
    with TaskGroup(group_id="wait_for_files") as wait_group:
        for filename in PATHS["input_files"]:
            FileSensor(
                task_id=f"wait_{filename.split('.')[0]}",
                filepath=f"{PATHS['inputs_dir']}/{filename}",
                poke_interval=60,
                timeout=60 * 90,
                mode="reschedule",
            )

    # ── Pipeline tasks ────────────────────────────────────────────────────────

    def ingest(**context):
        stats = run_ingestion(
            inputs_dir=PATHS["inputs_dir"],
            outputs_dir=PATHS["outputs_dir"],
            execution_date=context["execution_date"].date()
        )
        # Explicit XCom push — makes the push visible and intentional
        context["ti"].xcom_push(key="ingest_stats", value=stats)
        return stats  # also auto-pushed under key="return_value"

    def transform(**context):
        stats = run_transform(
            outputs_dir=PATHS["outputs_dir"],
            execution_date=context["execution_date"].date()
        )
        context["ti"].xcom_push(key="transform_stats", value=stats)
        return stats

    def load(**context):
        stats = run_load(
            outputs_dir=PATHS["outputs_dir"],
            db_path=DB_PATH,
            execution_date=context["execution_date"].date()
        )
        context["ti"].xcom_push(key="load_stats", value=stats)
        return stats

    def report(**context):
        ti = context["ti"]
        run_report(
            outputs_dir=PATHS["outputs_dir"],
            execution_date=context["execution_date"].date(),
            ingest_stats=ti.xcom_pull(task_ids="run_ingestion", key="ingest_stats"),
            transform_stats=ti.xcom_pull(task_ids="run_transform", key="transform_stats"),
            load_stats=ti.xcom_pull(task_ids="run_load", key="load_stats"),
        )

    ingest_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=ingest,
    )

    transform_task = PythonOperator(
        task_id="run_transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="run_load",
        python_callable=load,
        retries=5,                     # override default for the most critical task
        retry_delay=__import__("datetime").timedelta(minutes=2),
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=report,
        trigger_rule="all_done",       # run report even if load had retries (but succeeded)
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    wait_group >> ingest_task >> transform_task >> load_task >> report_task
```

---

## Step 3 — Understand XComs in Depth

XComs are stored in Airflow's metadata database. Every `ti.xcom_push(key, value)` call writes to the `xcom` table with:

- `dag_id`
- `run_id`
- `task_id`
- `key`
- `value` (JSON-serialised)

When `ti.xcom_pull(task_ids="run_ingestion", key="ingest_stats")` is called, Airflow queries this table for the matching row.

**What XComs are NOT for:**

```python
# WRONG — never do this
context["ti"].xcom_push(key="dataframe", value=df.to_json())  # could be 500MB
context["ti"].xcom_push(key="records", value=list_of_10000_dicts)
```

XComs are limited by the metadata database's row size (typically 64KB–1MB). For large data, write to S3/GCS/local disk and XCom the path:

```python
# CORRECT — XCom the path, not the data
context["ti"].xcom_push(key="clean_csv_path", value="/opt/airflow/data/outputs/clean.csv")
```

---

## Step 4 — Simulate and Verify Retry Logic

Add a task that fails intentionally on the first two attempts, then succeeds. This verifies that retry + exponential backoff works before a real failure occurs.

```python
# scripts/flaky_task.py

import os
import tempfile


def run_flaky_task(**context):
    """
    Simulates a task that fails twice then succeeds.
    Uses a temp file to count attempts across retries.
    In production this represents a transient DB connection failure.
    """
    attempt_file = f"/tmp/airflow_flaky_{context['run_id'].replace(':', '_')}.txt"

    attempt = 1
    if os.path.exists(attempt_file):
        with open(attempt_file) as f:
            attempt = int(f.read().strip()) + 1

    with open(attempt_file, "w") as f:
        f.write(str(attempt))

    print(f"[FlakyTask] Attempt {attempt}")

    if attempt < 3:
        raise ConnectionError(
            f"Simulated transient DB connection failure on attempt {attempt}. "
            "Airflow will retry with exponential backoff."
        )

    print(f"[FlakyTask] Success on attempt {attempt}. Cleaning up.")
    os.remove(attempt_file)
    return {"attempts_needed": attempt}
```

Add it to the DAG temporarily:

```python
# Add inside the `with DAG(...)` block for testing

from scripts.flaky_task import run_flaky_task

flaky = PythonOperator(
    task_id="flaky_connection_test",
    python_callable=run_flaky_task,
    retries=4,
    retry_delay=__import__("datetime").timedelta(seconds=10),
    retry_exponential_backoff=True,
)

# Add after wait_group but before ingest:
wait_group >> flaky >> ingest_task
```

Trigger the DAG manually. In the Airflow UI, watch `flaky_connection_test`:
- It turns **red** (failed) twice
- Then turns **green** (succeeded on attempt 3)
- The log shows "Attempt 1", "Attempt 2", "Attempt 3" with exponential wait times between

---

## Step 5 — Understand trigger_rule

The `report_task` uses `trigger_rule="all_done"`. Understand all trigger rules:

| trigger_rule | Task runs when... | Use case |
|---|---|---|
| `all_success` | All upstream tasks succeeded | Default — normal pipelines |
| `all_done` | All upstream tasks finished (success or failure) | Cleanup/reporting tasks that must always run |
| `one_failed` | At least one upstream task failed | Failure handling, alerting |
| `none_failed` | No upstream tasks failed (skipped is OK) | Conditional branches |
| `all_failed` | All upstream tasks failed | Last-resort recovery |

The report task uses `all_done` because you want a report even when load fails — the report documents the failure. If it used `all_success` (the default), it would skip when load fails, and you would lose the run's statistics.

---

## Step 6 — Add the Custom FileSensor with Content Check

The standard `FileSensor` only checks if the file exists. A file may exist but be empty (the process writing it crashed). Create a sensor that also checks for minimum file size.

```python
# dags/sensors.py

from airflow.sensors.filesystem import FileSensor
from airflow.utils.context import Context
import os


class NonEmptyFileSensor(FileSensor):
    """
    FileSensor that also verifies the file is non-empty.
    A file might exist but be 0 bytes if the upstream process crashed mid-write.
    """

    def __init__(self, min_size_bytes: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.min_size_bytes = min_size_bytes

    def poke(self, context: Context) -> bool:
        # First check: does the file exist?
        if not super().poke(context):
            return False

        # Second check: is it large enough?
        size = os.path.getsize(self.filepath)
        if size < self.min_size_bytes:
            self.log.warning(
                f"File {self.filepath} exists but is only {size} bytes "
                f"(minimum: {self.min_size_bytes}). Waiting..."
            )
            return False

        self.log.info(f"File {self.filepath} found with {size} bytes. Sensor passed.")
        return True
```

Use `NonEmptyFileSensor` instead of `FileSensor` in the DAG:

```python
from dags.sensors import NonEmptyFileSensor

# Inside TaskGroup:
NonEmptyFileSensor(
    task_id=f"wait_{filename.split('.')[0]}",
    filepath=f"{PATHS['inputs_dir']}/{filename}",
    min_size_bytes=50,
    poke_interval=60,
    timeout=60 * 90,
    mode="reschedule",
)
```

---

## Day 2 Checklist

Before moving to Day 3, confirm:

- [ ] `wait_for_files` TaskGroup appears in the DAG graph with 5 parallel sensors
- [ ] All sensors run in `reschedule` mode (verify in task log: "Task is in reschedule mode")
- [ ] Remove one input file, trigger the DAG, and confirm the corresponding sensor fails after timeout
- [ ] XCom values are visible in the UI: Task detail → XComs tab shows `ingest_stats`, `transform_stats`, `load_stats`
- [ ] `report_task` correctly pulls all three XCom values (verify in the JSON output file)
- [ ] `flaky_connection_test` fails twice then succeeds — visible in the task log and graph
- [ ] `generate_report` runs with `trigger_rule="all_done"` even when a simulated upstream failure occurs

---

*Day 3 completes the project with backfill demonstration, SLA enforcement, and Slack failure notifications.*
