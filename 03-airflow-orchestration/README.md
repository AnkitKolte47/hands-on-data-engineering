# AirflowOrchestrator — A 3-Day Data Engineering Project

> **Automate the SwiftShip pipeline with Apache Airflow — covering DAG design, task dependencies, XComs, sensors, retry logic, backfill, and SLA alerting — everything a production pipeline scheduler needs.**

---

## Table of Contents

1. [What Is This Project?](#what-is-this-project)
2. [Why These Concepts Matter in Data Engineering](#why-these-concepts-matter-in-data-engineering)
3. [Project Architecture Overview](#project-architecture-overview)
4. [Folder Structure](#folder-structure)
5. [Day 1 — Airflow Setup, DAG Structure & Basic Task Definitions](Day-1.md)
6. [Day 2 — Task Dependencies, XComs, Sensors & Retry Logic](Day-2.md)
7. [Day 3 — Backfill, SLA Alerts & Slack Notifications](Day-3.md)
8. [How to Run the Full Project](#how-to-run-the-full-project)
9. [Sample DAG Configuration](#sample-dag-configuration)
10. [Interview Cheat Sheet](#interview-cheat-sheet)

---

## What Is This Project?

You are still at **SwiftShip**, and the pipeline is working.

The problem: someone has to manually run `python main.py` every night. If they forget — or the server reboots — no data is loaded. The analytics team queries stale data and nobody knows the pipeline failed until they see yesterday's numbers in today's dashboard.

Your manager gives you one requirement: **automate it.**

You will build an Airflow DAG (Directed Acyclic Graph) that:

- **Runs automatically** every day at 6 AM, without human intervention
- **Waits** for all five regional input files to arrive before starting (sensors)
- **Runs pipeline stages as separate tasks** — ingest, transform, load — so a failure in one stage is visible and retryable independently
- **Retries automatically** on transient failures (network blip, DB connection timeout) with exponential backoff
- **Sends a Slack alert** when a task fails, with a direct link to the failed task's logs
- **Enforces SLA** — if the full pipeline has not completed by 8 AM, the on-call engineer is alerted
- **Supports backfill** — if the pipeline missed three days, Airflow can re-run those exact runs in the right order with the right execution dates

This is the difference between a pipeline that runs and a pipeline that operates.

---

## Why These Concepts Matter in Data Engineering

| Concept | Real Problem It Solves | What Happens Without It |
|---|---|---|
| **DAGs** | Define task execution order as a graph — tasks run in the right sequence, in parallel where possible | Manual scripts with `subprocess.call()` chains that fail silently and have no retry |
| **Sensors** | Wait for upstream data to arrive before starting — files may be late | Pipeline starts, finds no input files, marks run as success with zero rows loaded |
| **XComs** | Pass small values (row counts, file paths, run metadata) between tasks without writing to disk | Tasks cannot communicate — each task is isolated and cannot report results to downstream tasks |
| **Retries + Backoff** | Transient failures (DB timeouts, network blips) are retried automatically | One dropped DB connection at 3 AM fails the entire pipeline; on-call is paged unnecessarily |
| **SLA** | Guarantee that downstream consumers have data by a committed time | Dashboards load with no data; analysts report a bug; trust in the data platform erodes |
| **Backfill** | Re-run the pipeline for historical dates using the correct execution date context | Manually re-running scripts for missed dates with hardcoded dates — error-prone and undocumented |
| **Alerts** | Notify the right person immediately when a task fails, with context | Failures are discovered hours later when a stakeholder notices stale data |

---

## Project Architecture Overview

```
Airflow Scheduler (runs at 06:00 daily)
        │
        ▼
┌─────────────────────────────────────┐
│  swiftship_daily_pipeline DAG       │
│                                     │
│  [wait_for_files]  ← FileSensors    │
│        │                            │
│        ▼                            │
│  [run_ingestion]   ← PythonOperator │
│        │  XCom: row counts          │
│        ▼                            │
│  [run_transform]   ← PythonOperator │
│        │  XCom: rejected count      │
│        ▼                            │
│  [run_load]        ← PythonOperator │
│        │  XCom: db row count        │
│        ▼                            │
│  [generate_report] ← PythonOperator │
│        │                            │
│        ▼                            │
│  [notify_success]  ← SlackOperator  │
│                                     │
│  SLA: all tasks must complete       │
│       by 08:00 or alert fires       │
└─────────────────────────────────────┘
        │ on any task failure
        ▼
  [on_failure_callback] → Slack alert with task name + log URL
```

---

## Folder Structure

```
airflow_orchestration/
│
├── dags/
│   ├── swiftship_pipeline.py        ← Day 1: main DAG file
│   ├── callbacks.py                 ← Day 3: failure/SLA callbacks
│   └── sensors.py                  ← Day 2: custom file sensor
│
├── plugins/
│   └── swiftship_operators.py       ← Day 2: custom operators (optional)
│
├── config/
│   ├── pipeline_config.py           ← Day 1: paths, schedule, settings
│   └── slack_config.py              ← Day 3: Slack webhook config
│
├── scripts/
│   ├── ingest.py                    ← Day 1: callable for ingestion task
│   ├── transform.py                 ← Day 1: callable for transform task
│   ├── load.py                      ← Day 1: callable for load task
│   └── report.py                    ← Day 1: callable for report task
│
├── data/
│   ├── inputs/                      ← regional source files land here
│   └── outputs/                     ← pipeline writes here
│
├── tests/
│   └── test_dag_integrity.py        ← Day 1: DAG import and cycle check
│
└── docker-compose.yml               ← Day 1: local Airflow setup
```

---

## How to Run the Full Project

### Prerequisites

```bash
pip install apache-airflow==2.9.0 apache-airflow-providers-slack
# Or use Docker (recommended):
docker-compose up -d
```

### Access the Airflow UI

```
http://localhost:8080
Username: admin
Password: admin
```

### Trigger a Manual Run

```bash
# Via CLI
airflow dags trigger swiftship_daily_pipeline

# Via UI: go to DAGs → swiftship_daily_pipeline → Trigger DAG
```

### Run a Backfill

```bash
airflow dags backfill \
  --start-date 2024-01-08 \
  --end-date 2024-01-10 \
  swiftship_daily_pipeline
```

---

## Sample DAG Configuration

```python
# config/pipeline_config.py

from datetime import datetime, timedelta

DAG_CONFIG = {
    "dag_id":            "swiftship_daily_pipeline",
    "schedule_interval": "0 6 * * *",   # 6:00 AM every day
    "start_date":        datetime(2024, 1, 1),
    "catchup":           True,           # enable backfill
    "max_active_runs":   1,              # only one run at a time
    "tags":              ["swiftship", "etl", "production"],
}

DEFAULT_ARGS = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure":  False,          # we use Slack instead
    "sla":               timedelta(hours=2),  # must finish by 08:00
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

## Interview Cheat Sheet

**"What is a DAG and why does Airflow use them?"**
> DAG stands for Directed Acyclic Graph. Directed means tasks have dependencies (A must run before B). Acyclic means there are no cycles — task A cannot depend on task B if B depends on A. Airflow uses this structure because it can determine the correct execution order automatically, run independent tasks in parallel, and retry only the failed tasks rather than the whole pipeline.

**"What is an XCom and when would you use it?"**
> XCom (cross-communication) is Airflow's mechanism for passing small values between tasks. When `run_ingestion` finishes, it pushes the row count to XCom. When `generate_report` runs, it pulls that value to include in the report. XComs are stored in the Airflow metadata database and are keyed by task ID and execution date. They are for small metadata, never for large datasets — do not XCom a DataFrame.

**"What is catchup and when would you set it to False?"**
> When `catchup=True` and you have a `start_date` of two weeks ago, Airflow will immediately schedule and run all the missed daily runs since then. This is correct for data pipelines that need historical data. Set `catchup=False` for monitoring or alerting DAGs where historical runs are meaningless — you only care about the current state.

**"How do retries with exponential backoff work?"**
> With `retries=3` and `retry_exponential_backoff=True`, Airflow waits longer between each retry: first retry after 5 minutes, second after 10 minutes, third after 20 minutes (doubles each time). This is the correct behaviour for transient failures — a database that is temporarily overloaded will likely recover within minutes, not seconds. Retrying immediately hammers the already-struggling system.

**"What is an SLA in Airflow?"**
> SLA stands for Service Level Agreement. In Airflow, an SLA on a task means: if this task has not completed within N time of the DAG's scheduled execution time, trigger a callback. I set `sla=timedelta(hours=2)` on my load task, which means: if it has not finished by 08:00 (6 AM start + 2 hours), the `sla_miss_callback` fires and sends a Slack alert to the on-call channel.

**"What is the difference between a sensor and an operator?"**
> An operator defines what a task does — run a Python function, execute a SQL query, call an API. A sensor is a special operator that waits for a condition to become true — a file to appear, an API to return a specific status, another DAG's task to complete. Sensors poke the condition at a configurable interval (`poke_interval`) and block the downstream tasks until the condition is met or a timeout is reached.

---

*Every concept in this project — DAGs, sensors, XComs, retries, SLAs, backfill — is asked about in data engineering interviews at companies that run Airflow, which is most of them.*
