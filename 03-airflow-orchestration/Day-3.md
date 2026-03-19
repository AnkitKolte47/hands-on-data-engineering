# Day 3 — Backfill, SLA Alerts & Slack Notifications

> **Goal: Demonstrate a real backfill run for missed dates, configure SLA miss callbacks that fire when the pipeline is too slow, and wire Slack notifications for both task failures and successful runs. By end of day, the pipeline is fully observable — you know immediately when it fails, when it is slow, and when it succeeds.**

---

## What You Are Building Today

1. Understand how Airflow's `execution_date` works for backfill and why it matters
2. Run a real backfill for 3 historical dates using the Airflow CLI
3. Implement `on_failure_callback` — Slack message with task name, DAG name, and log URL on any task failure
4. Implement `sla_miss_callback` — Slack message when the pipeline runs too long
5. Implement `on_success_callback` — Slack message confirming the daily run completed
6. Add `depends_on_past` understanding — when should today's run wait for yesterday's?

---

## Step 1 — Understand Backfill and execution_date

This is the concept most engineers get wrong. Read carefully.

**`execution_date`** is the logical business date a DAG run represents — NOT the wall-clock time the run executes.

```
Schedule: daily at 06:00

DAG scheduled at:  2024-01-11 06:00:00
execution_date:    2024-01-10 00:00:00   ← represents Jan 10 data
```

Airflow assumes: a pipeline running at 6 AM on the 11th is processing data from the 10th (the previous interval). This is the **interval end** convention. Your scripts must use `execution_date` as the date filter, not `datetime.now()`.

**Why this matters for backfill:**

```bash
# This re-runs the pipeline for Jan 8, Jan 9, Jan 10 — with the CORRECT execution_date
airflow dags backfill \
  --start-date 2024-01-08 \
  --end-date 2024-01-10 \
  swiftship_daily_pipeline
```

Each backfill run gets its own `execution_date`. The `run_ingestion` callable receives the right date and can filter source files accordingly. Without using `execution_date`, all backfill runs would process "today's" data — useless and incorrect.

**`depends_on_past`:**

When `depends_on_past=True`, a task will not start if the same task in the previous day's run did not succeed. For the SwiftShip pipeline, this is appropriate for the `run_load` task — you do not want to load Jan 11 data if Jan 10 failed (the database would have a gap).

```python
# In DEFAULT_ARGS — applies to all tasks unless overridden
"depends_on_past": False,   # default: tasks run independently

# Override per task for the load step:
load_task = PythonOperator(
    task_id="run_load",
    python_callable=load,
    depends_on_past=True,   # do not load today if yesterday's load failed
)
```

---

## Step 2 — Run a Backfill

First, pause the scheduler so it does not interfere with your manual backfill:

```bash
# In the Airflow UI: toggle DAG to "Paused"
# Or via CLI:
docker exec -it <airflow-scheduler-container> airflow dags pause swiftship_daily_pipeline
```

Create input data for three historical dates (simulates late-arriving files):

```bash
# Copy your input files into date-tagged directories or just use the same files
# The key is that the pipeline must be able to run for past dates
mkdir -p data/inputs
cp data/inputs/north_region.csv data/inputs/
# (files already exist — no changes needed for this demo)
```

Run the backfill:

```bash
docker exec -it <airflow-scheduler-container> airflow dags backfill \
  --start-date 2024-01-08 \
  --end-date 2024-01-10 \
  swiftship_daily_pipeline
```

You will see Airflow create three DagRuns:

```
[2024-01-08T00:00:00+00:00] DagRun backfill__2024-01-08T00:00:00+00:00 → running
[2024-01-09T00:00:00+00:00] DagRun backfill__2024-01-09T00:00:00+00:00 → running
[2024-01-10T00:00:00+00:00] DagRun backfill__2024-01-10T00:00:00+00:00 → running
```

Each runs with its own `execution_date`. In the UI, you will see three separate rows in the DagRuns view, each with their own task log entries.

**Verify backfill in the database:**

```bash
sqlite3 data/swiftship.db \
  "SELECT execution_date, COUNT(*) FROM shipments GROUP BY execution_date ORDER BY execution_date;"
```

You should see three distinct execution_dates with record counts — confirming the backfill populated data for all three historical dates.

---

## Step 3 — Configure Slack

Create `config/slack_config.py`. Use Slack's Incoming Webhooks (free, no bot token needed).

```python
# config/slack_config.py

import os

# Set this in your environment or .env file — never hardcode tokens
SLACK_WEBHOOK_URL = os.environ.get(
    "SLACK_WEBHOOK_URL",
    ""  # empty string = Slack notifications silently disabled
)

SLACK_CHANNEL = "#data-pipeline-alerts"
SLACK_USERNAME = "SwiftShip Pipeline Bot"
SLACK_ICON_EMOJI = ":truck:"

# Airflow base URL for building log links
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
```

To get a webhook URL: Slack → Your workspace → Apps → Incoming WebHooks → Add to Slack → choose channel → copy webhook URL.

Set it in docker-compose:

```yaml
# docker-compose.yml — add to environment section of airflow services
environment:
  SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
  AIRFLOW_BASE_URL: "http://localhost:8080"
```

---

## Step 4 — Write the Callback Functions

Create `dags/callbacks.py`.

```python
# dags/callbacks.py

import json
import urllib.request
from datetime import datetime

from config.slack_config import SLACK_WEBHOOK_URL, SLACK_USERNAME, SLACK_ICON_EMOJI, AIRFLOW_BASE_URL


def _send_slack_message(message: str) -> None:
    """
    Sends a message to Slack via Incoming Webhook.
    Silently does nothing if SLACK_WEBHOOK_URL is not configured.
    Never raises — a notification failure must never crash the pipeline.
    """
    if not SLACK_WEBHOOK_URL:
        print(f"[Slack] Webhook not configured. Message suppressed: {message[:100]}")
        return

    payload = json.dumps({
        "username":   SLACK_USERNAME,
        "icon_emoji": SLACK_ICON_EMOJI,
        "text":       message,
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                print(f"[Slack] Non-200 response: {resp.status}")
    except Exception as e:
        # Log but never raise — a notification failure must not fail the pipeline
        print(f"[Slack] Failed to send message: {e}")


def on_failure_callback(context: dict) -> None:
    """
    Called by Airflow when any task fails.
    Sends a Slack message with the failed task name, DAG name, execution date,
    and a direct link to the task log.

    How Airflow calls this:
        Default args: "on_failure_callback": on_failure_callback
        This function is called for every failed TaskInstance in the DAG.
    """
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "Unknown error")

    log_url = (
        f"{AIRFLOW_BASE_URL}/log?"
        f"dag_id={dag_id}"
        f"&task_id={task_id}"
        f"&execution_date={execution_date.isoformat()}"
    )

    message = (
        f":red_circle: *SwiftShip Pipeline — Task Failed*\n"
        f"• *DAG:* `{dag_id}`\n"
        f"• *Task:* `{task_id}`\n"
        f"• *Execution Date:* `{execution_date.date()}`\n"
        f"• *Failed At:* `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}`\n"
        f"• *Error:* `{str(exception)[:200]}`\n"
        f"• *Logs:* <{log_url}|View Task Log>"
    )

    print(f"[on_failure_callback] Sending Slack alert for {dag_id}.{task_id}")
    _send_slack_message(message)


def on_success_callback(context: dict) -> None:
    """
    Called by Airflow when the DAG run completes successfully.
    Attach this to the last task (generate_report) to signal pipeline completion.
    """
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]

    # Pull stats from XCom to include in the success message
    load_stats = task_instance.xcom_pull(task_ids="run_load", key="load_stats") or {}
    transform_stats = task_instance.xcom_pull(task_ids="run_transform", key="transform_stats") or {}

    message = (
        f":white_check_mark: *SwiftShip Pipeline — Daily Run Complete*\n"
        f"• *DAG:* `{dag_id}`\n"
        f"• *Execution Date:* `{execution_date.date()}`\n"
        f"• *Rows Loaded:* `{load_stats.get('rows_loaded', 'N/A'):,}`\n"
        f"• *Rows Rejected:* `{transform_stats.get('rejected_count', 'N/A'):,}`\n"
        f"• *Total in DB:* `{load_stats.get('total_in_db', 'N/A'):,}`"
    )

    _send_slack_message(message)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """
    Called by Airflow when a task misses its SLA.
    Signature is fixed by Airflow — all parameters are provided by the scheduler.

    slas: list of SLAMiss objects — contains task_id and execution_date
    blocking_tis: TaskInstances that are still running and blocking SLA completion
    """
    missed_tasks = [sla.task_id for sla in slas]
    blocking_tasks = [ti.task_id for ti in blocking_tis]

    message = (
        f":warning: *SwiftShip Pipeline — SLA Missed*\n"
        f"• *DAG:* `{dag.dag_id}`\n"
        f"• *SLA missed by tasks:* `{', '.join(missed_tasks)}`\n"
        f"• *Currently blocking:* `{', '.join(blocking_tasks)}`\n"
        f"• *SLA:* Tasks must complete within 2 hours of 06:00 (by 08:00)\n"
        f"• *Action:* Check for slow DB queries, large input files, or resource contention.\n"
        f"• *Dashboard:* <{AIRFLOW_BASE_URL}/dags/{dag.dag_id}/grid|View DAG>"
    )

    print(f"[sla_miss_callback] SLA missed for tasks: {missed_tasks}")
    _send_slack_message(message)
```

---

## Step 5 — Wire Callbacks Into the DAG

Update `dags/swiftship_pipeline.py` to attach all callbacks:

```python
# dags/swiftship_pipeline.py  (final version — add callbacks)

import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from config.pipeline_config import DAG_CONFIG, DEFAULT_ARGS, PATHS
from dags.callbacks import on_failure_callback, on_success_callback, sla_miss_callback
from scripts.ingest import run_ingestion
from scripts.transform import run_transform
from scripts.load import run_load
from scripts.report import run_report

DB_PATH = "/opt/airflow/data/swiftship.db"

# Wire callbacks into DEFAULT_ARGS so they apply to ALL tasks
DEFAULT_ARGS_WITH_CALLBACKS = {
    **DEFAULT_ARGS,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    default_args=DEFAULT_ARGS_WITH_CALLBACKS,
    schedule_interval=DAG_CONFIG["schedule_interval"],
    start_date=DAG_CONFIG["start_date"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
    sla_miss_callback=sla_miss_callback,   # DAG-level SLA callback
    description="SwiftShip daily ETL pipeline — full production version",
) as dag:

    with TaskGroup(group_id="wait_for_files") as wait_group:
        for filename in PATHS["input_files"]:
            FileSensor(
                task_id=f"wait_{filename.split('.')[0]}",
                filepath=f"{PATHS['inputs_dir']}/{filename}",
                poke_interval=60,
                timeout=60 * 90,
                mode="reschedule",
            )

    def ingest(**context):
        stats = run_ingestion(
            inputs_dir=PATHS["inputs_dir"],
            outputs_dir=PATHS["outputs_dir"],
            execution_date=context["execution_date"].date()
        )
        context["ti"].xcom_push(key="ingest_stats", value=stats)
        return stats

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

    ingest_task = PythonOperator(task_id="run_ingestion", python_callable=ingest)
    transform_task = PythonOperator(task_id="run_transform", python_callable=transform)

    load_task = PythonOperator(
        task_id="run_load",
        python_callable=load,
        retries=5,
        retry_delay=timedelta(minutes=2),
        depends_on_past=True,
        sla=timedelta(hours=1, minutes=30),   # load must finish within 1.5h of 6 AM
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=report,
        trigger_rule="all_done",
        on_success_callback=on_success_callback,   # fire only on the last task
    )

    wait_group >> ingest_task >> transform_task >> load_task >> report_task
```

---

## Step 6 — Simulate Failure and Verify Alerts

```bash
# 1. Trigger a DAG run manually
airflow dags trigger swiftship_daily_pipeline

# 2. Then inject a failure by making run_transform raise an exception:
#    Edit scripts/transform.py, add at the top of run_transform():
#    raise RuntimeError("Simulated transform failure for alert testing")

# 3. Trigger again — the transform task fails → Slack message fires

# 4. Remove the raise, trigger again — pipeline succeeds → success Slack fires
```

---

## Day 3 Checklist

Before calling Project 03 complete, confirm:

- [ ] Backfill for 3 dates produces 3 separate DagRuns visible in the Airflow grid view
- [ ] `SELECT execution_date, COUNT(*) FROM shipments GROUP BY execution_date` shows 3 dates
- [ ] `on_failure_callback` fires a Slack message when a task raises an exception
- [ ] `sla_miss_callback` fires a Slack message when SLA threshold is breached (test by setting SLA to 1 second)
- [ ] `on_success_callback` fires on the report task when the full DAG succeeds
- [ ] Callbacks never raise exceptions — a bad Slack URL does not crash the pipeline
- [ ] `depends_on_past=True` on `run_load` prevents today's run if yesterday's run failed (verify by marking yesterday's load as failed and triggering today's run)

---

*With Project 03 complete, your pipeline is scheduled, observable, and resilient. Project 05 builds the data warehouse modeling layer using dbt so analysts can query clean, business-ready data from the database you are now loading reliably.*
