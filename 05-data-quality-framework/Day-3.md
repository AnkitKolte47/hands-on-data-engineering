# Day 3 — HTML Reports, Quality Score Gating & Trend Tracking

> **Goal: Convert the engine results into a professional HTML quality report using Jinja2, implement a quality score gate that blocks the pipeline when data falls below threshold, store scores in a trend database, and wire everything into a single main.py that controls the whole flow. By end of day, running main.py produces a quality report viewable in any browser and gates the pipeline based on data quality.**

---

## What You Are Building Today

1. Build the `QualityScorer` — converts raw validation results into a 0–100 quality score
2. Build the `Reporter` — generates an HTML report using Jinja2
3. Build the `TrendTracker` — stores quality scores per run in SQLite
4. Wire everything into `main.py` with CLI arguments
5. Test the full pipeline — run it, view the HTML report, verify the gate blocks a bad run

---

## Step 1 — Build the Quality Scorer

Create `quality/scorer.py`.

```python
# quality/scorer.py

from dataclasses import dataclass
from typing import Dict

from quality.engine import EngineRunResult


@dataclass
class QualityScore:
    """
    The quality score for a single pipeline run.

    score_pct: 0.0 to 100.0 — percentage of records that passed all rules
    passed_gate: True if score >= threshold
    total: total records validated
    passed: records with no critical or error failures
    failed: records with at least one critical or error failure
    threshold: the minimum acceptable score (configurable)
    """
    score_pct: float
    passed_gate: bool
    total: int
    passed: int
    failed: int
    threshold: float
    grade: str   # A/B/C/D/F — for human-readable reporting


class QualityScorer:
    """
    Computes a quality score from an EngineRunResult.

    Score formula:
        score = (passed_records / total_records) * 100

    Warning-only failures do not affect the score — they appear in
    the report but do not count toward failed_records.

    Gate: if score < threshold, the pipeline is blocked. The load step
    does not run. The HTML report is still generated so the data team
    can diagnose the problem.
    """

    GRADE_THRESHOLDS = [
        (98.0, "A"),
        (95.0, "B"),
        (90.0, "C"),
        (80.0, "D"),
        (0.0,  "F"),
    ]

    def __init__(self, threshold: float = 95.0):
        if not (0 <= threshold <= 100):
            raise ValueError(f"Threshold must be 0–100, got {threshold}")
        self.threshold = threshold

    def score(self, run_result: EngineRunResult) -> QualityScore:
        """Compute quality score and gate decision from engine results."""
        total = run_result.total_records
        if total == 0:
            return QualityScore(
                score_pct=0.0, passed_gate=False,
                total=0, passed=0, failed=0,
                threshold=self.threshold, grade="F"
            )

        passed = run_result.passed_count
        score_pct = round((passed / total) * 100, 2)
        passed_gate = score_pct >= self.threshold
        grade = self._compute_grade(score_pct)

        qs = QualityScore(
            score_pct=score_pct,
            passed_gate=passed_gate,
            total=total,
            passed=passed,
            failed=run_result.failed_count,
            threshold=self.threshold,
            grade=grade
        )

        self._print_gate_decision(qs)
        return qs

    def _compute_grade(self, score: float) -> str:
        for min_score, grade in self.GRADE_THRESHOLDS:
            if score >= min_score:
                return grade
        return "F"

    def _print_gate_decision(self, qs: QualityScore) -> None:
        status = "PASS" if qs.passed_gate else "BLOCK"
        print(f"""
[QualityScorer] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Quality Score : {qs.score_pct:.2f}% (Grade: {qs.grade})
  Threshold     : {qs.threshold:.1f}%
  Gate Decision : {status}
  Records       : {qs.passed} passed / {qs.failed} failed / {qs.total} total
[QualityScorer] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
        if not qs.passed_gate:
            print(f"[QualityScorer] PIPELINE BLOCKED: quality score {qs.score_pct:.2f}% "
                  f"is below threshold {qs.threshold:.1f}%. "
                  f"Fix data quality issues before loading. "
                  f"See the HTML report for details.")
```

---

## Step 2 — Build the Trend Tracker

Create `quality/trend_tracker.py`.

```python
# quality/trend_tracker.py

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from quality.scorer import QualityScore


class TrendTracker:
    """
    Stores quality scores per pipeline run in a SQLite database.

    Why SQLite for trends? The quality framework should not depend on the
    pipeline's database. A separate SQLite file gives the quality team
    an independent, queryable history — even if the main DB is down.

    Trend analysis queries:
    - Average score over last 7 days
    - Days since last gate failure
    - Score by source file over time
    - Worst-performing rules historically
    """

    def __init__(self, db_path: str = "data/outputs/quality_trends.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_runs (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at          TEXT NOT NULL DEFAULT (datetime('now')),
                    input_file      TEXT,
                    total_records   INTEGER,
                    passed_records  INTEGER,
                    failed_records  INTEGER,
                    score_pct       REAL,
                    grade           TEXT,
                    threshold       REAL,
                    passed_gate     INTEGER,   -- 1 = passed, 0 = blocked
                    notes           TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rule_run_stats (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id          INTEGER REFERENCES quality_runs(id),
                    rule_name       TEXT,
                    severity        TEXT,
                    passed_count    INTEGER,
                    failed_count    INTEGER
                )
            """)
            conn.commit()

    def record(
        self,
        score: QualityScore,
        input_file: str,
        rule_stats: Dict[str, Dict],
        notes: str = ""
    ) -> int:
        """
        Save a run's quality score to the trend database.
        Returns the run_id for reference.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO quality_runs
                    (input_file, total_records, passed_records, failed_records,
                     score_pct, grade, threshold, passed_gate, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                input_file,
                score.total, score.passed, score.failed,
                score.score_pct, score.grade, score.threshold,
                1 if score.passed_gate else 0,
                notes
            ))
            run_id = cursor.lastrowid

            for rule_name, stats in rule_stats.items():
                conn.execute("""
                    INSERT INTO rule_run_stats
                        (run_id, rule_name, severity, passed_count, failed_count)
                    VALUES (?, ?, ?, ?, ?)
                """, (run_id, rule_name, stats["severity"],
                      stats["passed"], stats["failed"]))

            conn.commit()

        print(f"[TrendTracker] Run recorded with id={run_id} in {self.db_path}")
        return run_id

    def get_trend(self, last_n_runs: int = 30) -> List[Dict]:
        """Return the last N runs as a list of dicts for charting."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT id, run_at, score_pct, grade, passed_gate, total_records
                FROM quality_runs
                ORDER BY id DESC
                LIMIT ?
            """, (last_n_runs,)).fetchall()

        return [dict(row) for row in reversed(rows)]

    def print_trend_summary(self, last_n_runs: int = 10) -> None:
        trend = self.get_trend(last_n_runs)
        if not trend:
            print("[TrendTracker] No historical runs found.")
            return

        print(f"\n[TrendTracker] Last {len(trend)} run(s):")
        print(f"  {'Run ID':<8} {'Date':<22} {'Score':>8} {'Grade':<6} {'Gate':<8} {'Records':>8}")
        print(f"  {'-'*8} {'-'*22} {'-'*8} {'-'*6} {'-'*8} {'-'*8}")
        for r in trend:
            gate = "PASS" if r["passed_gate"] else "BLOCK"
            print(f"  {r['id']:<8} {r['run_at']:<22} {r['score_pct']:>7.2f}% "
                  f"{r['grade']:<6} {gate:<8} {r['total_records']:>8,}")
```

---

## Step 3 — Build the HTML Reporter

Create `quality/reporter.py`. Uses Jinja2 to render a professional HTML report.

```python
# quality/reporter.py

from pathlib import Path
from datetime import datetime
from typing import List, Dict

from jinja2 import Template

from quality.scorer import QualityScore
from quality.engine import EngineRunResult


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SwiftShip Data Quality Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               margin: 0; background: #f5f5f5; color: #333; }
        .header { background: #1a1a2e; color: white; padding: 24px 40px; }
        .header h1 { margin: 0; font-size: 1.6rem; }
        .header p  { margin: 4px 0 0; opacity: 0.7; font-size: 0.9rem; }
        .body { padding: 32px 40px; max-width: 1200px; margin: 0 auto; }
        .score-card { display: flex; gap: 16px; margin-bottom: 32px; flex-wrap: wrap; }
        .card { background: white; border-radius: 8px; padding: 20px 24px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; min-width: 160px; }
        .card .label { font-size: 0.75rem; text-transform: uppercase;
                       letter-spacing: 0.05em; color: #888; }
        .card .value { font-size: 2rem; font-weight: 700; margin-top: 4px; }
        .gate-pass { color: #2d6a4f; }
        .gate-fail { color: #c1121f; }
        table { width: 100%; border-collapse: collapse; background: white;
                border-radius: 8px; overflow: hidden;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 32px; }
        th { background: #f0f0f0; padding: 12px 16px; text-align: left;
             font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }
        td { padding: 10px 16px; border-top: 1px solid #f0f0f0; font-size: 0.9rem; }
        tr:hover td { background: #fafafa; }
        .pill { display: inline-block; padding: 2px 10px; border-radius: 12px;
                font-size: 0.78rem; font-weight: 600; }
        .critical { background: #fee2e2; color: #991b1b; }
        .error    { background: #fef3c7; color: #92400e; }
        .warning  { background: #e0f2fe; color: #0c4a6e; }
        .pass-pill { background: #d1fae5; color: #065f46; }
        .fail-pill { background: #fee2e2; color: #991b1b; }
        h2 { font-size: 1.1rem; margin: 0 0 16px; }
        .footer { text-align: center; color: #aaa; font-size: 0.8rem;
                  padding: 24px 0; }
    </style>
</head>
<body>
<div class="header">
    <h1>SwiftShip — Data Quality Report</h1>
    <p>Generated {{ generated_at }} &nbsp;|&nbsp; Input: {{ input_file }}</p>
</div>
<div class="body">

    <div class="score-card">
        <div class="card">
            <div class="label">Quality Score</div>
            <div class="value {{ 'gate-pass' if score.passed_gate else 'gate-fail' }}">
                {{ score.score_pct }}%
            </div>
        </div>
        <div class="card">
            <div class="label">Grade</div>
            <div class="value">{{ score.grade }}</div>
        </div>
        <div class="card">
            <div class="label">Gate Decision</div>
            <div class="value {{ 'gate-pass' if score.passed_gate else 'gate-fail' }}">
                {{ 'PASS' if score.passed_gate else 'BLOCKED' }}
            </div>
        </div>
        <div class="card">
            <div class="label">Total Records</div>
            <div class="value">{{ "{:,}".format(score.total) }}</div>
        </div>
        <div class="card">
            <div class="label">Passed</div>
            <div class="value gate-pass">{{ "{:,}".format(score.passed) }}</div>
        </div>
        <div class="card">
            <div class="label">Failed</div>
            <div class="value gate-fail">{{ "{:,}".format(score.failed) }}</div>
        </div>
    </div>

    <h2>Rule Results</h2>
    <table>
        <tr>
            <th>Rule Name</th>
            <th>Severity</th>
            <th>Passed</th>
            <th>Failed</th>
            <th>Failure Rate</th>
        </tr>
        {% for rule_name, stats in rule_stats.items() %}
        <tr>
            <td>{{ rule_name }}</td>
            <td><span class="pill {{ stats.severity }}">{{ stats.severity }}</span></td>
            <td>{{ "{:,}".format(stats.passed) }}</td>
            <td>{{ "{:,}".format(stats.failed) }}</td>
            <td>
                {% if (stats.passed + stats.failed) > 0 %}
                {{ "%.1f"|format(100 * stats.failed / (stats.passed + stats.failed)) }}%
                {% else %}N/A{% endif %}
            </td>
        </tr>
        {% endfor %}
    </table>

    {% if failed_samples %}
    <h2>Sample Failed Records (first {{ failed_samples|length }})</h2>
    <table>
        <tr>
            <th>Shipment ID</th>
            <th>Failure Reasons</th>
        </tr>
        {% for rec in failed_samples %}
        <tr>
            <td>{{ rec.get('shipment_id', '(null)') }}</td>
            <td style="color: #c1121f; font-size: 0.82rem;">
                {{ rec.get('_failure_reasons', '') }}
            </td>
        </tr>
        {% endfor %}
    </table>
    {% endif %}

</div>
<div class="footer">
    SwiftShip Data Quality Framework &nbsp;|&nbsp; Threshold: {{ score.threshold }}%
</div>
</body>
</html>
"""


class Reporter:
    """Renders the HTML quality report from engine results and score."""

    def __init__(self, output_path: str = "data/outputs/quality_report.html"):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        score: QualityScore,
        run_result: EngineRunResult,
        input_file: str,
        max_sample_failures: int = 20
    ) -> str:
        """Render and write the HTML report. Returns the output path."""
        template = Template(HTML_TEMPLATE)
        html = template.render(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            input_file=input_file,
            score=score,
            rule_stats=run_result.rule_stats,
            failed_samples=run_result.failed_records[:max_sample_failures]
        )

        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"[Reporter] HTML report written to {self.output_path}")
        return str(self.output_path)
```

---

## Step 4 — Wire Everything in main.py

```python
# main.py

import argparse
import sys

from quality.expectation_loader import ExpectationLoader
from quality.engine import ValidationEngine
from quality.scorer import QualityScorer
from quality.reporter import Reporter
from quality.trend_tracker import TrendTracker


def run_quality_check(
    input_file: str,
    expectations_file: str,
    threshold: float = 95.0,
    dry_run: bool = False
) -> bool:
    """
    Run the full data quality pipeline.
    Returns True if the quality gate passes, False if blocked.
    """
    print(f"\n{'='*55}")
    print(f"  SwiftShip Data Quality Framework")
    print(f"  Input     : {input_file}")
    print(f"  Rules     : {expectations_file}")
    print(f"  Threshold : {threshold}%")
    print(f"{'='*55}\n")

    # Step 1 — Load rules
    loader = ExpectationLoader(expectations_file)
    rules = loader.load()

    # Step 2 — Run validation
    engine = ValidationEngine(rules)
    run_result = engine.validate_file(input_file)
    engine.write_failed_records(run_result, "data/outputs/failed_records.csv")

    # Step 3 — Score
    scorer = QualityScorer(threshold=threshold)
    score = scorer.score(run_result)

    # Step 4 — Report
    reporter = Reporter("data/outputs/quality_report.html")
    report_path = reporter.generate(score, run_result, input_file)

    # Step 5 — Trend tracking
    tracker = TrendTracker("data/outputs/quality_trends.db")
    tracker.record(score, input_file, run_result.rule_stats)
    tracker.print_trend_summary(last_n_runs=10)

    # Step 6 — Gate decision
    if not score.passed_gate:
        print(f"\n[GATE BLOCKED] Quality score {score.score_pct:.2f}% < threshold {threshold}%.")
        print(f"[GATE BLOCKED] Load step will NOT run.")
        print(f"[GATE BLOCKED] Review: {report_path}")
        if not dry_run:
            return False

    print(f"\n[GATE PASSED] Quality score {score.score_pct:.2f}% >= threshold {threshold}%.")
    print(f"[GATE PASSED] Pipeline may proceed to load step.")
    return True


def main():
    parser = argparse.ArgumentParser(description="SwiftShip Data Quality Framework")
    parser.add_argument(
        "--input", default="data/inputs/consolidated.csv",
        help="Path to the CSV file to validate"
    )
    parser.add_argument(
        "--expectations", default="config/expectations.yaml",
        help="Path to the expectations YAML config"
    )
    parser.add_argument(
        "--threshold", type=float, default=95.0,
        help="Quality score threshold (0–100). Pipeline blocked if score falls below this."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run quality checks and generate report, but do not block pipeline even if score fails"
    )
    args = parser.parse_args()

    passed = run_quality_check(
        input_file=args.input,
        expectations_file=args.expectations,
        threshold=args.threshold,
        dry_run=args.dry_run
    )

    sys.exit(0 if passed else 1)  # exit code 1 = blocked (Airflow treats non-zero as failure)


if __name__ == "__main__":
    main()
```

---

## Step 5 — Test the Full Flow

```bash
# Run with good data (should pass)
python main.py --input data/inputs/consolidated.csv

# Run with bad data (should be blocked)
python main.py --input data/inputs/day2_test.csv --threshold 95

# Run with strict threshold
python main.py --threshold 100  # any failure blocks

# Dry run (report generated but gate does not block)
python main.py --input data/inputs/day2_test.csv --dry-run

# View report
open data/outputs/quality_report.html

# Query trend database
sqlite3 data/outputs/quality_trends.db \
  "SELECT run_at, score_pct, grade, passed_gate FROM quality_runs ORDER BY id DESC LIMIT 10;"
```

---

## Day 3 Checklist

Before calling Project 06 complete, confirm:

- [ ] `main.py` exits with code 0 when quality gate passes, code 1 when blocked
- [ ] HTML report opens in a browser and displays score card, rule table, and failed samples
- [ ] HTML report shows `BLOCKED` in red when gate fails, `PASS` in green when it passes
- [ ] `quality_trends.db` has one new row after each run (verify with SQLite query)
- [ ] Running `--dry-run` with bad data generates the report but exits with code 0
- [ ] `trend_tracker.print_trend_summary()` shows all historical runs with scores
- [ ] Adding a new rule to `expectations.yaml` requires no Python code changes
- [ ] The quality framework can be added as a task in the Airflow DAG from Project 03 by calling `python main.py` and checking the exit code

---

*With Project 06 complete, you have a full data quality framework that validates data before it reaches the warehouse, generates human-readable reports, gates the pipeline, and tracks quality trends over time. Project 07 moves the entire stack to Azure cloud.*
