# Day 1 — Rule Engine Design, Schema & Null Validation

> **Goal: Design the rule engine architecture using abstract base classes, implement schema validation and null checks, build the ValidationEngine that runs all rules against a dataset, and verify it catches real schema and null violations. By end of day, running the engine on a broken CSV produces a clear list of failed records with reasons.**

---

## What You Are Building Today

1. Design and implement the `BaseRule` abstract class — the contract every validation rule must follow
2. Implement `SchemaRule` — validates required columns exist and have the right type
3. Implement `NullRule` — validates required fields are not null or empty
4. Build the `ValidationEngine` that loads rules, iterates records, and collects results
5. Build the `ExpectationLoader` that reads rules from `config/expectations.yaml`
6. Test with a deliberately corrupted input file

---

## Step 1 — Design the Rule Engine Architecture

Before writing code, design the system. Every rule must answer the same question: "Does this record pass?"

```
ExpectationLoader
    reads config/expectations.yaml
    builds: List[BaseRule]
        │
        ▼
ValidationEngine
    iterates records from CSVParser
    for each record: runs all rules
    collects: ValidationResult per record
        │
        ├── passed_records  → forwarded to pipeline
        ├── failed_records  → written to failed_records.csv
        └── run_statistics  → passed to QualityScorer (Day 3)
```

Each `Rule` has:
- `name` — human-readable label for the report
- `severity` — `critical`, `error`, or `warning`
- `validate(record)` — returns `RuleResult(passed=True/False, message="")`

---

## Step 2 — Create the Base Rule

Create `quality/rules/base_rule.py`:

```python
# quality/rules/base_rule.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class RuleResult:
    """
    The result of running one rule against one record.

    passed:  True if the record satisfies the rule, False if it violates it.
    message: Empty string on pass. Human-readable failure reason on fail.
             This message appears in the HTML report and failed_records.csv.
    """
    passed: bool
    message: str = ""

    @classmethod
    def pass_(cls) -> "RuleResult":
        return cls(passed=True, message="")

    @classmethod
    def fail(cls, message: str) -> "RuleResult":
        return cls(passed=False, message=message)


class BaseRule(ABC):
    """
    Abstract base class for all data quality rules.

    Design contract:
    - Every subclass must implement validate(record) → RuleResult
    - Rules are stateless — validate() must not store record data on self
    - Rules receive one record at a time — they cannot see the full dataset
      (cross-record rules are implemented at the engine level)
    - Rules never raise exceptions — they catch internal errors and return
      a RuleResult.fail() with the exception message
    """

    VALID_SEVERITIES = {"critical", "error", "warning"}

    def __init__(self, name: str, severity: str = "error"):
        if severity not in self.VALID_SEVERITIES:
            raise ValueError(
                f"Rule '{name}': invalid severity '{severity}'. "
                f"Must be one of: {self.VALID_SEVERITIES}"
            )
        self.name = name
        self.severity = severity

    @abstractmethod
    def validate(self, record: Dict[str, Any]) -> RuleResult:
        """
        Validate a single record dict against this rule.

        Args:
            record: One row from the dataset as a dict of {column: value}.

        Returns:
            RuleResult.pass_() if the record satisfies the rule.
            RuleResult.fail(message) if it violates the rule.

        Must never raise. Catch all exceptions internally and return fail().
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, severity={self.severity!r})"
```

---

## Step 3 — Implement SchemaRule

Create `quality/rules/schema_rule.py`.

Schema validation is the first check — if the columns are wrong, all subsequent rules will fail with confusing errors. Run schema validation once for the whole dataset, not per-record.

```python
# quality/rules/schema_rule.py

from typing import Dict, Any, List, Set, Optional

from quality.rules.base_rule import BaseRule, RuleResult


class SchemaRule(BaseRule):
    """
    Validates that a record has all required columns.
    Optionally validates column types.

    This rule is typically run once on the first record (to detect
    structural issues early) and then on every record if strict mode is on.

    In practice: if a CSV is missing the 'shipment_id' column entirely,
    every single record will fail the NullRule for shipment_id — 10,000
    failures that all have the same root cause. SchemaRule catches the
    root cause once, at the column level.
    """

    def __init__(
        self,
        name: str,
        required_columns: List[str],
        type_expectations: Optional[Dict[str, type]] = None,
        severity: str = "critical"
    ):
        super().__init__(name, severity)
        self.required_columns: Set[str] = set(required_columns)
        self.type_expectations: Dict[str, type] = type_expectations or {}

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            record_columns = set(record.keys())
            missing = self.required_columns - record_columns

            if missing:
                return RuleResult.fail(
                    f"Missing required columns: {sorted(missing)}. "
                    f"Record has: {sorted(record_columns)}"
                )

            # Type checks (optional — only if type_expectations were provided)
            type_errors = []
            for col, expected_type in self.type_expectations.items():
                value = record.get(col)
                if value is not None and not isinstance(value, expected_type):
                    try:
                        # Attempt coercion — if it works, the type is convertible
                        expected_type(value)
                    except (ValueError, TypeError):
                        type_errors.append(
                            f"Column '{col}': expected {expected_type.__name__}, "
                            f"got {type(value).__name__} (value: {value!r})"
                        )

            if type_errors:
                return RuleResult.fail("; ".join(type_errors))

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"SchemaRule internal error: {e}")
```

---

## Step 4 — Implement NullRule

Create `quality/rules/null_rule.py`.

```python
# quality/rules/null_rule.py

from typing import Dict, Any, List

from quality.rules.base_rule import BaseRule, RuleResult


class NullRule(BaseRule):
    """
    Validates that specified columns are not null and not empty string.

    Why check for empty string as well as None?
    CSV files do not distinguish between null and empty. A CSV cell
    with no content is read as "" (empty string) by csv.DictReader.
    Both None and "" must be treated as "missing" for required fields.
    """

    def __init__(self, name: str, column: str, severity: str = "error"):
        super().__init__(name, severity)
        self.column = column

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            value = record.get(self.column)

            if value is None:
                return RuleResult.fail(
                    f"Column '{self.column}' is null. "
                    f"This is a required field."
                )

            if isinstance(value, str) and value.strip() == "":
                return RuleResult.fail(
                    f"Column '{self.column}' is empty string. "
                    f"This is a required field."
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"NullRule internal error: {e}")


class MultiColumnNullRule(BaseRule):
    """
    Validates multiple columns for null in a single rule.
    More efficient than creating one NullRule per column when many
    columns have the same severity.
    """

    def __init__(self, name: str, columns: List[str], severity: str = "error"):
        super().__init__(name, severity)
        self.columns = columns

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            null_columns = []
            for col in self.columns:
                value = record.get(col)
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    null_columns.append(col)

            if null_columns:
                return RuleResult.fail(
                    f"Required columns are null or empty: {null_columns}"
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"MultiColumnNullRule internal error: {e}")
```

---

## Step 5 — Build the Expectation Loader

Create `quality/expectation_loader.py`. This reads `expectations.yaml` and returns a list of `BaseRule` objects.

```python
# quality/expectation_loader.py

from pathlib import Path
from typing import List

import yaml

from quality.rules.base_rule import BaseRule
from quality.rules.schema_rule import SchemaRule
from quality.rules.null_rule import NullRule


# Registry maps type strings in YAML to concrete Rule classes.
# Adding a new rule type = adding one entry here.
RULE_REGISTRY = {
    "schema":   SchemaRule,
    "not_null": NullRule,
    # range, accepted_values, freshness, referential added in Day 2
}


class ExpectationLoader:
    """
    Reads an expectations YAML config and builds a list of BaseRule objects.

    Design: the config file is the source of truth for all validation logic.
    Adding or changing a rule requires editing the YAML, not the Python code.
    This follows the Open/Closed Principle: the engine is closed for modification
    but open for extension via new YAML entries.
    """

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Expectations config not found: {self.config_path}")

    def load(self) -> List[BaseRule]:
        """
        Parse the YAML config and return an ordered list of Rule objects.
        Rules are applied in the order they appear in the config.
        Critical rules appear first — they can short-circuit the rest.
        """
        with open(self.config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        rules = []
        for rule_def in config.get("rules", []):
            rule_type = rule_def.get("type")
            if rule_type not in RULE_REGISTRY:
                raise ValueError(
                    f"Unknown rule type: '{rule_type}'. "
                    f"Registered types: {list(RULE_REGISTRY.keys())}"
                )

            rule_class = RULE_REGISTRY[rule_type]
            rule = self._build_rule(rule_class, rule_def)
            rules.append(rule)

        print(f"[ExpectationLoader] Loaded {len(rules)} rules from {self.config_path}")
        return rules

    def _build_rule(self, rule_class, rule_def: dict) -> BaseRule:
        """
        Builds a rule instance from its YAML definition.
        Each rule class has a different set of constructor parameters.
        """
        name = rule_def["name"]
        severity = rule_def.get("severity", "error")
        rule_type = rule_def["type"]

        if rule_type == "schema":
            return rule_class(
                name=name,
                required_columns=rule_def["required_columns"],
                severity=severity
            )
        elif rule_type == "not_null":
            return rule_class(
                name=name,
                column=rule_def["column"],
                severity=severity
            )
        else:
            # For Day 2 rules — pass all config keys as kwargs
            params = {k: v for k, v in rule_def.items()
                      if k not in ("type", "name", "severity")}
            return rule_class(name=name, severity=severity, **params)
```

---

## Step 6 — Build the ValidationEngine

Create `quality/engine.py`:

```python
# quality/engine.py

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any

from quality.rules.base_rule import BaseRule, RuleResult


@dataclass
class RecordValidationResult:
    """Result of running all rules against one record."""
    record: Dict[str, Any]
    rule_results: List[tuple]  # [(rule_name, severity, RuleResult)]

    @property
    def passed(self) -> bool:
        """Record passes if no critical or error rules failed."""
        for rule_name, severity, result in self.rule_results:
            if not result.passed and severity in ("critical", "error"):
                return False
        return True

    @property
    def failure_messages(self) -> List[str]:
        return [
            f"[{severity.upper()}] {rule_name}: {result.message}"
            for rule_name, severity, result in self.rule_results
            if not result.passed
        ]


@dataclass
class EngineRunResult:
    """Aggregate result of running the engine over the full dataset."""
    total_records: int = 0
    passed_count: int = 0
    failed_count: int = 0
    warning_count: int = 0
    rule_stats: Dict[str, Dict] = field(default_factory=dict)
    failed_records: List[Dict] = field(default_factory=list)


class ValidationEngine:
    """
    Runs all rules against every record in a CSV file.
    Collects per-record and aggregate results.
    """

    def __init__(self, rules: List[BaseRule]):
        self.rules = rules
        # Initialise per-rule counters
        self.rule_stats = {
            rule.name: {"passed": 0, "failed": 0, "severity": rule.severity}
            for rule in rules
        }

    def validate_file(self, file_path: str) -> EngineRunResult:
        """
        Validate all records in the CSV file.
        Returns an EngineRunResult with full statistics and failed records.
        """
        result = EngineRunResult()
        result.rule_stats = dict(self.rule_stats)

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for record in reader:
                result.total_records += 1
                record_result = self._validate_record(record)

                # Update rule-level stats
                for rule_name, severity, rule_result in record_result.rule_results:
                    if rule_result.passed:
                        result.rule_stats[rule_name]["passed"] += 1
                    else:
                        result.rule_stats[rule_name]["failed"] += 1

                if record_result.passed:
                    result.passed_count += 1
                else:
                    result.failed_count += 1
                    failed_copy = dict(record)
                    failed_copy["_failure_reasons"] = " | ".join(record_result.failure_messages)
                    result.failed_records.append(failed_copy)

        print(f"[ValidationEngine] Validated {result.total_records} records: "
              f"{result.passed_count} passed, {result.failed_count} failed.")
        return result

    def _validate_record(self, record: Dict[str, Any]) -> RecordValidationResult:
        """Run all rules against one record. Catch-all prevents any rule from crashing the engine."""
        rule_results = []
        for rule in self.rules:
            try:
                result = rule.validate(record)
            except Exception as e:
                # Rule itself raised unexpectedly — treat as failure but do not crash
                result = RuleResult.fail(f"Rule raised exception: {e}")
            rule_results.append((rule.name, rule.severity, result))
        return RecordValidationResult(record=record, rule_results=rule_results)

    def write_failed_records(self, run_result: EngineRunResult, output_path: str) -> None:
        """Write all failed records to a CSV with failure reasons as an extra column."""
        if not run_result.failed_records:
            print("[ValidationEngine] No failed records to write.")
            return

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(run_result.failed_records[0].keys())

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(run_result.failed_records)

        print(f"[ValidationEngine] {len(run_result.failed_records)} failed records "
              f"written to {output_path}")
```

---

## Step 7 — Smoke Test Day 1

Create a corrupted test file `data/inputs/broken_test.csv`:

```csv
shipment_id,origin_city,destination_city,dispatch_date,status,weight_kg
SH001,mumbai,delhi,2024-01-10,DELIVERED,12.5
,pune,bangalore,2024-01-11,IN_TRANSIT,8.2
SH003,,,2024-01-12,,
SH004,nagpur,bhopal,2024-01-12,INVALID_STATUS,99.9
```

Then write `test_day1.py`:

```python
from quality.expectation_loader import ExpectationLoader
from quality.engine import ValidationEngine

loader = ExpectationLoader("config/expectations.yaml")
rules = loader.load()

engine = ValidationEngine(rules)
result = engine.validate_file("data/inputs/broken_test.csv")
engine.write_failed_records(result, "data/outputs/failed_records.csv")

print(f"\nTotal: {result.total_records}")
print(f"Passed: {result.passed_count}")
print(f"Failed: {result.failed_count}")
for rec in result.failed_records:
    print(f"  {rec.get('shipment_id', '(null)')} → {rec['_failure_reasons']}")
```

Expected output:

```
[ExpectationLoader] Loaded 3 rules from config/expectations.yaml
[ValidationEngine] Validated 4 records: 1 passed, 3 failed.

Total: 4
Passed: 1
Failed: 3
  (null)  → [CRITICAL] shipment_id must never be null: Column 'shipment_id' is null...
  SH003   → [ERROR] status must never be null: Column 'status' is empty string...
  SH004   → (passes null check — Day 2 adds the accepted_values check)
```

---

## Day 1 Checklist

Before moving to Day 2, confirm:

- [ ] `BaseRule` is abstract — instantiating it directly raises `TypeError`
- [ ] `SchemaRule` with a missing column returns `RuleResult.fail(...)` with the missing column name
- [ ] `NullRule` treats both `None` and `""` as failures
- [ ] `ExpectationLoader.load()` returns the correct number of rules from the YAML
- [ ] `ValidationEngine.validate_file()` processes all records even when one rule raises an exception
- [ ] Running `test_day1.py` with `broken_test.csv` produces 3 failed records
- [ ] `failed_records.csv` contains the `_failure_reasons` column with clear, human-readable messages
- [ ] A correctly formatted CSV with valid data produces 0 failed records

---

*Day 2 adds the remaining four rule types: RangeRule, AcceptedValuesRule, FreshnessRule, and ReferentialRule — each addressing a class of data quality problem that schema and null checks cannot catch.*
