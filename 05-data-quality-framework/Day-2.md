# Day 2 — Range Checks, Freshness Checks & Referential Integrity

> **Goal: Add four new rule types — RangeRule, AcceptedValuesRule, FreshnessRule, and ReferentialRule — wire them into the ExpectationLoader, and validate a dataset that has range violations, stale dates, unknown statuses, and cities that do not exist in the reference table. By end of day, the engine catches all six classes of data quality violation.**

---

## What You Are Building Today

1. Implement `RangeRule` — numeric bounds and date range validation
2. Implement `AcceptedValuesRule` — enum / whitelist validation
3. Implement `FreshnessRule` — date recency check (how old is this data?)
4. Implement `ReferentialRule` — cross-table existence check (does this city exist?)
5. Register all new rules in the `ExpectationLoader`
6. Write a comprehensive test dataset that triggers every rule type

---

## Step 1 — Implement RangeRule

Create `quality/rules/range_rule.py`.

```python
# quality/rules/range_rule.py

from typing import Dict, Any, Optional, Union
from datetime import date

from quality.rules.base_rule import BaseRule, RuleResult

Number = Union[int, float]


class RangeRule(BaseRule):
    """
    Validates that a numeric or date column falls within a specified range.

    Handles:
    - Numeric ranges: weight_kg must be between 0.1 and 500
    - Date ranges: dispatch_date must be after 2020-01-01
    - Null handling: configurable — some fields are optional (allow_null=True)

    Why not just check at ingest? The ingester does not know the business rules.
    The weight sensor may send 9999 — technically a valid number, but physically
    impossible for a road shipment. RangeRule encodes the business constraint.
    """

    def __init__(
        self,
        name: str,
        column: str,
        min_value: Optional[Number] = None,
        max_value: Optional[Number] = None,
        allow_null: bool = False,
        severity: str = "error"
    ):
        super().__init__(name, severity)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.allow_null = allow_null

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            raw_value = record.get(self.column)

            # Handle null/empty
            if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ""):
                if self.allow_null:
                    return RuleResult.pass_()
                return RuleResult.fail(
                    f"Column '{self.column}' is null/empty and null is not allowed."
                )

            # Attempt numeric coercion
            # Strips common suffixes like "kg" from "12.5kg"
            value_str = str(raw_value).replace("kg", "").replace(",", "").strip()
            try:
                value = float(value_str)
            except ValueError:
                return RuleResult.fail(
                    f"Column '{self.column}' value {raw_value!r} is not numeric."
                )

            # Range check
            if self.min_value is not None and value < self.min_value:
                return RuleResult.fail(
                    f"Column '{self.column}' value {value} is below minimum {self.min_value}."
                )

            if self.max_value is not None and value > self.max_value:
                return RuleResult.fail(
                    f"Column '{self.column}' value {value} exceeds maximum {self.max_value}."
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"RangeRule internal error: {e}")


class DateRangeRule(BaseRule):
    """
    Validates that a date column is within a range.
    For SwiftShip: dispatch_date must be in the past (not future).
    """

    def __init__(
        self,
        name: str,
        column: str,
        min_date: Optional[str] = None,   # ISO format: "2020-01-01"
        max_date: Optional[str] = None,   # "today" is a special keyword
        allow_null: bool = True,
        severity: str = "error"
    ):
        super().__init__(name, severity)
        self.column = column
        self.min_date = date.fromisoformat(min_date) if min_date else None
        self.max_date_str = max_date
        self.allow_null = allow_null

    def _resolve_max_date(self) -> Optional[date]:
        if self.max_date_str == "today":
            return date.today()
        if self.max_date_str:
            return date.fromisoformat(self.max_date_str)
        return None

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            raw_value = record.get(self.column)

            if not raw_value or (isinstance(raw_value, str) and raw_value.strip() == ""):
                if self.allow_null:
                    return RuleResult.pass_()
                return RuleResult.fail(f"Column '{self.column}' is null/empty.")

            try:
                parsed_date = date.fromisoformat(str(raw_value).strip())
            except ValueError:
                return RuleResult.fail(
                    f"Column '{self.column}' value {raw_value!r} is not a valid ISO date (YYYY-MM-DD)."
                )

            max_date = self._resolve_max_date()

            if self.min_date and parsed_date < self.min_date:
                return RuleResult.fail(
                    f"Column '{self.column}' date {parsed_date} is before minimum {self.min_date}."
                )

            if max_date and parsed_date > max_date:
                return RuleResult.fail(
                    f"Column '{self.column}' date {parsed_date} is in the future "
                    f"(today is {date.today()}). Future dispatch dates are not valid."
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"DateRangeRule internal error: {e}")
```

---

## Step 2 — Implement AcceptedValuesRule

Create `quality/rules/accepted_values_rule.py`.

```python
# quality/rules/accepted_values_rule.py

from typing import Dict, Any, List, Optional

from quality.rules.base_rule import BaseRule, RuleResult


class AcceptedValuesRule(BaseRule):
    """
    Validates that a column's value is one of a specified whitelist.

    Case handling is configurable — for status codes, case-insensitive matching
    prevents "delivered" from failing when "DELIVERED" is the accepted value.

    Why whitelists matter: source systems occasionally introduce new status codes
    without warning ("LOST_IN_TRANSIT", "CUSTOMS_HOLD"). These new values are not
    in the downstream mapping tables — they would cause silent null joins in dbt marts.
    An AcceptedValuesRule catches them at ingestion.
    """

    def __init__(
        self,
        name: str,
        column: str,
        values: List[str],
        case_sensitive: bool = False,
        allow_null: bool = False,
        severity: str = "error"
    ):
        super().__init__(name, severity)
        self.column = column
        self.allow_null = allow_null
        self.case_sensitive = case_sensitive

        if case_sensitive:
            self.accepted = set(values)
        else:
            self.accepted = {v.upper() for v in values}

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            value = record.get(self.column)

            if value is None or (isinstance(value, str) and value.strip() == ""):
                if self.allow_null:
                    return RuleResult.pass_()
                return RuleResult.fail(
                    f"Column '{self.column}' is null/empty. Expected one of: {sorted(self.accepted)}"
                )

            check_value = str(value).strip()
            if not self.case_sensitive:
                check_value = check_value.upper()

            if check_value not in self.accepted:
                return RuleResult.fail(
                    f"Column '{self.column}' has unexpected value {value!r}. "
                    f"Accepted values: {sorted(self.accepted)}"
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"AcceptedValuesRule internal error: {e}")
```

---

## Step 3 — Implement FreshnessRule

Create `quality/rules/freshness_rule.py`.

```python
# quality/rules/freshness_rule.py

from typing import Dict, Any
from datetime import date, timedelta

from quality.rules.base_rule import BaseRule, RuleResult


class FreshnessRule(BaseRule):
    """
    Validates that a date column is not older than max_age_days from today.

    The freshness check catches:
    1. Pipelines replaying historical data (dispatch_date from 2 years ago)
    2. Source systems with stuck timestamps (same date for 30 consecutive days)
    3. Off-by-one timezone bugs (dispatch_date of yesterday appears as last year)

    Warning vs Error: freshness violations are usually warnings — a 35-day-old
    date might be a legitimate late correction. Use severity="error" only when
    the business requires strict recency guarantees.
    """

    def __init__(
        self,
        name: str,
        column: str,
        max_age_days: int,
        allow_null: bool = True,
        severity: str = "warning"
    ):
        super().__init__(name, severity)
        self.column = column
        self.max_age_days = max_age_days
        self.allow_null = allow_null

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            raw_value = record.get(self.column)

            if not raw_value or (isinstance(raw_value, str) and raw_value.strip() == ""):
                if self.allow_null:
                    return RuleResult.pass_()
                return RuleResult.fail(f"Column '{self.column}' is null/empty.")

            try:
                parsed_date = date.fromisoformat(str(raw_value).strip())
            except ValueError:
                return RuleResult.fail(
                    f"Column '{self.column}' value {raw_value!r} is not a valid ISO date."
                )

            today = date.today()
            age_days = (today - parsed_date).days

            if age_days > self.max_age_days:
                return RuleResult.fail(
                    f"Column '{self.column}' date {parsed_date} is {age_days} days old "
                    f"(maximum allowed: {self.max_age_days} days). "
                    f"This record may be a replay or from a stuck timestamp."
                )

            if age_days < 0:
                return RuleResult.fail(
                    f"Column '{self.column}' date {parsed_date} is {abs(age_days)} days in the future. "
                    f"Dispatch dates must be in the past."
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"FreshnessRule internal error: {e}")
```

---

## Step 4 — Implement ReferentialRule

Create `quality/rules/referential_rule.py`.

```python
# quality/rules/referential_rule.py

import csv
from typing import Dict, Any, Set
from pathlib import Path

from quality.rules.base_rule import BaseRule, RuleResult


class ReferentialRule(BaseRule):
    """
    Validates that a column's value exists in a reference dataset.

    Equivalent to a foreign key check in SQL, but applied at ingestion time
    before data reaches the database.

    For SwiftShip: validates that origin_city and destination_city exist in
    the known cities reference table. A shipment to "mumbaaaai" (typo) should
    be flagged before it creates an "Unknown" city entry in dim_city.

    The reference set is loaded once at rule construction time — not on every
    record validation. For a reference table with 1,000 cities, this means
    one file read at startup, not 1,000,000 reads for 1,000,000 records.
    """

    def __init__(
        self,
        name: str,
        column: str,
        reference_file: str,
        reference_column: str,
        allow_null: bool = True,
        case_sensitive: bool = False,
        severity: str = "warning"
    ):
        super().__init__(name, severity)
        self.column = column
        self.allow_null = allow_null
        self.case_sensitive = case_sensitive
        self.reference_file = reference_file

        # Load reference values once at construction
        self._reference_values: Set[str] = self._load_reference(reference_file, reference_column)
        print(f"[ReferentialRule] '{name}': loaded {len(self._reference_values)} "
              f"reference values from {reference_file}")

    def _load_reference(self, file_path: str, column: str) -> Set[str]:
        """Load and return the set of valid values from the reference CSV."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Reference file not found: {path}. "
                f"Create it or set severity='warning' to allow this check to be skipped."
            )

        values = set()
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get(column, "").strip()
                if val:
                    values.add(val if self.case_sensitive else val.lower())
        return values

    def validate(self, record: Dict[str, Any]) -> RuleResult:
        try:
            value = record.get(self.column)

            if value is None or (isinstance(value, str) and value.strip() == ""):
                if self.allow_null:
                    return RuleResult.pass_()
                return RuleResult.fail(f"Column '{self.column}' is null/empty.")

            check_value = str(value).strip()
            if not self.case_sensitive:
                check_value = check_value.lower()

            if check_value not in self._reference_values:
                return RuleResult.fail(
                    f"Column '{self.column}' value {value!r} not found in reference table "
                    f"({self.reference_file}). "
                    f"This city is unknown — it will create an 'Unknown' region in dim_city."
                )

            return RuleResult.pass_()

        except Exception as e:
            return RuleResult.fail(f"ReferentialRule internal error: {e}")
```

---

## Step 5 — Register All Rules in ExpectationLoader

Update `quality/expectation_loader.py` to include all new rule types:

```python
# quality/expectation_loader.py  (updated RULE_REGISTRY)

from quality.rules.schema_rule import SchemaRule
from quality.rules.null_rule import NullRule
from quality.rules.range_rule import RangeRule, DateRangeRule
from quality.rules.accepted_values_rule import AcceptedValuesRule
from quality.rules.freshness_rule import FreshnessRule
from quality.rules.referential_rule import ReferentialRule

RULE_REGISTRY = {
    "schema":           SchemaRule,
    "not_null":         NullRule,
    "range":            RangeRule,
    "date_range":       DateRangeRule,
    "accepted_values":  AcceptedValuesRule,
    "freshness":        FreshnessRule,
    "referential":      ReferentialRule,
}
```

Update `_build_rule` to handle the new types:

```python
    def _build_rule(self, rule_class, rule_def: dict) -> BaseRule:
        name = rule_def["name"]
        severity = rule_def.get("severity", "error")
        rule_type = rule_def["type"]

        if rule_type == "schema":
            return rule_class(name=name, required_columns=rule_def["required_columns"], severity=severity)
        elif rule_type == "not_null":
            return rule_class(name=name, column=rule_def["column"], severity=severity)
        elif rule_type == "range":
            return rule_class(
                name=name, column=rule_def["column"],
                min_value=rule_def.get("min"), max_value=rule_def.get("max"),
                allow_null=rule_def.get("allow_null", True), severity=severity
            )
        elif rule_type == "date_range":
            return rule_class(
                name=name, column=rule_def["column"],
                min_date=rule_def.get("min_date"), max_date=rule_def.get("max_date"),
                allow_null=rule_def.get("allow_null", True), severity=severity
            )
        elif rule_type == "accepted_values":
            return rule_class(
                name=name, column=rule_def["column"],
                values=rule_def["values"],
                case_sensitive=rule_def.get("case_sensitive", False),
                allow_null=rule_def.get("allow_null", False), severity=severity
            )
        elif rule_type == "freshness":
            return rule_class(
                name=name, column=rule_def["column"],
                max_age_days=rule_def["max_age_days"],
                allow_null=rule_def.get("allow_null", True), severity=severity
            )
        elif rule_type == "referential":
            return rule_class(
                name=name, column=rule_def["column"],
                reference_file=rule_def["reference_file"],
                reference_column=rule_def["reference_column"],
                allow_null=rule_def.get("allow_null", True), severity=severity
            )
        else:
            raise ValueError(f"No builder defined for rule type: {rule_type}")
```

---

## Step 6 — Create Reference Data

Create `reference_data/valid_cities.csv`:

```csv
city_name,region
mumbai,West
pune,West
ahmedabad,West
delhi,North
bangalore,South
chennai,South
hyderabad,South
kolkata,East
nagpur,Central
bhopal,Central
```

---

## Step 7 — Comprehensive Day 2 Test

Create `data/inputs/day2_test.csv` with violations for each new rule type:

```csv
shipment_id,origin_city,destination_city,dispatch_date,delivery_date,status,weight_kg
SH001,mumbai,delhi,2024-01-10,2024-01-13,DELIVERED,12.5
SH002,mumbai,delhi,2024-01-10,2024-01-13,UNKNOWN_STATUS,12.5
SH003,mumbai,delhi,2024-01-10,2024-01-13,DELIVERED,9999.0
SH004,mumbai,delhi,2020-01-01,2020-01-05,DELIVERED,12.5
SH005,typo_city,delhi,2024-01-10,2024-01-13,DELIVERED,12.5
SH006,mumbai,delhi,2026-06-01,2026-06-04,DELIVERED,12.5
```

Expected failures:
- `SH002` — AcceptedValuesRule (status=UNKNOWN_STATUS)
- `SH003` — RangeRule (weight=9999 exceeds max 500)
- `SH004` — FreshnessRule (dispatch_date is 4+ years old)
- `SH005` — ReferentialRule (origin_city=typo_city not in valid_cities.csv)
- `SH006` — DateRangeRule (dispatch_date is in the future)

---

## Day 2 Checklist

Before moving to Day 3, confirm:

- [ ] `RangeRule` catches `weight_kg=9999` and `weight_kg=-1`
- [ ] `AcceptedValuesRule` catches `status="UNKNOWN_STATUS"` and is case-insensitive (catches `"delivered"` too, since default whitelist is uppercase)
- [ ] `FreshnessRule` catches a dispatch_date that is more than 60 days old
- [ ] `DateRangeRule` catches a future dispatch_date
- [ ] `ReferentialRule` catches `origin_city="typo_city"` not in the reference CSV
- [ ] `ReferentialRule` loads the reference CSV once at construction — not once per record (verify by adding a print statement in `_load_reference`)
- [ ] Running `day2_test.csv` through the engine produces exactly 5 failed records
- [ ] `SH001` (valid record) still passes all rules

---

*Day 3 converts the engine output into an HTML quality report, adds quality score gating, and stores scores in a SQLite trend database so you can track quality degradation over time.*
