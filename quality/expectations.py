"""Great Expectations-inspired declarative validators for data-lake quality.

Each ``Expectation`` subclass validates a single property of a dataset and
returns a structured ``ExpectationResult``.  Group them into an
``ExpectationSuite`` to validate an entire dataset in one pass.

Suites are JSON-serialisable so they can live in Airflow Variables, config
files, or a database.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ── Result objects ───────────────────────────────────────────────────────────

@dataclass
class ExpectationResult:
    expectation: str
    passed: bool
    detail: str
    kwargs: dict = field(default_factory=dict)


@dataclass
class SuiteResult:
    suite_name: str
    passed: bool
    total: int
    failures: int
    results: list[ExpectationResult] = field(default_factory=list)


# ── Base class ───────────────────────────────────────────────────────────────

class Expectation:
    """Base class — every subclass must implement ``validate``."""

    name: str = "base_expectation"

    def validate(self, records: list[dict]) -> ExpectationResult:
        raise NotImplementedError

    def _pass(self, detail: str = "", **kw: Any) -> ExpectationResult:
        return ExpectationResult(self.name, True, detail, kw)

    def _fail(self, detail: str = "", **kw: Any) -> ExpectationResult:
        return ExpectationResult(self.name, False, detail, kw)


# ── Table-level expectations ─────────────────────────────────────────────────

class ExpectTableRowCountToBeBetween(Expectation):
    name = "expect_table_row_count_to_be_between"

    def __init__(self, min_value: int = 0, max_value: int | None = None):
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, records: list[dict]) -> ExpectationResult:
        count = len(records)
        above_min = count >= self.min_value
        below_max = count <= self.max_value if self.max_value is not None else True
        if above_min and below_max:
            return self._pass(f"Row count {count} in [{self.min_value}, {self.max_value}]")
        return self._fail(
            f"Row count {count} outside [{self.min_value}, {self.max_value}]",
            row_count=count,
        )


# ── Column-level expectations ────────────────────────────────────────────────

class ExpectColumnToExist(Expectation):
    name = "expect_column_to_exist"

    def __init__(self, column: str):
        self.column = column

    def validate(self, records: list[dict]) -> ExpectationResult:
        if not records:
            return self._fail(f"No records to check for column '{self.column}'")
        missing_rows = [i for i, r in enumerate(records) if self.column not in r]
        if not missing_rows:
            return self._pass(f"Column '{self.column}' present in all {len(records)} rows")
        return self._fail(
            f"Column '{self.column}' missing in {len(missing_rows)} of {len(records)} rows",
            missing_rows=missing_rows[:20],
        )


class ExpectColumnValuesToNotBeNull(Expectation):
    name = "expect_column_values_to_not_be_null"

    def __init__(self, column: str):
        self.column = column

    def validate(self, records: list[dict]) -> ExpectationResult:
        null_count = sum(1 for r in records if r.get(self.column) is None)
        if null_count == 0:
            return self._pass(f"Column '{self.column}' has no nulls")
        return self._fail(
            f"Column '{self.column}' has {null_count} null(s) in {len(records)} rows",
            null_count=null_count,
        )


class ExpectColumnValuesToBeUnique(Expectation):
    name = "expect_column_values_to_be_unique"

    def __init__(self, column: str):
        self.column = column

    def validate(self, records: list[dict]) -> ExpectationResult:
        vals = [r.get(self.column) for r in records]
        dupes = len(vals) - len(set(vals))
        if dupes == 0:
            return self._pass(f"Column '{self.column}' is unique")
        return self._fail(
            f"Column '{self.column}' has {dupes} duplicate(s)",
            duplicate_count=dupes,
        )


class ExpectColumnValuesToBeInSet(Expectation):
    name = "expect_column_values_to_be_in_set"

    def __init__(self, column: str, value_set: list[Any]):
        self.column = column
        self.value_set = set(value_set)

    def validate(self, records: list[dict]) -> ExpectationResult:
        bad = [
            (i, r.get(self.column))
            for i, r in enumerate(records)
            if r.get(self.column) not in self.value_set
        ]
        if not bad:
            return self._pass(f"All values in '{self.column}' are in allowed set")
        return self._fail(
            f"{len(bad)} value(s) in '{self.column}' not in allowed set",
            invalid_samples=bad[:10],
        )


class ExpectColumnValuesToMatchRegex(Expectation):
    name = "expect_column_values_to_match_regex"

    def __init__(self, column: str, regex: str):
        self.column = column
        self.regex = regex
        self._pattern = re.compile(regex)

    def validate(self, records: list[dict]) -> ExpectationResult:
        bad = [
            (i, r.get(self.column))
            for i, r in enumerate(records)
            if r.get(self.column) is not None and not self._pattern.search(str(r[self.column]))
        ]
        if not bad:
            return self._pass(f"All values in '{self.column}' match /{self.regex}/")
        return self._fail(
            f"{len(bad)} value(s) in '{self.column}' don't match /{self.regex}/",
            invalid_samples=bad[:10],
        )


class ExpectColumnValuesToBeBetween(Expectation):
    name = "expect_column_values_to_be_between"

    def __init__(self, column: str, min_value: float | None = None, max_value: float | None = None):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, records: list[dict]) -> ExpectationResult:
        bad = []
        for i, r in enumerate(records):
            val = r.get(self.column)
            if val is None:
                continue
            try:
                num = float(val)
            except (TypeError, ValueError):
                bad.append((i, val, "not numeric"))
                continue
            if self.min_value is not None and num < self.min_value:
                bad.append((i, val, f"< {self.min_value}"))
            elif self.max_value is not None and num > self.max_value:
                bad.append((i, val, f"> {self.max_value}"))
        if not bad:
            return self._pass(
                f"All values in '{self.column}' in [{self.min_value}, {self.max_value}]"
            )
        return self._fail(
            f"{len(bad)} value(s) in '{self.column}' out of range",
            invalid_samples=bad[:10],
        )


class ExpectColumnValuesToBeOfType(Expectation):
    name = "expect_column_values_to_be_of_type"

    _TYPE_MAP = {"str": str, "int": int, "float": (int, float), "bool": bool, "list": list, "dict": dict}

    def __init__(self, column: str, expected_type: str):
        self.column = column
        self.expected_type = expected_type

    def validate(self, records: list[dict]) -> ExpectationResult:
        py_type = self._TYPE_MAP.get(self.expected_type)
        if py_type is None:
            return self._fail(f"Unknown type '{self.expected_type}'")
        bad = [
            (i, type(r.get(self.column)).__name__)
            for i, r in enumerate(records)
            if r.get(self.column) is not None and not isinstance(r[self.column], py_type)
        ]
        if not bad:
            return self._pass(f"All values in '{self.column}' are {self.expected_type}")
        return self._fail(
            f"{len(bad)} value(s) in '{self.column}' are not {self.expected_type}",
            invalid_samples=bad[:10],
        )


class ExpectColumnMeanToBeBetween(Expectation):
    name = "expect_column_mean_to_be_between"

    def __init__(self, column: str, min_value: float | None = None, max_value: float | None = None):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, records: list[dict]) -> ExpectationResult:
        nums = []
        for r in records:
            val = r.get(self.column)
            if val is None:
                continue
            try:
                nums.append(float(val))
            except (TypeError, ValueError):
                pass
        if not nums:
            return self._fail(f"No numeric values in '{self.column}'")
        mean = statistics.mean(nums)
        above = mean >= self.min_value if self.min_value is not None else True
        below = mean <= self.max_value if self.max_value is not None else True
        if above and below:
            return self._pass(f"Mean of '{self.column}' is {mean:.4f}")
        return self._fail(
            f"Mean of '{self.column}' is {mean:.4f}, expected [{self.min_value}, {self.max_value}]",
            actual_mean=mean,
        )


class ExpectDatasetToBeRecent(Expectation):
    name = "expect_dataset_to_be_recent"

    def __init__(self, timestamp_column: str, max_age_hours: float = 24):
        self.timestamp_column = timestamp_column
        self.max_age_hours = max_age_hours

    def validate(self, records: list[dict]) -> ExpectationResult:
        if not records:
            return self._fail("No records to check for recency")
        now = datetime.now(timezone.utc)
        stale = 0
        for r in records:
            ts_str = r.get(self.timestamp_column)
            if ts_str is None:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_str))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_hours = (now - ts).total_seconds() / 3600
                if age_hours > self.max_age_hours:
                    stale += 1
            except (TypeError, ValueError):
                stale += 1
        if stale == 0:
            return self._pass(
                f"All rows in '{self.timestamp_column}' within {self.max_age_hours}h"
            )
        return self._fail(
            f"{stale} row(s) older than {self.max_age_hours}h in '{self.timestamp_column}'",
            stale_count=stale,
        )


# ── Expectation Suite ────────────────────────────────────────────────────────

# Registry used by ``ExpectationSuite.from_config`` to build expectations
# from their JSON names.
EXPECTATION_REGISTRY: dict[str, type[Expectation]] = {
    "expect_table_row_count_to_be_between": ExpectTableRowCountToBeBetween,
    "expect_column_to_exist": ExpectColumnToExist,
    "expect_column_values_to_not_be_null": ExpectColumnValuesToNotBeNull,
    "expect_column_values_to_be_unique": ExpectColumnValuesToBeUnique,
    "expect_column_values_to_be_in_set": ExpectColumnValuesToBeInSet,
    "expect_column_values_to_match_regex": ExpectColumnValuesToMatchRegex,
    "expect_column_values_to_be_between": ExpectColumnValuesToBeBetween,
    "expect_column_values_to_be_of_type": ExpectColumnValuesToBeOfType,
    "expect_column_mean_to_be_between": ExpectColumnMeanToBeBetween,
    "expect_dataset_to_be_recent": ExpectDatasetToBeRecent,
}


class ExpectationSuite:
    """A named collection of expectations to run against a single dataset."""

    def __init__(self, name: str, expectations: list[Expectation] | None = None):
        self.name = name
        self.expectations: list[Expectation] = expectations or []

    def add(self, expectation: Expectation) -> None:
        self.expectations.append(expectation)

    def run(self, records: list[dict]) -> SuiteResult:
        results = [exp.validate(records) for exp in self.expectations]
        failures = sum(1 for r in results if not r.passed)
        return SuiteResult(
            suite_name=self.name,
            passed=failures == 0,
            total=len(results),
            failures=failures,
            results=results,
        )

    @classmethod
    def from_config(cls, config: dict) -> ExpectationSuite:
        """Build a suite from a JSON-serialisable dict.

        Expected format::

            {
                "suite_name": "orders_suite",
                "expectations": [
                    {
                        "expectation": "expect_column_to_exist",
                        "kwargs": {"column": "order_id"}
                    },
                    {
                        "expectation": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "order_id"}
                    },
                    {
                        "expectation": "expect_column_values_to_be_between",
                        "kwargs": {"column": "amount", "min_value": 0}
                    }
                ]
            }
        """
        suite = cls(name=config["suite_name"])
        for entry in config.get("expectations", []):
            exp_name = entry["expectation"]
            exp_cls = EXPECTATION_REGISTRY.get(exp_name)
            if exp_cls is None:
                raise ValueError(f"Unknown expectation: {exp_name}")
            suite.add(exp_cls(**entry.get("kwargs", {})))
        return suite
