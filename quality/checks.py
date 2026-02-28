"""Data quality checker that runs expectation suites against S3 objects.

Supports two modes:

1. **Suite mode** — pass an ``ExpectationSuite`` (built from JSON config) for
   fine-grained, Great Expectations-style validation.
2. **Quick-check mode** — the original ``check()`` interface with keyword args
   for simple one-off validation.

Both modes produce a ``QualityReport``, persist it to S3, and raise on failure
so Airflow marks the task as failed.
"""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import boto3

from .expectations import (
    ExpectationSuite,
    ExpectColumnToExist,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableRowCountToBeBetween,
    SuiteResult,
)

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    """Results of a quality check run."""

    s3_key: str
    suite_name: str
    checked_at: str
    record_count: int
    passed: bool
    total_expectations: int = 0
    failed_expectations: int = 0
    failures: list[str] = field(default_factory=list)
    results: list[dict] = field(default_factory=list)


class QualityChecker:
    """Run quality validations against S3 JSONL objects."""

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        report_prefix: str = "quality_reports",
    ):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)
        self.report_prefix = report_prefix.strip("/")

    # ── Suite-based validation ───────────────────────────────────────────

    def run_suite(
        self,
        s3_key: str,
        suite: ExpectationSuite,
    ) -> QualityReport:
        """Run a full expectation suite against an S3 JSONL object.

        Persists the report to S3 and raises ``ValueError`` on failure.
        """
        records = self._load_records(s3_key)
        suite_result: SuiteResult = suite.run(records)

        report = QualityReport(
            s3_key=s3_key,
            suite_name=suite.name,
            checked_at=datetime.now(timezone.utc).isoformat(),
            record_count=len(records),
            passed=suite_result.passed,
            total_expectations=suite_result.total,
            failed_expectations=suite_result.failures,
            failures=[r.detail for r in suite_result.results if not r.passed],
            results=[
                {
                    "expectation": r.expectation,
                    "passed": r.passed,
                    "detail": r.detail,
                    **r.kwargs,
                }
                for r in suite_result.results
            ],
        )

        self._persist_report(report)

        if not report.passed:
            logger.error(
                "Suite '%s' FAILED for %s: %d/%d expectations failed — %s",
                suite.name, s3_key, report.failed_expectations,
                report.total_expectations, report.failures,
            )
            raise ValueError(
                f"Quality suite '{suite.name}' failed for {s3_key}: {report.failures}"
            )

        logger.info(
            "Suite '%s' PASSED for %s — %d expectations OK (%d records)",
            suite.name, s3_key, report.total_expectations, report.record_count,
        )
        return report

    def run_suite_from_config(self, s3_key: str, suite_config: dict) -> QualityReport:
        """Build a suite from a JSON config dict and run it.

        Convenience wrapper for ``run_suite`` + ``ExpectationSuite.from_config``.
        """
        suite = ExpectationSuite.from_config(suite_config)
        return self.run_suite(s3_key, suite)

    # ── Quick-check mode (backwards compatible) ──────────────────────────

    def check(
        self,
        s3_key: str,
        *,
        min_records: int = 1,
        required_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
        no_nulls_in: list[str] | None = None,
    ) -> QualityReport:
        """Run simple checks via keyword arguments (original interface).

        Internally builds a one-off suite and delegates to ``run_suite``.
        """
        suite = ExpectationSuite(name="quick_check")
        suite.add(ExpectTableRowCountToBeBetween(min_value=min_records))
        for col in required_fields or []:
            suite.add(ExpectColumnToExist(column=col))
        for col in unique_fields or []:
            suite.add(ExpectColumnValuesToBeUnique(column=col))
        for col in no_nulls_in or []:
            suite.add(ExpectColumnValuesToNotBeNull(column=col))
        return self.run_suite(s3_key, suite)

    # ── Internals ────────────────────────────────────────────────────────

    def _load_records(self, s3_key: str) -> list[dict]:
        logger.info("Loading %s", s3_key)
        obj = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
        body = obj["Body"].read().decode("utf-8")
        return [json.loads(line) for line in body.strip().splitlines() if line.strip()]

    def _persist_report(self, report: QualityReport) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_key = (
            f"{self.report_prefix}/{report.suite_name}/{timestamp}.json"
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=report_key,
            Body=json.dumps(asdict(report), indent=2, default=str).encode("utf-8"),
            Metadata={
                "suite_name": report.suite_name,
                "passed": str(report.passed),
                "checked_at": report.checked_at,
            },
        )
        logger.info("Quality report persisted to %s", report_key)
        return report_key
