"""
Airflow DAG: data_lake_pipeline

Orchestrates the full data-lake lifecycle:
  1. Ingestion   — pull from APIs, databases, and files into raw/ zone
  2. Transformation — clean and normalize into curated/ zone
  3. Quality      — validate curated data against configurable rules
  4. Lineage      — record source-to-destination links for every step

Every task uses ``on_failure_callback=sns_on_failure`` so failures are
immediately published to the SNS topic stored in the Airflow Variable
``sns_alert_topic_arn``.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from callbacks import sns_on_failure
from ingestion import APIIngestor, DBIngestor, FileIngestor
from lineage import LineageTracker
from quality import QualityChecker
from quality.expectations import ExpectationSuite
from transformation import Transformer

# ── Configuration (Airflow Variables) ────────────────────────────────────────
BUCKET = Variable.get("data_lake_bucket", default_var="my-data-lake-bucket")
REGION = Variable.get("aws_region", default_var="us-east-1")
DYNAMODB_TABLE = Variable.get("lineage_dynamodb_table", default_var="data-lake-lineage-table")

API_SOURCES = Variable.get("api_sources", deserialize_json=True, default_var=[
    {"url": "https://jsonplaceholder.typicode.com/posts", "source_name": "jsonplaceholder_posts"},
    {"url": "https://jsonplaceholder.typicode.com/users", "source_name": "jsonplaceholder_users"},
])

# DB ingestion is disabled by default (no database configured).
# Set the "db_sources" Airflow Variable to enable it.
DB_SOURCES = Variable.get("db_sources", deserialize_json=True, default_var=[])

FILE_SOURCES = Variable.get("file_sources", deserialize_json=True, default_var=[
    {"file_path": "/opt/airflow/data/incoming/sample_sales.jsonl", "source_name": "sample_sales"},
])

QUALITY_RULES = Variable.get("quality_rules", deserialize_json=True, default_var={
    "min_records": 1,
    "required_fields": [],
    "unique_fields": [],
    "no_nulls_in": [],
})

# Per-dataset expectation suites (Great Expectations-style).
# Keys are source_name patterns; values are suite configs.
# Example:
#   {"orders": {"suite_name": "orders_suite", "expectations": [
#       {"expectation": "expect_column_to_exist", "kwargs": {"column": "order_id"}},
#       {"expectation": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
#       {"expectation": "expect_column_values_to_be_between",
#        "kwargs": {"column": "amount", "min_value": 0, "max_value": 100000}}
#   ]}}
QUALITY_SUITES = Variable.get("quality_suites", deserialize_json=True, default_var={})

TRANSFORM_OPTS = Variable.get("transform_options", deserialize_json=True, default_var={
    "deduplicate_key": None,
    "drop_nulls": False,
    "rename_fields": {},
})

# ── DAG defaults ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": sns_on_failure,
}

# ── Task callables ───────────────────────────────────────────────────────────

def ingest_apis(**context):
    """Ingest all configured API sources into raw/api/."""
    ingestor = APIIngestor(bucket=BUCKET, region=REGION)
    keys = []
    for src in API_SOURCES:
        key = ingestor.ingest(
            url=src["url"],
            source_name=src["source_name"],
            headers=src.get("headers"),
            params=src.get("params"),
        )
        keys.append(key)
    context["ti"].xcom_push(key="raw_keys", value=keys)
    return keys


def ingest_databases(**context):
    """Ingest all configured DB sources into raw/db/."""
    ingestor = DBIngestor(bucket=BUCKET, region=REGION)
    keys = []
    for src in DB_SOURCES:
        key = ingestor.ingest(
            connection_url=src["connection_url"],
            query=src["query"],
            source_name=src["source_name"],
            output_format=src.get("output_format", "jsonl"),
        )
        keys.append(key)
    context["ti"].xcom_push(key="raw_keys", value=keys)
    return keys


def ingest_files(**context):
    """Ingest all configured file sources into raw/files/."""
    ingestor = FileIngestor(bucket=BUCKET, region=REGION)
    keys = []
    for src in FILE_SOURCES:
        key = ingestor.ingest(
            file_path=src["file_path"],
            source_name=src["source_name"],
        )
        keys.append(key)
    context["ti"].xcom_push(key="raw_keys", value=keys)
    return keys


def transform_raw(**context):
    """Pull all raw keys from upstream ingestors and transform to curated/."""
    ti = context["ti"]
    raw_keys = []
    for task_id in ("ingest_apis", "ingest_databases", "ingest_files"):
        keys = ti.xcom_pull(task_ids=task_id, key="raw_keys") or []
        raw_keys.extend(keys)

    transformer = Transformer(bucket=BUCKET, region=REGION)
    curated_keys = []
    for raw_key in raw_keys:
        curated_key = transformer.transform(
            raw_key=raw_key,
            deduplicate_key=TRANSFORM_OPTS.get("deduplicate_key"),
            drop_nulls=TRANSFORM_OPTS.get("drop_nulls", False),
            rename_fields=TRANSFORM_OPTS.get("rename_fields"),
        )
        curated_keys.append({"raw_key": raw_key, "curated_key": curated_key})

    ti.xcom_push(key="curated_mappings", value=curated_keys)
    return curated_keys


def run_quality_checks(**context):
    """Validate every curated-zone object.

    For each dataset, if a matching expectation suite exists in the
    ``quality_suites`` Airflow Variable it is used (Great Expectations-style).
    Otherwise the flat ``quality_rules`` quick-check is applied as a fallback.

    Reports are persisted to ``s3://<bucket>/quality_reports/``.
    """
    ti = context["ti"]
    mappings = ti.xcom_pull(task_ids="transform_raw", key="curated_mappings") or []

    checker = QualityChecker(bucket=BUCKET, region=REGION)
    reports = []

    for mapping in mappings:
        curated_key = mapping["curated_key"]

        # Derive source_name from the curated key path for suite lookup
        # Key format: curated/<type>/<source_name>/<timestamp>.jsonl
        parts = curated_key.split("/")
        source_name = parts[2] if len(parts) > 2 else None

        suite_config = QUALITY_SUITES.get(source_name) if source_name else None

        if suite_config:
            report = checker.run_suite_from_config(curated_key, suite_config)
        else:
            report = checker.check(
                s3_key=curated_key,
                min_records=QUALITY_RULES.get("min_records", 1),
                required_fields=QUALITY_RULES.get("required_fields"),
                unique_fields=QUALITY_RULES.get("unique_fields"),
                no_nulls_in=QUALITY_RULES.get("no_nulls_in"),
            )

        reports.append({
            "curated_key": curated_key,
            "suite": report.suite_name,
            "passed": report.passed,
            "total": report.total_expectations,
            "failed": report.failed_expectations,
        })

    ti.xcom_push(key="quality_reports", value=reports)
    return reports


def record_lineage(**context):
    """Write lineage events for every raw → curated transformation."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    mappings = ti.xcom_pull(task_ids="transform_raw", key="curated_mappings") or []

    tracker = LineageTracker(
        bucket=BUCKET,
        region=REGION,
        dynamodb_table=DYNAMODB_TABLE,
    )

    events = []
    for mapping in mappings:
        event = tracker.record(
            source_key=mapping["raw_key"],
            destination_key=mapping["curated_key"],
            operation="transform_raw_to_curated",
            dag_id=dag_run.dag_id,
            task_id="record_lineage",
            run_id=dag_run.run_id,
        )
        events.append(event)

    return events


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="data_lake_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end data lake: ingest → transform → quality → lineage",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-lake", "ingestion", "quality", "lineage"],
) as dag:

    # Stage 1 — Ingestion (parallel)
    t_ingest_apis = PythonOperator(
        task_id="ingest_apis",
        python_callable=ingest_apis,
    )

    t_ingest_dbs = PythonOperator(
        task_id="ingest_databases",
        python_callable=ingest_databases,
    )

    t_ingest_files = PythonOperator(
        task_id="ingest_files",
        python_callable=ingest_files,
    )

    # Stage 2 — Transformation (waits for all ingestors)
    t_transform = PythonOperator(
        task_id="transform_raw",
        python_callable=transform_raw,
    )

    # Stage 3 — Quality checks
    t_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    # Stage 4 — Lineage recording (runs after quality passes)
    t_lineage = PythonOperator(
        task_id="record_lineage",
        python_callable=record_lineage,
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # Ingestors run in parallel, then transform, then quality, then lineage
    [t_ingest_apis, t_ingest_dbs, t_ingest_files] >> t_transform >> t_quality >> t_lineage
