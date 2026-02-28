"""Ingest data from relational databases into the S3 raw zone."""

import csv
import io
import json
import logging
from datetime import datetime, timezone

import boto3
import sqlalchemy

logger = logging.getLogger(__name__)


class DBIngestor:
    """Runs a SQL query and writes results to s3://<bucket>/raw/db/."""

    def __init__(self, bucket: str, prefix: str = "raw/db", region: str | None = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def ingest(
        self,
        connection_url: str,
        query: str,
        source_name: str,
        *,
        output_format: str = "jsonl",
    ) -> str:
        """Execute *query* and store results in S3.

        Args:
            connection_url: SQLAlchemy connection string.
            query: SQL SELECT statement.
            source_name: Logical name used in the S3 key path.
            output_format: ``"jsonl"`` (default) or ``"csv"``.

        Returns the S3 key of the written object.
        """
        ingested_at = datetime.now(timezone.utc)
        timestamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

        engine = sqlalchemy.create_engine(connection_url)
        logger.info("Running query for %s", source_name)

        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query))
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        row_count = len(rows)
        logger.info("Fetched %d rows for %s", row_count, source_name)

        ext = output_format
        if output_format == "jsonl":
            body = "\n".join(json.dumps(row, default=str) for row in rows)
        elif output_format == "csv":
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
            body = buf.getvalue()
        else:
            raise ValueError(f"Unsupported format: {output_format}")

        key = f"{self.prefix}/{source_name}/{timestamp}.{ext}"
        metadata = {
            "source": source_name,
            "query": query[:512],
            "ingested_at": ingested_at.isoformat(),
            "row_count": str(row_count),
            "output_format": output_format,
        }

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            Metadata=metadata,
        )

        logger.info("Wrote %s (%d bytes, %d rows)", key, len(body), row_count)
        return key
