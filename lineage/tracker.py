"""Track data lineage events in S3 and DynamoDB."""

import json
import logging
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class LineageTracker:
    """Record lineage events so every dataset can be traced back to its source.

    Stores events as JSONL in ``s3://<bucket>/lineage/`` and optionally
    writes to a DynamoDB table for queryable lookups.
    """

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        dynamodb_table: str | None = None,
    ):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)
        self.dynamodb_table = dynamodb_table
        self.dynamodb = boto3.resource("dynamodb", region_name=region) if dynamodb_table else None

    def record(
        self,
        source_key: str,
        destination_key: str,
        operation: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        extra: dict | None = None,
    ) -> dict:
        """Persist a lineage event linking source to destination.

        Returns the lineage event dict.
        """
        event = {
            "lineage_id": str(uuid.uuid4()),
            "source_key": source_key,
            "destination_key": destination_key,
            "operation": operation,
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": run_id,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            **(extra or {}),
        }

        # Write to S3
        lineage_key = f"lineage/{dag_id}/{run_id}/{event['lineage_id']}.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=lineage_key,
            Body=json.dumps(event, default=str).encode("utf-8"),
        )
        logger.info("Lineage event %s written to %s", event["lineage_id"], lineage_key)

        # Optionally write to DynamoDB
        if self.dynamodb and self.dynamodb_table:
            table = self.dynamodb.Table(self.dynamodb_table)
            table.put_item(Item=event)
            logger.info("Lineage event %s written to DynamoDB %s", event["lineage_id"], self.dynamodb_table)

        return event
