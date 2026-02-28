"""Transform raw-zone data and write to the curated zone in S3."""

import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class Transformer:
    """Read from raw/, apply transforms, write to curated/."""

    def __init__(self, bucket: str, region: str | None = None):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)

    def transform(
        self,
        raw_key: str,
        curated_prefix: str = "curated",
        *,
        deduplicate_key: str | None = None,
        drop_nulls: bool = False,
        rename_fields: dict[str, str] | None = None,
    ) -> str:
        """Apply lightweight transforms to a raw-zone object.

        Supports deduplication, null removal, and field renaming.
        Returns the curated-zone S3 key.
        """
        logger.info("Reading raw object %s", raw_key)
        obj = self.s3.get_object(Bucket=self.bucket, Key=raw_key)
        body = obj["Body"].read().decode("utf-8")
        raw_metadata = obj.get("Metadata", {})

        records = self._parse(body)

        if deduplicate_key:
            seen = set()
            deduped = []
            for rec in records:
                val = rec.get(deduplicate_key)
                if val not in seen:
                    seen.add(val)
                    deduped.append(rec)
            logger.info("Deduplicated %d → %d rows on '%s'", len(records), len(deduped), deduplicate_key)
            records = deduped

        if drop_nulls:
            records = [
                {k: v for k, v in rec.items() if v is not None}
                for rec in records
            ]

        if rename_fields:
            records = [
                {rename_fields.get(k, k): v for k, v in rec.items()}
                for rec in records
            ]

        transformed_at = datetime.now(timezone.utc)
        timestamp = transformed_at.strftime("%Y%m%dT%H%M%SZ")

        # Mirror the raw path structure under curated/
        raw_relative = raw_key.split("/", 1)[1] if "/" in raw_key else raw_key
        curated_key = f"{curated_prefix}/{raw_relative}".rsplit(".", 1)[0] + f"_{timestamp}.jsonl"

        output = "\n".join(json.dumps(rec, default=str) for rec in records)

        metadata = {
            **raw_metadata,
            "transformed_at": transformed_at.isoformat(),
            "record_count": str(len(records)),
            "raw_source_key": raw_key,
        }

        self.s3.put_object(
            Bucket=self.bucket,
            Key=curated_key,
            Body=output.encode("utf-8"),
            Metadata=metadata,
        )

        logger.info("Wrote curated object %s (%d records)", curated_key, len(records))
        return curated_key

    @staticmethod
    def _parse(body: str) -> list[dict]:
        """Parse JSONL or JSON array from a raw object body."""
        stripped = body.strip()
        if stripped.startswith("["):
            return json.loads(stripped)
        return [json.loads(line) for line in stripped.splitlines() if line.strip()]
