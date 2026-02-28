"""Ingest data from REST APIs into the S3 raw zone."""

import json
import logging
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger(__name__)


class APIIngestor:
    """Pulls data from REST APIs and writes JSON to s3://<bucket>/raw/api/."""

    def __init__(self, bucket: str, prefix: str = "raw/api", region: str | None = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def ingest(
        self,
        url: str,
        source_name: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        timeout: int = 30,
    ) -> str:
        """Fetch a URL and store the response in S3.

        Returns the S3 key of the written object.
        """
        ingested_at = datetime.now(timezone.utc)
        timestamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

        logger.info("Fetching %s from %s", source_name, url)
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()

        payload = response.json() if "json" in response.headers.get("content-type", "") else response.text

        key = f"{self.prefix}/{source_name}/{timestamp}.json"
        metadata = {
            "source": source_name,
            "source_url": url,
            "ingested_at": ingested_at.isoformat(),
            "http_status": str(response.status_code),
            "content_type": response.headers.get("content-type", ""),
        }

        body = json.dumps(payload) if not isinstance(payload, str) else payload
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            Metadata=metadata,
        )

        logger.info("Wrote %s (%d bytes)", key, len(body))
        return key
