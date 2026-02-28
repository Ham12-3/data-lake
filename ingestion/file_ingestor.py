"""Ingest local or remote files into the S3 raw zone."""

import logging
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


class FileIngestor:
    """Uploads files from the local filesystem to s3://<bucket>/raw/files/."""

    def __init__(self, bucket: str, prefix: str = "raw/files", region: str | None = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def ingest(self, file_path: str | Path, source_name: str) -> str:
        """Upload a single file to S3.

        Returns the S3 key of the written object.
        """
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"No such file: {file_path}")

        ingested_at = datetime.now(timezone.utc)
        timestamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

        ext = file_path.suffix or ""
        key = f"{self.prefix}/{source_name}/{timestamp}{ext}"

        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        file_size = file_path.stat().st_size

        metadata = {
            "source": source_name,
            "original_filename": file_path.name,
            "ingested_at": ingested_at.isoformat(),
            "file_size_bytes": str(file_size),
            "content_type": content_type,
        }

        logger.info("Uploading %s (%d bytes)", file_path, file_size)
        self.s3.upload_file(
            str(file_path),
            self.bucket,
            key,
            ExtraArgs={"Metadata": metadata},
        )

        logger.info("Wrote %s", key)
        return key

    def ingest_directory(self, directory: str | Path, source_name: str) -> list[str]:
        """Upload all files in a directory (non-recursive) to S3.

        Returns a list of S3 keys written.
        """
        directory = Path(directory)
        if not directory.is_dir():
            raise NotADirectoryError(f"Not a directory: {directory}")

        keys = []
        for entry in sorted(directory.iterdir()):
            if entry.is_file():
                key = self.ingest(entry, source_name)
                keys.append(key)

        logger.info("Uploaded %d files from %s", len(keys), directory)
        return keys
