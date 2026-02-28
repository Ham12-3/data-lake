"""Version curated datasets with DVC after transformation.

Typical usage inside an Airflow task or standalone script::

    versioner = DatasetVersioner(
        bucket="my-data-lake",
        repo_root="/opt/airflow/data-lake",
    )
    info = versioner.version_dataset(
        s3_key="curated/orders/20240601T120000Z.jsonl",
        dataset_name="orders",
    )
    # info = {"dataset_name": "orders", "dvc_file": "...", "version_tag": "...", ...}
"""

import hashlib
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


class DatasetVersioner:
    """Download a curated S3 object, track it with DVC, and push to the remote.

    Parameters
    ----------
    bucket:
        S3 bucket that holds curated-zone data.
    repo_root:
        Local path to the git/DVC repo root.
    staging_dir:
        Subdirectory (relative to *repo_root*) where datasets are staged
        before ``dvc add``.  Defaults to ``data/staged``.
    region:
        AWS region for the S3 client.
    """

    def __init__(
        self,
        bucket: str,
        repo_root: str | Path,
        staging_dir: str = "data/staged",
        region: str | None = None,
    ):
        self.bucket = bucket
        self.repo_root = Path(repo_root)
        self.staging_dir = self.repo_root / staging_dir
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.s3 = boto3.client("s3", region_name=region)

    def version_dataset(
        self,
        s3_key: str,
        dataset_name: str,
        *,
        git_tag: bool = True,
        commit_message: str | None = None,
    ) -> dict:
        """Download, track, and push a curated dataset.

        Steps
        -----
        1. Download the S3 object into ``<staging_dir>/<dataset_name>/``.
        2. ``dvc add`` the file so DVC creates/updates the ``.dvc`` file.
        3. ``dvc push`` to upload the data to the configured DVC remote.
        4. ``git add`` + ``git commit`` the ``.dvc`` and ``.gitignore`` changes.
        5. Optionally ``git tag`` with a version string.

        Returns a metadata dict describing the versioned dataset.
        """
        versioned_at = datetime.now(timezone.utc)
        timestamp = versioned_at.strftime("%Y%m%dT%H%M%SZ")

        # ── 1. Download from S3 ─────────────────────────────────────────
        dataset_dir = self.staging_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        filename = Path(s3_key).name
        local_path = dataset_dir / filename

        logger.info("Downloading s3://%s/%s → %s", self.bucket, s3_key, local_path)
        self.s3.download_file(self.bucket, s3_key, str(local_path))

        md5 = self._md5(local_path)
        file_size = local_path.stat().st_size

        # ── 2. dvc add ──────────────────────────────────────────────────
        rel_path = local_path.relative_to(self.repo_root)
        logger.info("Running dvc add %s", rel_path)
        self._run(["dvc", "add", str(rel_path)])

        # ── 3. dvc push ─────────────────────────────────────────────────
        logger.info("Pushing to DVC remote")
        self._run(["dvc", "push"])

        # ── 4. git add + commit ─────────────────────────────────────────
        dvc_file = str(rel_path) + ".dvc"
        gitignore = str(rel_path.parent / ".gitignore")
        msg = commit_message or f"data: version {dataset_name} ({timestamp})"

        self._run(["git", "add", dvc_file, gitignore])
        self._run(["git", "commit", "-m", msg])

        # ── 5. Optional git tag ─────────────────────────────────────────
        version_tag = f"{dataset_name}/v-{timestamp}"
        if git_tag:
            logger.info("Tagging %s", version_tag)
            self._run(["git", "tag", version_tag])

        # ── 6. Write sidecar metadata ───────────────────────────────────
        meta = {
            "dataset_name": dataset_name,
            "s3_source_key": s3_key,
            "local_path": str(rel_path),
            "dvc_file": dvc_file,
            "version_tag": version_tag,
            "md5": md5,
            "file_size_bytes": file_size,
            "versioned_at": versioned_at.isoformat(),
        }
        meta_path = dataset_dir / f"{filename}.meta.json"
        meta_path.write_text(json.dumps(meta, indent=2))
        logger.info("Version metadata written to %s", meta_path)

        return meta

    # ── helpers ──────────────────────────────────────────────────────────────

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess:
        """Run a shell command from the repo root."""
        result = subprocess.run(
            cmd,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout:
            logger.debug("%s stdout: %s", cmd[0], result.stdout.strip())
        return result

    @staticmethod
    def _md5(path: Path) -> str:
        """Compute MD5 hex digest of a file."""
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
