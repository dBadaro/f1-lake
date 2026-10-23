"""Upload duplo: ADLS Gen2 (container raw) + volume Databricks (Files API)."""

import logging
import os
from pathlib import Path

import requests
from azure.storage.blob import BlobServiceClient

log = logging.getLogger(__name__)

CONTAINER = "raw"
VOLUME_URL = "{host}/api/2.0/fs/files/Volumes/main/f1_raw/landing/{remote}"


def upload_adls(local: Path, remote: str) -> None:
    svc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
    svc.get_blob_client(container=CONTAINER, blob=remote).upload_blob(
        local.read_bytes(), overwrite=True
    )


def upload_databricks(local: Path, remote: str) -> None:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    url = VOLUME_URL.format(host=host, remote=remote)
    r = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}"},
        data=local.read_bytes(),
        timeout=300,
    )
    r.raise_for_status()


def upload_pair(local: Path, remote: str) -> None:
    """ADLS sempre; volume Databricks só se as envs existirem (graceful degradation)."""
    upload_adls(local, remote)
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        upload_databricks(local, remote)
    else:
        log.warning("DATABRICKS_HOST/TOKEN ausentes — %s enviado só no ADLS", remote)
