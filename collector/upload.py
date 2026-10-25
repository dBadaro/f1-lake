"""Upload duplo: ADLS Gen2 (container raw) + volume Databricks (Files API)."""

import logging
import os
from pathlib import Path

import requests
from azure.storage.blob import BlobServiceClient, ContainerClient

log = logging.getLogger(__name__)

CONTAINER = "raw"
VOLUME_URL = "{host}/api/2.0/fs/files/Volumes/prod/f1_raw/landing/{remote}"

_container: ContainerClient | None = None


def _get_container() -> ContainerClient:
    global _container
    if _container is None:
        svc = BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"])
        _container = svc.get_container_client(CONTAINER)
    return _container


def blob_exists(remote: str) -> bool:
    return _get_container().get_blob_client(blob=remote).exists()


def upload_adls(local: Path, remote: str) -> None:
    _get_container().get_blob_client(blob=remote).upload_blob(
        local.read_bytes(), overwrite=True
    )


def upload_databricks(local: Path, remote: str) -> None:
    host = os.environ["DATABRICKS_HOST"].strip().rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"].strip()
    url = VOLUME_URL.format(host=host, remote=remote)
    r = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}"},
        data=local.read_bytes(),
        timeout=300,
    )
    r.raise_for_status()


def upload_pair(local: Path, remote: str) -> None:
    """ADLS sempre; volume Databricks é best-effort (falha não derruba o job)."""
    upload_adls(local, remote)
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        try:
            upload_databricks(local, remote)
        except Exception:
            log.warning("upload Databricks falhou para %s — enviado só no ADLS", remote,
                        exc_info=True)
    else:
        log.warning("DATABRICKS_HOST/TOKEN ausentes — %s enviado só no ADLS", remote)
