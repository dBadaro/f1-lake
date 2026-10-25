"""Espelha o container raw/f1-lake do ADLS para o volume prod.f1_raw.landing.

Uso único: cobre arquivos que chegaram ao ADLS sem os envs do Databricks
(upload best-effort). Idempotente (overwrite=true); rodar de novo é inofensivo.

    python collector/sync_volume.py
"""

import logging
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collector.config import LAKE_PREFIX  # noqa: E402
from collector.upload import VOLUME_URL, _get_container  # noqa: E402

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    host = os.environ.get("DATABRICKS_HOST", "").strip().rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if not (host and token):
        sys.exit("DATABRICKS_HOST e DATABRICKS_TOKEN são obrigatórios (veja .env)")

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    prefix = f"{LAKE_PREFIX}/"

    total = done = 0
    for blob in _get_container().list_blobs(name_starts_with=prefix):
        total += 1
        data = _get_container().get_blob_client(blob).download_blob().readall()
        r = session.put(
            VOLUME_URL.format(host=host, remote=blob.name),
            params={"overwrite": "true"},
            data=data,
            timeout=300,
        )
        r.raise_for_status()
        done += 1
        if done % 100 == 0:
            log.info("%d/%d arquivos sincronizados", done, total)

    print(f"sincronizado: {done} arquivo(s)")


if __name__ == "__main__":
    main()
