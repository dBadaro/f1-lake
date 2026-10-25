"""Constantes do collector — fonte única de verdade."""

import os
from pathlib import Path

from dotenv import load_dotenv

# .env na raiz do repo (gitignored). Override opcional via F1_LAKE_DOTENV;
# silencioso se o arquivo não existir.
_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(os.environ.get("F1_LAKE_DOTENV", str(_REPO_ROOT / ".env")))

# Datasets coletados (sem telemetria bruta)
DATASETS = ["schedule", "results", "laps", "weather"]

# Sessões por evento; inexistente no evento → skip silencioso.
# SQ/S só existem a partir de 2021.
SESSIONS = ["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"]

# Raiz local: data/f1-lake/{dataset}/{year}/{year}_{round:02}_{session}.parquet
# O path remoto (ADLS e volume Databricks) é idêntico ao local, sem o "data/".
RAW_ROOT = "data"
LAKE_PREFIX = "f1-lake"


def local_path(dataset: str, year: int, round_no: int, session: str) -> Path:
    return (Path(RAW_ROOT) / LAKE_PREFIX / dataset / str(year)
            / f"{year}_{round_no:02}_{session}.parquet")


def remote_path(dataset: str, year: int, round_no: int, session: str) -> str:
    """Path remoto (ADLS/volume) = path local sem o prefixo RAW_ROOT."""
    return local_path(dataset, year, round_no, session).relative_to(RAW_ROOT).as_posix()
