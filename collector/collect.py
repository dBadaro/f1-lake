"""Extração FastF1 → parquet local, por dataset."""

import logging
import time
from pathlib import Path

import fastf1
import pandas as pd
from fastf1.exceptions import RateLimitExceededError

from collector.config import SESSIONS, local_path

log = logging.getLogger(__name__)

CACHE_DIR = "./cache"
SESSION_SLEEP_S = 2  # rate limit Jolpica
RATE_LIMIT_WAIT_S = 15 * 60  # espera ao bater 500 calls/h antes de tentar de novo
RATE_LIMIT_RETRIES = 18      # ~4,5 h de espera no pior caso (timeout do runner: 5,5 h)


def enable_cache() -> None:
    Path(CACHE_DIR).mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)


def _save(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)  # idempotente: mesmo nome sobrescreve
    log.info("salvo: %s (%d linhas)", path, len(df))
    return path


def _event_schedule(year: int):
    """Calendário do ano, sem testes (RoundNumber sempre presente)."""
    return fastf1.get_event_schedule(year, include_testing=False)


def _rounds(year: int) -> list[int]:
    """RoundNumbers do calendário do ano."""
    return [int(r) for r in _event_schedule(year)["RoundNumber"].dropna()]


def _load_session(year: int, round_no: int, name: str):
    """Carrega a sessão; rate limit → espera e tenta de novo; outro erro → None."""
    for attempt in range(RATE_LIMIT_RETRIES + 1):
        try:
            session = fastf1.get_session(year, round_no, name)
            session.load(telemetry=False, weather=True, messages=False)
            return session
        except RateLimitExceededError:
            if attempt == RATE_LIMIT_RETRIES:
                log.warning("skip %d r%02d %s: rate limit persistente", year, round_no, name)
                return None
            log.warning(
                "rate limit %d r%02d %s — aguardando %d min (tentativa %d/%d)",
                year, round_no, name, RATE_LIMIT_WAIT_S // 60, attempt + 1, RATE_LIMIT_RETRIES,
            )
            time.sleep(RATE_LIMIT_WAIT_S)
        except Exception as exc:
            log.warning("skip %d r%02d %s: %s", year, round_no, name, exc)
            return None


def collect_schedule(year: int) -> list[Path]:
    """1 linha/evento — um parquet por evento do calendário."""
    calendar = _event_schedule(year)
    saved = []
    for _, event in calendar.iterrows():
        round_no = int(event["RoundNumber"])
        df = event.to_frame().T.reset_index(drop=True)
        saved.append(_save(df, local_path("schedule", year, round_no, "schedule")))
    return saved


def collect_per_session(year: int, dataset: str) -> list[Path]:
    """results/laps/weather: 1 parquet por sessão existente."""
    attr = {"results": "results", "laps": "laps", "weather": "weather"}[dataset]
    saved = []
    for round_no in _rounds(year):
        for name in SESSIONS:
            session = _load_session(year, round_no, name)
            if session is None:
                continue
            try:
                df = getattr(session, attr)
            except Exception as exc:
                # ex. DataNotLoadedError quando o livetiming falha no load
                log.warning("skip %d r%02d %s (%s): %s", year, round_no, name, dataset, exc)
                continue
            if df is None or len(df) == 0:
                continue
            if dataset == "laps":
                df = df.reset_index(drop=True)
            saved.append(_save(df, local_path(dataset, year, round_no, name)))
            time.sleep(SESSION_SLEEP_S)
    return saved


def collect(year: int, dataset: str) -> list[Path]:
    if dataset == "schedule":
        return collect_schedule(year)
    return collect_per_session(year, dataset)
