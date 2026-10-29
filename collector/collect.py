"""Extração FastF1 → parquet local, por dataset."""

import logging
import time
from collections.abc import Iterator
from pathlib import Path

import fastf1
import pandas as pd
from fastf1.exceptions import RateLimitExceededError

from collector import upload
from collector.config import SESSIONS, local_path, remote_path

log = logging.getLogger(__name__)

CACHE_DIR = "./cache"
SESSION_SLEEP_S = 1  # cortesia entre sessões; o pacing real é no fastf1.req
RATE_LIMIT_WAIT_S = 10 * 60  # espera ao estourar o contador de chamadas/h
RATE_LIMIT_RETRIES = 10

# pacing sob os limites que o próprio fastf1 impõe (fastf1/req.py):
# Jolpica 200/h, demais APIs (livetiming real + mirror) 500/h
PACE_JOPLICA_S = 3600 / 190
PACE_GENERAL_S = 3600 / 450


def enable_cache() -> None:
    Path(CACHE_DIR).mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)
    pace_apis()


def pace_apis() -> None:
    """Espalha as chamadas HTTP do FastF1 abaixo dos limites por hora.

    Sem isso o collector estoura o contador em minutos e entra no ciclo
    RateLimitExceeded → sleep → estoura de novo (foi o que consumiu 5h15 de
    sleep no run de 2023 cancelado no timeout). Ajusta o intervalo mínimo das
    instâncias privadas `_MinIntervalLimitDelay` da 3.9.0a0 (pinned no
    requirements); se a estrutura interna mudar, segue com o padrão.
    """
    try:
        from fastf1 import req as ff_req

        for pattern, limiters in ff_req._SessionWithRateLimiting._RATE_LIMITS.items():
            interval = PACE_JOPLICA_S if "jolpi" in pattern.pattern else PACE_GENERAL_S
            for lim in limiters:
                if lim.__class__.__name__ == "_MinIntervalLimitDelay":
                    lim._interval = interval
    except Exception:
        log.warning("pacing do fastf1.req indisponível — usando limites padrão",
                    exc_info=True)


def _save(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    # coerce_timestamps: pandas grava ns, e o leitor Parquet do Spark/Databricks
    # só aceita us — sem isto, colunas datetime viram timestamp[ns] ilegíveis
    df.to_parquet(path, index=False, coerce_timestamps="us")
    log.info("salvo: %s (%d linhas)", path, len(df))
    return path


def _event_schedule(year: int):
    """Calendário do ano, sem testes (RoundNumber sempre presente)."""
    return fastf1.get_event_schedule(year, include_testing=False)


def _rounds(year: int) -> list[int]:
    """RoundNumbers do calendário do ano."""
    return [int(r) for r in _event_schedule(year)["RoundNumber"].dropna()]


def _load_session(year: int, round_no: int, name: str, need_laps: bool, need_weather: bool):
    """Carrega a sessão; rate limit → espera e tenta de novo; outro erro → None.

    Carrega só o que o dataset pede: evita baixar timing (lento, WAF da F1
    bloqueia IP de datacenter) quando o dataset é results/weather.
    """
    for attempt in range(RATE_LIMIT_RETRIES + 1):
        try:
            session = fastf1.get_session(year, round_no, name)
            session.load(telemetry=False, weather=need_weather, laps=need_laps,
                         messages=False)
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


def collect_schedule(year: int) -> Iterator[Path]:
    """1 linha/evento — um parquet por evento do calendário."""
    calendar = _event_schedule(year)
    for _, event in calendar.iterrows():
        round_no = int(event["RoundNumber"])
        df = event.to_frame().T.reset_index(drop=True)
        yield _save(df, local_path("schedule", year, round_no, "schedule"))


def collect_per_session(year: int, dataset: str, skip_existing: bool = True) -> Iterator[Path]:
    """results/laps/weather: 1 parquet por sessão existente.

    Com skip_existing, sessões cujo parquet já está no ADLS não são nem
    carregadas — re-runs de backfill retomam de onde pararam.
    """
    # FastF1 >= 3.9: session.weather virou session.weather_data
    attr = {"results": "results", "laps": "laps", "weather": "weather_data"}[dataset]
    need_laps, need_weather = dataset == "laps", dataset == "weather"
    for round_no in _rounds(year):
        for name in SESSIONS:
            remote = remote_path(dataset, year, round_no, name)
            if skip_existing and upload.blob_exists(remote):
                log.info("existe no ADLS, pulando: %s", remote)
                continue
            session = _load_session(year, round_no, name, need_laps, need_weather)
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
            yield _save(df, local_path(dataset, year, round_no, name))
            time.sleep(SESSION_SLEEP_S)


def collect(year: int, dataset: str, skip_existing: bool = True) -> Iterator[Path]:
    if dataset == "schedule":
        yield from collect_schedule(year)
    else:
        yield from collect_per_session(year, dataset, skip_existing)
