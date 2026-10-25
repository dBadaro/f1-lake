"""CLI do collector: python collector/main.py --years 2024 --datasets schedule results"""

import argparse
import logging
import sys
from pathlib import Path

# permite rodar como script solto: python collector\main.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collector import collect  # noqa: E402
from collector.config import DATASETS, RAW_ROOT  # noqa: E402
from collector.upload import upload_pair  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Coleta F1 (FastF1) → parquet → ADLS + volume")
    parser.add_argument("--years", nargs="+", type=int, required=True,
                        help="Anos a coletar, ex: --years 2018 2019 2020")
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, default=DATASETS,
                        help="Datasets a coletar (default: todos)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Recoleta mesmo se o parquet já existir no ADLS "
                             "(default: pular sessões já coletadas)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    collect.enable_cache()

    counts: dict[tuple[int, str], int] = {}
    for year in args.years:
        for dataset in args.datasets:
            n = 0
            # upload por arquivo, imediato: timeout/crash não perde trabalho
            for path in collect.collect(year, dataset, skip_existing=not args.overwrite):
                remote = path.relative_to(RAW_ROOT).as_posix()
                upload_pair(path, remote)
                n += 1
            counts[(year, dataset)] = n

    print("\n=== Resumo ===")
    for (year, dataset), n in sorted(counts.items()):
        print(f"{year} {dataset:<10} {n} arquivo(s)")
    total = sum(counts.values())
    print(f"total: {total} arquivo(s)")


if __name__ == "__main__":
    main()
