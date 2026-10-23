"""Inspeciona o lake F1 com DuckDB — local (espelho do raw) ou ADLS remoto.

Uso (na raiz do repo):
  python scripts/inspeciona.py                       # cria/atualiza lake.duckdb com views dos parquets locais
  python scripts/inspeciona.py --sql "SELECT ..."    # roda SQL contra lake.duckdb
  python scripts/inspeciona.py --repl                # shell Python com `con` (duckdb) e helpers prontos
  python scripts/inspeciona.py --list [prefixo]      # lista blobs no container raw do ADLS (default: f1-lake/)
"""

import argparse
import code
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # permite importar collector

from collector.config import DATASETS, LAKE_PREFIX, RAW_ROOT  # noqa: E402

DB_PATH = ROOT / "lake.duckdb"


def _views(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Cria uma view por dataset sobre o glob de parquets locais; retorna contagens."""
    counts = {}
    for ds in DATASETS:
        con.execute(f"DROP VIEW IF EXISTS {ds}")
        glob = ROOT / RAW_ROOT / LAKE_PREFIX / ds / "**" / "*.parquet"
        ds_dir = ROOT / RAW_ROOT / LAKE_PREFIX / ds
        if ds_dir.is_dir() and any(ds_dir.rglob("*.parquet")):
            con.execute(
                f"CREATE VIEW {ds} AS "
                f"SELECT *, filename FROM read_parquet('{glob.as_posix()}', filename=true)"
            )
            counts[ds] = con.execute(f"SELECT count(*) FROM {ds}").fetchone()[0]
    return counts


def _adls_urls(prefix: str) -> list[str]:
    """URLs https + SAS (1h, só leitura) para todos os blobs sob o prefixo."""
    from datetime import datetime, timedelta, timezone

    from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    svc = BlobServiceClient.from_connection_string(sys.modules["os"].environ["AZURE_STORAGE_CONNECTION_STRING"])

    def _url(blob_name: str) -> str:
        sas = generate_blob_sas(
            account_name=svc.account_name,
            container_name="raw",
            blob_name=blob_name,
            account_key=svc.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        return f"https://{svc.account_name}.blob.core.windows.net/raw/{blob_name}?{sas}"

    return [_url(b.name) for b in svc.get_container_client("raw").list_blobs(name_starts_with=prefix)]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sql", metavar="QUERY", help="roda a SQL contra lake.duckdb e imprime o resultado")
    p.add_argument("--repl", action="store_true", help="shell Python interativo com `con` pronta")
    p.add_argument("--list", nargs="?", const=f"{LAKE_PREFIX}/", metavar="PREFIXO",
                   help="lista blobs do ADLS (não roda SQL)")
    args = p.parse_args()

    if args.list is not None:
        for b in _adls_urls(args.list):
            print(b.split("?")[0])
        return

    con = duckdb.connect(str(DB_PATH))
    if args.sql:
        print(con.execute(args.sql).fetchdf().to_string(index=False))
        return

    if args.repl:
        counts = _views(con)
        con.execute("INSTALL httpfs; LOAD httpfs;")
        code.interact(
            banner=(
                f"views: {counts}\n"
                "ex.: con.sql('SELECT count(*) FROM results').show()\n"
                "     con.sql(\"SELECT * FROM read_parquet(_adls_urls('f1-lake/schedule/2024'))\")\n"
                "sair: exit()"
            ),
            local={"con": con, "duckdb": duckdb, "_adls_urls": _adls_urls},
        )
        return

    counts = _views(con)
    print(f"lake.duckdb atualizado — {DB_PATH}")
    for ds, n in counts.items():
        print(f"  {ds:<9} {n:>6} linhas")
    if not counts:
        print("  (nenhum parquet em data/f1-lake — rode o collector primeiro)")
    print("\nuse com: harlequin lake.duckdb   |   python scripts/inspeciona.py --sql \"SELECT ...\"")


if __name__ == "__main__":
    main()
