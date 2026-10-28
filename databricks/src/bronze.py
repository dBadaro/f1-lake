"""Bronze: parquet do volume → tabelas Delta gerenciadas (prod.f1_bronze.*).

Fiel à origem (sem transformação de negócio) + colunas de contexto derivadas
do NOME do arquivo — nenhum dataset carrega ano/round nos dados, só no nome
({ano}_{round}_{SESSAO}.parquet):

- year, round  → chave temporal/posicional (int)
- event        → FP1 | FP2 | FP3 | Qualify | Sprint Qualifying | Sprint | Race
- _source_file → proveniência (path completo no volume)

Como rodar (nada aqui é executado por este repo automaticamente):
- Notebook: colar o arquivo inteiro numa célula e Run all (spark já existe).
- Job "Python file": apontar para este arquivo — main() cria a SparkSession.

Idempotente: CREATE OR REPLACE por tabela a cada run (volume pequeno, sem
merge), como define a Fase 5 do runbook.

Obs. de schema: os tempos do FastF1 estão gravados como duration[ns] no
parquet (Q1/LapTime/Time etc.). Se o runtime estourar na leitura
("Illegal Parquet type: DURATION"), tratar na hora — converter timedeltas
na coleta ou sobrescrever o schema de leitura; silver tipa isso de todo jeito.
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

SRC = "/Volumes/prod/f1_raw/landing"
TARGET = "prod.f1_bronze"
DATASETS = ["results", "laps", "weather"]  # schedule: adicionar quando quiser

# final do nome do arquivo → valor de event (SQ/S existem a partir de 2021;
# sufixo desconhecido cai no fallback: o próprio código em maiúsculas)
EVENT_BY_SUFFIX = {
    "FP1": "FP1",
    "FP2": "FP2",
    "FP3": "FP3",
    "Q": "Qualify",
    "SQ": "Sprint Qualifying",
    "S": "Sprint",
    "R": "Race",
}


def _context_from_filename() -> dict[str, Column]:
    """year/round/event/_source_file a partir do path lido (input_file_name)."""
    stem = F.regexp_replace(
        F.element_at(F.split(F.input_file_name(), "/"), -1),  # 2024_01_FP1.parquet
        r"\.parquet$",
    )
    parts = F.split(stem, "_")  # [ano, round, sessao]
    suffix = parts[2]
    event_map = F.create_map(
        *[F.lit(v) for par in EVENT_BY_SUFFIX.items() for v in par]
    )
    return {
        "year": parts[0].cast("int"),
        "round": parts[1].cast("int"),
        "event": F.coalesce(event_map[suffix], F.upper(suffix)),
        "_source_file": F.input_file_name(),
    }


def bronze(spark: SparkSession, dataset: str) -> None:
    """(Re)cria prod.f1_bronze.{dataset} a partir de {SRC}/{dataset}/."""
    df = (
        spark.read
        # schemas evoluem entre anos/FastF1: união de colunas, faltante → NULL
        .option("mergeSchema", "true")
        .parquet(f"{SRC}/{dataset}")
        .withColumns(_context_from_filename())
    )
    df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{TARGET}.{dataset}")


def main() -> None:
    try:
        spark  # noqa: B018  # notebook: sessão já existe
    except NameError:
        spark = SparkSession.builder.appName("f1-bronze").getOrCreate()

    for dataset in DATASETS:
        bronze(spark, dataset)
        t = spark.table(f"{TARGET}.{dataset}")
        totals = t.agg(
            F.count("*").alias("rows"),
            F.countDistinct("_source_file").alias("files"),
            F.min("year").alias("y0"),
            F.max("year").alias("y1"),
        ).first()
        events = {
            r["event"]: r["count"]
            for r in t.groupBy("event").count().orderBy("event").collect()
        }
        print(f"{TARGET}.{dataset}: {totals['rows']:,} linhas | "
              f"{totals['files']:,} arquivos | {totals['y0']}-{totals['y1']} | {events}")


if __name__ == "__main__":
    main()
