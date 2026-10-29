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

Obs. de schema: ler o DIRETÓRIO sem schema explícito falha com
UNABLE_TO_INFER_SCHEMA (arquivos heterogêneos; laps.Deleted é parquet NULL).
Arquivo único lê normal. Por isso SCHEMAS explícitos abaixo — ver comentário
na própria constante. Tempos duration[ns] chegam como long (nanossegundos);
silver tipa isso.
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

SRC = "/Volumes/prod/f1_raw/landing/f1-lake"  # os uploads levam o prefixo do lake
TARGET = "prod.f1_bronze"
DATASETS = ["schedule", "results", "laps", "weather"]

# Schemas EXPLÍCITOS: a inferência de diretório do Spark falha neste corpus
# (UNABLE_TO_INFER_SCHEMA — merge entre arquivos heterogêneos; coluna Deleted
# dos laps está gravada como parquet NULL e durações como duration[ns]).
# Derivados do corpus completo via data/backfill-logs/scan_schemas.py
# (tipos estáveis entre anos — mesmo FastF1 3.9.0a0 em toda a coleta).
# duration[ns] → long (nanossegundos); Deleted (null em 100% dos arquivos)
# fica podada; colunas novas em arquivos futuros são ignoradas até o schema
# ser atualizado aqui.
SCHEMAS = {
    # schedule: 1 linha/evento, nome do arquivo {ano}_{round}_schedule.parquet
    # (event cai no fallback "SCHEDULE"); timestamps já nascem em us no FastF1.
    "schedule": """
        RoundNumber long, Country string, Location string,
        OfficialEventName string, EventDate timestamp, EventName string,
        EventFormat string, Session1 string, Session1Date timestamp,
        Session1DateUtc timestamp, Session2 string, Session2Date timestamp,
        Session2DateUtc timestamp, Session3 string, Session3Date timestamp,
        Session3DateUtc timestamp, Session4 string, Session4Date timestamp,
        Session4DateUtc timestamp, Session5 string, Session5Date timestamp,
        Session5DateUtc timestamp, F1ApiSupport boolean
    """,
    "results": """
        DriverNumber string, BroadcastName string, Abbreviation string,
        DriverId string, TeamName string, TeamColor string, TeamId string,
        FirstName string, LastName string, FullName string, HeadshotUrl string,
        CountryCode string, Position double, ClassifiedPosition string,
        GridPosition double, Q1 long, Q2 long, Q3 long, Time long,
        Status string, Points double, Laps double
    """,
    "laps": """
        Time long, Driver string, DriverNumber string, LapTime long,
        LapNumber double, Stint double, PitOutTime long, PitInTime long,
        Sector1Time long, Sector2Time long, Sector3Time long,
        Sector1SessionTime long, Sector2SessionTime long, Sector3SessionTime long,
        SpeedI1 double, SpeedI2 double, SpeedFL double, SpeedST double,
        IsPersonalBest boolean, Compound string, TyreLife double,
        FreshTyre boolean, Team string, LapStartTime long,
        LapStartDate timestamp, TrackStatus string, Position double,
        DeletedReason string, FastF1Generated boolean, IsAccurate boolean
    """,
    "weather": """
        Time long, AirTemp double, Humidity double, Pressure double,
        Rainfall boolean, TrackTemp double, WindDirection long, WindSpeed double
    """,
}

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
    """year/round/event/_source_file a partir dos metadados do arquivo lido.

    input_file_name() não existe no Unity Catalog — o equivalente é o struct
    _metadata (file_name/file_path), disponível na leitura de parquet.
    """
    stem = F.regexp_replace(F.col("_metadata.file_name"), r"\.parquet$", "")
    parts = F.split(stem, "_")  # [ano, round, sessao]
    suffix = parts[2]
    event_map = F.create_map(
        *[F.lit(v) for par in EVENT_BY_SUFFIX.items() for v in par]
    )
    return {
        "year": parts[0].cast("int"),
        "round": parts[1].cast("int"),
        "event": F.coalesce(event_map[suffix], F.upper(suffix)),
        "_source_file": F.col("_metadata.file_path"),
    }


def bronze(spark: SparkSession, dataset: str) -> None:
    """(Re)cria prod.f1_bronze.{dataset} a partir de {SRC}/{dataset}/."""
    df = (
        spark.read
        # com schema explícito o Spark não desce nas subpastas de anos — sem
        # isto a tabela sai vazia (0 arquivos na "raiz" do dataset)
        .option("recursiveFileLookup", "true")
        .schema(SCHEMAS[dataset])
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
