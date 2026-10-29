"""Silver: tabelas Bronze (Delta) → limpeza, tipagem e joins (prod.f1_silver.*).

Convenções (decididas com o usuário, 28/10):
- snake_case em TODAS as colunas; flags são boolean (fl_*).
- Durações chegam da Bronze como long nanossegundos → *_sec (÷ 1e9).
- double → int com NaN → null (Position/GridPosition/Laps/LapNumber/Stint/
  TyreLife); ClassifiedPosition (string) é preservada — o "positionText" do
  Ergast nunca existiu neste lake.
- laps não tem DriverId/TeamId (só nomes Driver/Team): joins com as dims são
  por nome — fragilidade de renomeação de equipes entre eras, conhecida.
- races mantém o formato largo de sessões da Bronze; SessionXDate locais
  foram normalizadas ao fuso +03:00 na Bronze (instantes preservados) — para
  análise, usar as colunas *_date_utc.
- pit_stops é derivada de laps (PitInTime da volta N + PitOutTime da N+1) —
  não existe dataset próprio de pit stops.
- laps/weather cobrem só os anos presentes na Bronze (2023-2025
  até o backfill fechar). Re-rodar este job após cada `bundle run bronze`.

Idempotente: CREATE OR REPLACE por tabela a cada run. Depende do job bronze
ter rodado (lê prod.f1_bronze.*).

Como rodar: job do bundle `f1-lake-silver` (databricks bundle deploy + bundle
run silver, na pasta databricks/) — ou colar como notebook.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

NS = 1e9  # nanossegundos → segundos


def _dim_drivers(results: DataFrame) -> DataFrame:
    """1 linha/piloto — dedup por DriverId com último valor não-nulo."""
    return (results.groupBy("DriverId")
            .agg(F.last("DriverNumber", True).alias("driver_number"),
                 F.last("FullName", True).alias("full_name"),
                 F.last("FirstName", True).alias("first_name"),
                 F.last("LastName", True).alias("last_name"),
                 F.last("Abbreviation", True).alias("abbreviation"),
                 F.last("BroadcastName", True).alias("broadcast_name"),
                 F.last("CountryCode", True).alias("country_code"),
                 F.last("HeadshotUrl", True).alias("headshot_url"),
                 F.min("year").alias("first_year"),
                 F.max("year").alias("last_year"),
                 F.countDistinct("year").alias("n_seasons"))
            .withColumnRenamed("DriverId", "driver_id"))


def _dim_constructors(results: DataFrame) -> DataFrame:
    """1 linha/equipe — dedup por TeamId com último valor não-nulo."""
    return (results.groupBy("TeamId")
            .agg(F.last("TeamName", True).alias("team_name"),
                 F.last("TeamColor", True).alias("team_color"),
                 F.min("year").alias("first_year"),
                 F.max("year").alias("last_year"))
            .withColumnRenamed("TeamId", "team_id"))


def _races(schedule: DataFrame) -> DataFrame:
    """1 linha/evento — snake_case; sessões permanecem em formato largo."""
    cols = ["year", "round",
            F.col("EventName").alias("event_name"),
            F.col("OfficialEventName").alias("official_event_name"),
            F.col("Country").alias("country"),
            F.col("Location").alias("location"),
            F.col("EventFormat").alias("event_format"),
            F.col("EventDate").alias("event_date"),
            F.col("F1ApiSupport").alias("f1_api_support")]
    for i in range(1, 6):
        cols += [F.col(f"Session{i}").alias(f"session{i}"),
                 F.col(f"Session{i}Date").alias(f"session{i}_date"),
                 F.col(f"Session{i}DateUtc").alias(f"session{i}_date_utc")]
    return schedule.select(*cols)


def _results(results: DataFrame) -> DataFrame:
    return results.select(
        "year", "round", "event",
        F.col("DriverId").alias("driver_id"),
        F.col("DriverNumber").alias("driver_number"),
        F.col("TeamId").alias("team_id"),
        F.col("TeamName").alias("team_name"),
        F.col("Position").cast("int").alias("position_order"),  # NaN → null
        F.col("ClassifiedPosition").alias("classified_position"),
        F.col("GridPosition").cast("int").alias("grid_position"),
        F.col("Laps").cast("int").alias("laps"),
        F.col("Points").alias("points"),
        (F.col("Q1") / NS).alias("q1_sec"),
        (F.col("Q2") / NS).alias("q2_sec"),
        (F.col("Q3") / NS).alias("q3_sec"),
        (F.col("Time") / NS).alias("time_sec"),
        F.col("Status").alias("status"),
        # classificado como finisher: 'Finished' ou chegou com voltas de atraso ('+...')
        ((F.col("Status") == "Finished") | F.col("Status").rlike(r"^\+"))
        .alias("fl_finished"))


def _laps(laps: DataFrame) -> DataFrame:
    return laps.select(
        "year", "round", "event",
        F.col("Driver").alias("driver"),
        F.col("DriverNumber").alias("driver_number"),
        F.col("Team").alias("team"),
        F.col("LapNumber").cast("int").alias("lap_number"),
        F.col("Stint").cast("int").alias("stint"),
        (F.col("LapTime") / NS).alias("lap_time_sec"),
        (F.col("Sector1Time") / NS).alias("sector1_time_sec"),
        (F.col("Sector2Time") / NS).alias("sector2_time_sec"),
        (F.col("Sector3Time") / NS).alias("sector3_time_sec"),
        (F.col("LapStartTime") / NS).alias("lap_start_time_sec"),
        F.col("LapStartDate").alias("lap_start_date"),
        F.col("SpeedI1").alias("speed_i1"),
        F.col("SpeedI2").alias("speed_i2"),
        F.col("SpeedFL").alias("speed_fl"),
        F.col("SpeedST").alias("speed_st"),
        F.col("IsPersonalBest").alias("is_personal_best"),
        F.col("Compound").alias("compound"),
        F.col("TyreLife").cast("int").alias("tyre_life"),
        F.col("FreshTyre").alias("fresh_tyre"),
        F.col("PitInTime").isNotNull().alias("fl_pit_in"),
        F.col("PitOutTime").isNotNull().alias("fl_pit_out"),
        F.col("DeletedReason").isNotNull().alias("fl_deleted"),
        F.col("DeletedReason").alias("deleted_reason"),
        F.col("TrackStatus").alias("track_status"),
        F.col("Position").cast("int").alias("position"),
        F.col("FastF1Generated").alias("fastf1_generated"),
        F.col("IsAccurate").alias("is_accurate"))


def _pit_stops(laps: DataFrame) -> DataFrame:
    """1 linha/pit stop: PitInTime na volta N + PitOutTime na volta seguinte."""
    w = Window.partitionBy("year", "round", "event", "Driver") \
        .orderBy("LapNumber")
    return (laps
            .withColumn("pit_out_next", F.lead("PitOutTime").over(w))
            .withColumn("lap_out_next", F.lead("LapNumber").over(w))
            .where(F.col("PitInTime").isNotNull()
                   & F.col("pit_out_next").isNotNull())
            .select("year", "round", "event",
                    F.col("Driver").alias("driver"),
                    F.col("DriverNumber").alias("driver_number"),
                    F.col("LapNumber").cast("int").alias("lap_in"),
                    F.col("lap_out_next").cast("int").alias("lap_out"),
                    ((F.col("pit_out_next") - F.col("PitInTime")) / NS)
                    .alias("pit_duration_sec")))


def _weather(weather: DataFrame) -> DataFrame:
    return weather.select(
        "year", "round", "event",
        (F.col("Time") / NS).alias("time_sec"),
        F.col("AirTemp").alias("air_temp"),
        F.col("TrackTemp").alias("track_temp"),
        F.col("Humidity").alias("humidity"),
        F.col("Pressure").alias("pressure"),
        F.col("Rainfall").alias("rainfall"),
        F.col("WindDirection").cast("int").alias("wind_direction"),
        F.col("WindSpeed").alias("wind_speed"))


def main() -> None:
    try:
        spark  # noqa: B018  # notebook: sessão já existe
    except NameError:
        spark = SparkSession.builder.appName("f1-silver").getOrCreate()

    bronze = {ds: spark.table(f"prod.f1_bronze.{ds}")
              for ds in ("schedule", "results", "laps", "weather")}

    tables = {
        "dim_drivers": _dim_drivers(bronze["results"]),
        "dim_constructors": _dim_constructors(bronze["results"]),
        "races": _races(bronze["schedule"]),
        "results": _results(bronze["results"]),
        "laps": _laps(bronze["laps"]),
        "pit_stops": _pit_stops(bronze["laps"]),
        "weather": _weather(bronze["weather"]),
    }

    for name, df in tables.items():
        df.write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(f"prod.f1_silver.{name}")

    print("\n=== Resumo Silver ===")
    for name in tables:
        t = spark.table(f"prod.f1_silver.{name}")
        ycol = "first_year" if name.startswith("dim_") else "year"
        row = t.agg(F.count("*").alias("rows"),
                    F.min(ycol).alias("y0"),
                    F.max(ycol).alias("y1")).first()
        extra = ""
        if "event" in t.columns:
            extra = f" | {t.select('event').distinct().count()} tipos de event"
        print(f"prod.f1_silver.{name}: {row['rows']:,} linhas | "
              f"{row['y0']}-{row['y1']}{extra}")

    # Checkpoint F6 (runbook): sem órfãos, pit stops por corrida, totais = bronze
    orphans = (spark.table("prod.f1_silver.results")
               .join(spark.table("prod.f1_silver.dim_drivers"), "driver_id",
                     "left_anti")
               .count())
    races_with_pits = (spark.table("prod.f1_silver.pit_stops")
                       .where("event = 'Race'")
                       .select("year", "round").distinct().count())
    print(f"checkpoint: órfãos results↔dim_drivers = {orphans} (esperado 0) | "
          f"corridas com pit stops = {races_with_pits} (> 0)")
    for src, silver in [("results", "results"), ("laps", "laps"),
                        ("weather", "weather")]:
        nb = spark.table(f"prod.f1_bronze.{src}").count()
        ns = spark.table(f"prod.f1_silver.{silver}").count()
        status = "OK" if nb == ns else "DIVERGE!"
        print(f"checkpoint: bronze.{src} {nb:,} = silver.{silver} {ns:,} → {status}")


if __name__ == "__main__":
    main()
