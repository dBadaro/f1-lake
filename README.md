# F1 Lake — collector

Coleta de dados de Fórmula 1 (2018 → hoje) via FastF1 e upload duplo:
ADLS Gen2 (container `raw`) + Volume Databricks (`main.f1_raw.landing`).

## Uso

```powershell
pip install -r collector/requirements.txt
$env:AZURE_STORAGE_CONNECTION_STRING = "<AZ_CONN_STR>"   # obrigatória
$env:DATABRICKS_HOST = "https://dbc-xxxx.cloud.databricks.com"  # opcional
$env:DATABRICKS_TOKEN = "<PAT>"                                  # opcional

python collector\main.py --years 2024 --datasets schedule results laps weather
```

Datasets: `schedule`, `results`, `laps`, `weather` — gravados em
`data/f1-lake/{dataset}/{year}/{year}_{round:02}_{session}.parquet` (local, ADLS
e volume usam a mesma árvore de paths). A coleta é idempotente: mesmo nome de
arquivo sobrescreve.
