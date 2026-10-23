# F1 Lake — collector

Coleta de dados de Fórmula 1 (2018 → hoje) via FastF1 e upload duplo:
ADLS Gen2 (container `raw`) + Volume Databricks (`prod.f1_raw.landing`).

## Uso

```powershell
pip install -r collector/requirements.txt
$env:AZURE_STORAGE_CONNECTION_STRING = "<AZ_CONN_STR>"   # obrigatória
$env:DATABRICKS_HOST = "https://dbc-xxxx.cloud.databricks.com"  # opcional
$env:DATABRICKS_TOKEN = "<PAT>"                                  # opcional

python collector\main.py --years 2024 --datasets schedule results laps weather
```

## Auth F1TV (obrigatória para laps/weather)

A F1 exige login nos endpoints de livetiming (de onde vêm laps, weather e
telemetria). É preciso uma **conta F1TV (free)** e o FastF1 **3.9.0a0**:

```powershell
python -m fastf1 auth f1tv --authenticate   # pede email + senha (uma vez)
python -m fastf1 auth f1tv --status         # confere token/expiração
```

O token fica em `%LOCALAPPDATA%\fastf1\f1auth_token.json` e expira sem
refresh — re-autentique quando laps/weather voltarem a vir vazios. No
GitHub Actions, o workflow autentica sozinho com os secrets `F1TV_USERNAME`
e `F1TV_PASSWORD`.

Datasets: `schedule`, `results`, `laps`, `weather` — gravados em
`data/f1-lake/{dataset}/{year}/{year}_{round:02}_{session}.parquet` (local, ADLS
e volume usam a mesma árvore de paths). A coleta é idempotente: mesmo nome de
arquivo sobrescreve.
