# F1 Lake — collector

Coleta de dados de Fórmula 1 (2018 → hoje) via FastF1 e upload duplo:
ADLS Gen2 (container `raw`) + Volume Databricks (`prod.f1_raw.landing`).

## Divisão de trabalho (importante)

| Onde | Datasets | Por quê |
|---|---|---|
| GitHub Actions (seg 06:00 UTC) | `schedule`, `results` | Leves (só Jolpica); mantém a PAT viva |
| Máquina local | `laps`, `weather` | A F1 bloqueia IP de datacenter no livetiming real: no runner o FastF1 cai num mirror comunitário vazio. Local (IP residencial + auth F1TV) funciona |

## Uso local

```powershell
pip install -r collector/requirements.txt
python -m fastf1 auth f1tv --authenticate   # uma vez por máquina (laps/weather)
python collector\main.py --years 2024 --datasets laps weather
```

O `.env` na raiz (ver `.env.example`) carrega a connection string do ADLS e,
opcionalmente, host/token Databricks para o upload duplo (sem eles, sobe só no ADLS).

Comportamento:

- **Resumível**: sessões cujo parquet já existe no ADLS são puladas; re-runs
  retomam de onde pararam. `--overwrite` recoleta tudo.
- **Upload por arquivo, imediato**: timeout/crash não perce trabalho.
- **Pacing automático** sob os limites de API (Jolpica 200/h, demais 500/h).
  laps+weather custam ~2-3 min/sessão (download + pacing) → ~4-6 h por ano.
- `python collector\sync_volume.py` — espelha o ADLS inteiro para o volume
  Databricks (cobre arquivos subidos sem os envs do Databricks; idempotente).

## Auth F1TV (obrigatória para laps/weather)

A F1 exige login nos endpoints de livetiming (de onde vêm laps, weather e
telemetria). É preciso uma **conta F1TV** e o FastF1 **3.9.0a0**:

```powershell
python -m fastf1 auth f1tv --authenticate   # pede email + senha (uma vez)
python -m fastf1 auth f1tv --status         # confere token/expiração
```

O token fica em `%LOCALAPPDATA%\fastf1\f1auth_token.json` e expira sem
refresh — re-autentique quando laps/weather voltarem a vir vazios.

Datasets: `schedule`, `results`, `laps`, `weather` — gravados em
`data/f1-lake/{dataset}/{year}/{year}_{round:02}_{session}.parquet` (local, ADLS
e volume usam a mesma árvore de paths). A coleta é idempotente: mesmo nome de
arquivo sobrescreve.
