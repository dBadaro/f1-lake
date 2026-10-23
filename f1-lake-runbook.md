# Runbook — F1 Lake

**Objetivo:** replicar o projeto [TeoMeWhy/f1-lake](https://github.com/TeoMeWhy/f1-lake) com custo **R$ 0**, usando Azure Storage (raw) + Databricks Free Edition (bronze/silver/gold), coleta via GitHub Actions.

**Escopo:** schedule, results (R/Q/FP1-3/S), laps, weather — histórico **2018 → hoje**. Sem telemetria bruta, sem ML/app (fases futuras).

**Stack:** Python + FastF1 · GitHub Actions · ADLS Gen2 (ambiente já criado, sem IaC) · Databricks Free Edition (UC + serverless) · Databricks CLI + Asset Bundles no VS Code.

```
[GitHub Actions]                     [Azure Storage]              [Databricks Free Edition]
 collector.py (FastF1)  ──parquet──▶ container "raw"              catalog prod
 upload.py                           brazilsouth · LRS · HNS       ├─ schema f1_raw  → volume landing (parquet)
   ├─ ADLS (SDK)                     (raw permanente / backup)     ├─ schema f1_bronze (Delta, fiel à origem)
   └─ Files API + PAT                                              ├─ schema f1_silver (entidades limpas)
                                                                   └─ schema f1_gold   (relatórios)
                                     [Job serverless agendado: bronze → silver → gold]
```

**Como usar:** execute as fases em ordem. Cada fase tem um **✅ Checkpoint** — só avance depois de validar. Marque os checkboxes conforme conclui.

---

## Fase 0 — Pré-requisitos (≈ 30 min)

### 0.1 Contas (você já tem)
- [ ] Azure free (região **brazilsouth**) — storage já criado, ver Fase 1
- [ ] Databricks **Free Edition** (login em `accounts.databricks.com`, sem Azure)
- [ ] GitHub (dBadaro)

### 0.2 Instalar ferramentas no Windows
Abra o **PowerShell como Administrador** e rode:

```powershell
winget install --id Microsoft.AzureCLI -e
winget install --id Databricks.DatabricksCLI -e
winget install --id Git.Git -e
winget install --id Python.Python.3.12 -e
```

Feche e reabra o PowerShell, então valide:

```powershell
az version
databricks version
git --version
py -3.12 --version
```

### 0.3 VS Code
- [ ] Extensão **Databricks** (`databricks.databricks`)
- [ ] Extensão **Python** (Microsoft)
- [ ] *(Opcional)* **Roo Code** — AI de código com sua API key da z.ai (provider "Z AI" nativo)

### ✅ Checkpoint F0
Todos os comandos de versão respondem sem erro.

---

## Fase 1 — Azure raw: validar o ambiente existente (≈ 15 min)

> O storage ADLS Gen2 **já existe** (1 resource group + storage account LRS, brazilsouth + container `raw`). Esta fase só confirma que tudo está no lugar e extrai a connection string. Nada é criado via código — se um dia precisar recriar, faça pelo Portal (ou `az storage account create` com `--enable-hierarchical-namespace true`).

### 1.1 Login Azure e ID da subscription

```powershell
az login
az account show --query id -o tsv
```

### 1.2 Localizar e validar os recursos

```powershell
az storage account list --query "[].{nome:name, rg:resourceGroup, regiao:location, hns:isHnsEnabled}" -o table
az storage container list --account-name stf1lakedrk001 -o table
```

Confira: storage em **brazilsouth**, `isHnsEnabled = true` (ADLS Gen2), e o container `raw` listado. Se o container não existir:

```powershell
az storage container create --account-name stf1lakedrk001 --name raw --auth-mode login
```

> Anote o **nome do resource group** e o **nome da storage account** da listagem — são usados no comando da connection string abaixo.

### 1.3 Guardar a connection string (vira secret na Fase 2)

```powershell
az storage account show-connection-string -g rg-f1-lake -n stf1lakedrk001 --query connectionString -o tsv
```

Copie e guarde (vou chamar de `<AZ_CONN_STR>`).

> ⚠️ A connection string carrega a **account key** (acesso total). Ela só vive em 2 lugares: secret do GitHub e variável local. Se vazar, regenere a key no Portal (Settings → Access keys).

### 1.4 Alerta de custo (proteção do custo zero)
Portal Azure → **Cost Management** → **Budgets** → **Add**:
- Name: `alerta-f1-lake` · Amount: **5 BRL** · Alert threshold: 50% e 80% · Email: o seu.
- Se o budget já existir, apenas confirme que está ativo.

### ✅ Checkpoint F1
`az storage container list` mostra o container `raw`, e `<AZ_CONN_STR>` está guardada em local seguro.

---

## Fase 2 — Repo GitHub + collector + workflow (≈ 4 h, a maior fase de código)

### 2.1 Criar o repo no GitHub (web)
1. github.com → **New repository** → nome `f1-lake`
2. Private (2.000 min Actions/mês, suficiente) ou **Public** (Actions ilimitado — dados F1 são públicos, não tem segredo no código)
3. **Não** inicialize com README

### 2.2 Git inicial

```powershell
mkdir C:\dev\f1-lake
cd C:\dev\f1-lake
git init
git branch -M main
git remote add origin https://github.com/dBadaro/f1-lake.git
```

> Vai pedir login no primeiro push: use **Sign in with browser** (Git Credential Manager abre o GitHub).

### 2.3 Estrutura do repo

```
f1-lake/
├── collector/
│   ├── config.py               # constantes: datasets, sessões, paths
│   ├── collect.py              # FastF1 → parquet local
│   ├── upload.py               # parquet → ADLS + volume Databricks
│   ├── main.py                 # CLI: --years --datasets
│   └── requirements.txt
├── .github/workflows/
│   └── collect.yml             # agendado semanal + backfill manual
├── databricks/                 # (Fase 3)
├── .gitignore
└── README.md
```

**`.gitignore`**
```
.venv/
cache/
data/
*.parquet
.env
__pycache__/
```

### 2.4 Collector — especificação por arquivo

**`collector/config.py`** — fonte única de verdade:
- `DATASETS = ["schedule", "results", "laps", "weather"]`
- `SESSIONS = ["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"]` (sessão inexistente no evento → skip silencioso; SQ/S só existem 2021+)
- `RAW_ROOT = "data"` · layout: `data/f1-lake/{dataset}/{year}/{year}_{round:02}_{session}.parquet` (no ADLS: `raw/f1-lake/...`)
- Path remoto idêntico ao local (mesma árvore no ADLS e no volume)

**`collector/collect.py`** — extração por dataset:

| Dataset | Extração FastF1 | Granularidade |
|---|---|---|
| `schedule` | `fastf1.get_event_calendar(year)` | 1 linha/evento |
| `results` | `session.results` (após `session.load()`) | 1 linha/piloto/sessão |
| `laps` | `session.laps` (reset_index antes do parquet) | 1 linha/volta |
| `weather` | `session.weather` | 1 leitura/minuto de sessão |

Regras: `fastf1.Cache.enable_cache("./cache")` · `time.sleep(2)` entre sessões (rate limit Jolpica) · sessão inexistente ou erro HTTP → loga e continua · **idempotente** (mesmo nome de arquivo sobrescreve).

**`collector/upload.py`** — upload duplo:

```python
import os, requests
from pathlib import Path
from azure.storage.blob import BlobServiceClient

def upload_adls(local: Path, remote: str):
    svc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
    svc.get_blob_client(container="raw", blob=remote).upload_blob(local.read_bytes(), overwrite=True)

def upload_databricks(local: Path, remote: str):
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    url = f"{host}/api/2.0/fs/files/Volumes/prod/f1_raw/landing/{remote}?overwrite=true"
    r = requests.put(url, headers={"Authorization": f"Bearer {token}"},
                     data=local.read_bytes(), timeout=300)
    r.raise_for_status()
```

Se as envs do Databricks não existirem → avisa e sobe só no ADLS (graceful degradation).

**`collector/main.py`** — argparse: `--years "2018 2019"` e `--datasets schedule results laps weather`; loop → collect → upload duplo; resumo final de contagem por dataset/ano.

**`collector/requirements.txt`**
```
# 3.9.0a0: obrigatória — F1 exigiu login nos endpoints de livetiming
# (laps/weather); versões <= 3.8.x quebraram. Requer auth F1TV (abaixo).
fastf1==3.9.0a0
pandas
pyarrow
azure-storage-blob
requests
python-dotenv
```

> **Auth F1TV (ajuste vs. original):** a F1 desligou o acesso anônimo ao
> livetiming (laps/weather/telemetria → 401; mirror comunitário vazio). Com
> FastF1 ≥ 3.9.0a0 e uma conta F1TV **free**, autentique uma vez por máquina:
> `python -m fastf1 auth f1tv --authenticate`. Token em
> `%LOCALAPPDATA%\fastf1\f1auth_token.json`, expira sem refresh. No Actions o
> workflow autentica com os secrets `F1TV_USERNAME` / `F1TV_PASSWORD`. Além
> disso, na 3.9 o atributo `session.weather` virou `session.weather_data`.

### 2.5 Teste local antes do Actions (amostra pequena)

```powershell
cd C:\dev\f1-lake
py -3.12 -m venv .venv
.\.venv\Scripts\activate
pip install -r collector\requirements.txt

$env:AZURE_STORAGE_CONNECTION_STRING = "<AZ_CONN_STR>"
python collector\main.py --years 2024 --datasets schedule results
```

Conferir: parquets em `data\f1-lake\results\2024\` e blobs no portal (container `raw`, pasta `f1-lake`).

### 2.6 Secrets do GitHub

Repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Valor |
|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | `<AZ_CONN_STR>` da Fase 1 |
| `DATABRICKS_HOST` | URL do workspace (`https://dbc-xxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | PAT — gerar **agora**: workspace Databricks → seu usuário (topo dir) → **Settings → Developer → Access tokens → Manage → Generate new token** (lifetime 90 dias; copie na hora — não aparece de novo) |
| `F1TV_USERNAME` / `F1TV_PASSWORD` | login da conta F1TV (free) — usados pelo step de auth do livetiming (laps/weather) |

> ⚠️ PAT auto-expira se ficar 90 dias **sem uso** — o job semanal mantém vivo. Falhou com 401? Gere outra e atualize o secret.

### 2.7 Workflow `.github/workflows/collect.yml`

```yaml
name: collect

on:
  schedule:
    - cron: "0 6 * * 1"        # segundas 06:00 UTC (03:00 GMT-3, pós-corrida de domingo)
  workflow_dispatch:
    inputs:
      from_year:
        description: "Ano inicial (vazio = ano atual)"
        required: false
      to_year:
        description: "Ano final (vazio = ano atual)"
        required: false
      datasets:
        description: "Datasets: schedule results laps weather"
        required: false
        default: "schedule results laps weather"

concurrency:
  group: collect
  cancel-in-progress: false    # nunca cancela uma coleta em andamento

jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 330       # margem antes do limite de 6 h do runner
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: collector/requirements.txt
      - id: years
        run: |
          FROM="${{ inputs.from_year }}"
          TO="${{ inputs.to_year }}"
          [ -z "$FROM" ] && FROM=$(date -u +%Y)
          [ -z "$TO" ] && TO=$(date -u +%Y)
          echo "from=$FROM" >> "$GITHUB_OUTPUT"
          echo "to=$TO" >> "$GITHUB_OUTPUT"
      - run: pip install -r collector/requirements.txt
      - name: Autenticar F1TV (livetiming — laps/weather — exige login)
        run: python -m fastf1 auth f1tv --authenticate
        env:
          F1_USERNAME: ${{ secrets.F1TV_USERNAME }}
          F1_PASSWORD: ${{ secrets.F1TV_PASSWORD }}
      - run: python collector/main.py --years $(seq -s ' ' "${{ steps.years.outputs.from }}" "${{ steps.years.outputs.to }}") --datasets "${{ inputs.datasets || 'schedule results laps weather' }}"
        env:
          AZURE_STORAGE_CONNECTION_STRING: ${{ secrets.AZURE_STORAGE_CONNECTION_STRING }}
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

> **Por que assim:** em runs agendados os `inputs` vêm vazios — o step `years` assume o ano corrente (coleta incremental semanal correta) e o fallback no `--datasets` evita o job rodar com lista vazia. O `seq` expande o range num único comando, sem matrix — mais simples e mais barato (1 runner em vez de N). Backfill ainda é recomendado **em chunks manuais** (Fase 4) para acompanhar falhas por ano.

### ✅ Checkpoint F2
1. **Actions → collect → Run workflow** manual: `from_year=2024`, `to_year=2024`
2. Job verde no GitHub
3. Portal Azure: `raw/f1-lake/results/2024/...` com parquets

---

## Fase 3 — Databricks: CLI, PAT e bundle com o UC (≈ 1 h)

> Criamos aqui: catalog `prod` + schemas `f1_raw`, `f1_bronze`, `f1_silver`, `f1_gold` + volume `landing` onde o collector joga os parquets. (Ajuste em relação à versão original: em vez do catalog `main` do workspace, usamos um catalog próprio `prod` — projeto isolado.)

### 3.1 Autenticar o CLI (PAT — OAuth **não funciona** na Free Edition)

```powershell
mkdir C:\dev\f1-lake\databricks
cd C:\dev\f1-lake\databricks
databricks configure
# Host: https://dbc-xxxx.cloud.databricks.com (o mesmo do secret)
# Token: o PAT da Fase 2
databricks current-user me    # valida
```

### 3.2 Estrutura do bundle

```
databricks/
├── databricks.yml
├── resources/
│   ├── uc.yml              # schemas + volume
│   └── job.yml             # (Fase 8)
└── src/
    ├── bronze.py           # (Fase 5)
    ├── silver.py           # (Fase 6)
    └── gold.py             # (Fase 7)
```

**`databricks/databricks.yml`**
```yaml
bundle:
  name: f1-lake

include:
  - resources/*.yml

workspace:
  host: https://dbc-xxxx.cloud.databricks.com   # seu host
```

**`databricks/resources/uc.yml`**
```yaml
variables:
  catalog:
    description: Catalog UC do projeto
    default: prod

resources:
  # Catalog "prod" é criado uma única vez pela UI (Default Storage — a API
  # de catalog não funciona na Free Edition sem MANAGED LOCATION).
  schemas:
    f1_raw:
      catalog_name: ${var.catalog}
      name: f1_raw
    f1_bronze:
      catalog_name: ${var.catalog}
      name: f1_bronze
    f1_silver:
      catalog_name: ${var.catalog}
      name: f1_silver
    f1_gold:
      catalog_name: ${var.catalog}
      name: f1_gold

  volumes:
    landing:
      catalog_name: ${var.catalog}
      schema_name: f1_raw
      name: landing
      volume_type: MANAGED
```

**Criar o catalog `prod` na UI (uma única vez):** Catalog Explorer → **Create catalog** → nome `prod` → na opção de storage, escolher **Default Storage** → Create. (Via API/bundle dá erro `INVALID_STATE: Metastore storage root URL does not exist` — limitação da Free Edition.)

### 3.3 Deploy

```powershell
cd C:\dev\f1-lake\databricks
databricks bundle validate
databricks bundle deploy   # sem -t: o bundle só tem o target default
```

> Se o CLI reclamar "databricks OAuth is not supported for this host": limitação conhecida da Free Edition — autentique sempre via `databricks configure` com PAT.

### ✅ Checkpoint F3
Databricks UI → **Catalog**: schemas `f1_raw`, `f1_bronze`, `f1_silver`, `f1_gold` e o volume `prod.f1_raw.landing` visíveis.

---

## Fase 4 — Backfill 2018 → hoje (≈ 1 h de supervisão)

### 4.1 Disparar em chunks

Repo GitHub → **Actions → collect → Run workflow**:
- `from_year=2018, to_year=2021` → aguardar concluir
- `from_year=2022, to_year=2024` → aguardar
- `from_year=2025, to_year=2026` → aguardar

> **Por que em chunks:** laps é pesado (~1 h/ano). Chunks evitam estourar o timeout do runner (6 h) e facilitam re-run de ano com falha.

### ✅ Checkpoint F4
1. Databricks UI → **Catalog → prod → f1_raw → landing**: pastas `schedule/`, `results/`, `laps/`, `weather/` com `year=2018..2026`
2. Sanidade via SQL Editor (warehouse 2X-Small): `LIST '/Volumes/prod/f1_raw/landing/results';`

---

## Fase 5 — Bronze (≈ 1 h)

> Bronze = parquet do volume → tabelas Delta gerenciadas, **fiel à origem** (sem transformação de negócio). `CREATE OR REPLACE` a cada run (volume pequeno, idempotente — sem merge).

**`databricks/src/bronze.py`**

```python
SRC = "/Volumes/prod/f1_raw/landing"

for dataset in ["schedule", "results", "laps", "weather"]:
    df = spark.read.parquet(f"{SRC}/{dataset}")
    df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"prod.f1_bronze.{dataset}")
```

### ✅ Checkpoint F5
4 tabelas em `prod.f1_bronze`; contagem por ano em `results` cobre 2018→2026 (~24 GPs/ano × ~20 pilotos).

---

## Fase 6 — Silver (≈ 2-3 h)

> Silver = limpeza, tipagem e joins. Referência de modelagem: as feature stores do Téo (`fs_f1_driver_life`), agora com laps/weather integrados.

Tabelas alvo (todas `prod.f1_silver.*`):

| Tabela | Conteúdo |
|---|---|
| `dim_drivers` | 1 linha/piloto: id, nome, código, nacionalidade (dedup do histórico) |
| `dim_constructors` | id, nome, cor do time |
| `races` | eventos: ano, round, nome, país, circuito, datas das sessões |
| `results_clean` | position tipada (positionText → ord), pontos, status, flags finished/DNF |
| `laps_clean` | stint, pneu, tempos em segundos, flags pit in/out (deriva **pit stops** — sem dataset separado) |
| `weather_clean` | temp ar/pista, umidade, chuva por sessão |

Padrão bronze → silver (exemplo):

```python
from pyspark.sql import functions as F

results = spark.table("prod.f1_bronze.results")
(results
  .withColumn("position_order", F.col("Position").cast("int"))
  .withColumn("fl_finished", F.col("Status").rlike("^Finished|^\\+").cast("int"))
  .write.mode("overwrite").option("overwriteSchema", "true")
  .saveAsTable("prod.f1_silver.results_clean"))
```

### ✅ Checkpoint F6
`results_clean` joina com `dim_drivers` sem órfãos; `laps_clean` tem pit stops > 0 por corrida; totais batem com o bronze.

---

## Fase 7 — Gold (≈ 1-2 h)

> Gold = tabelas de relatório, prontas pra BI. Começamos com 3 e crescemos por demanda:

| Tabela | Pergunta que responde |
|---|---|
| `championship_history` | classificação do campeonato corrida a corrida (posição, pontos acumulados, gap pro líder) |
| `driver_career_stats` | carreira por piloto até cada data (pódios, poles, vitórias, voltas, médias — inspirado no `fs_f1_driver_life`) |
| `race_weather_impact` | resultado vs. condições climáticas por corrida |

### ✅ Checkpoint F7
`SELECT * FROM prod.f1_gold.championship_history WHERE year = 2024` retorna a classificação final correta (conferir com reportagem da época).

---

## Fase 8 — Job agendado (≈ 45 min)

**`databricks/resources/job.yml`**
```yaml
resources:
  jobs:
    f1_pipeline:
      name: f1-pipeline
      schedule:
        quartz_cron_expression: "0 0 12 ? * MON"   # segundas 12:00 UTC (09:00 GMT-3)
        timezone_id: "UTC"
      tasks:
        - task_key: bronze
          notebook_task: { notebook_path: ${workspace.file_path}/src/bronze }
        - task_key: silver
          depends_on: [{ task_key: bronze }]
          notebook_task: { notebook_path: ${workspace.file_path}/src/silver }
        - task_key: gold
          depends_on: [{ task_key: silver }]
          notebook_task: { notebook_path: ${workspace.file_path}/src/gold }
```

> Tasks sem `job_cluster_key` = **serverless** (obrigatório na Free Edition — não declare clusters).

```powershell
databricks bundle deploy -t dev
databricks bundle run f1_pipeline     # teste manual imediato
```

### ✅ Checkpoint F8
Jobs UI: run manual verde, ponta a ponta < 10 min. Schedule ativo pra segundas.

**Fluxo semanal completo:** Actions (seg 06:00 UTC: coleta + upload duplo) → Databricks job (seg 12:00 UTC: bronze→silver→gold).

---

## Fase 9 — Operação, custos e troubleshooting

### Rotina de custo zero (verificação mensal)
- [ ] Azure **Cost Management → Cost analysis**: gasto do mês = R$ 0 (5 GB LRS free; alerta de 5 BRL como cinto de segurança)
- [ ] Databricks: sem aviso de fair use (compute caiu = cota do dia estourada → reduzir chunk/reagendar)
- [ ] GitHub Actions: minutos ok (2.000/mês no privado; ilimitado no público)

### Manutenção conhecida

| Sintoma | Causa | Ação |
|---|---|---|
| Actions falha 401 no upload Databricks | PAT expirada (90 d sem uso) | Gerar nova → atualizar secret `DATABRICKS_TOKEN` |
| CLI: "databricks OAuth is not supported" | Free Edition não tem OAuth | Sempre `databricks configure` com PAT |
| FastF1 HTTP 429 | Rate limit Jolpica | Aumentar sleep em `collect.py`, re-run do ano |
| Job serverless não roda | Fair use excedido no dia | Aguardar reset; rodar em chunks menores |
| 403 no Files API | PAT sem permissão no volume | Regenere o PAT (você é admin do seu workspace) |
| Storage account/conn string esquecidos | — | `az storage account list` (Fase 1.2) regenera a connection string |

### Recomeço do zero (se precisar)

- **Azure (raw):** dados parquet no container `raw` são o **backup permanente** — só apague pelo Portal se quiser mesmo destruir tudo.
- **Databricks:** `databricks bundle destroy -t dev` e re-deploy (Fase 3 + Fase 8). O volume se reconstrói re-disparando o workflow de backfill.

### Pendências futuras (fora deste runbook)
Feature store/ABT → treino MLflow → app de predição · telemetria bruta (estoura os 5 GB free) · Power BI/dashboards · IaC se um dia o ambiente precisar ser replicado (Terraform/Bicep — hoje desnecessário).

---

## Resumo de tempos

| Fase | Conteúdo | Tempo |
|---|---|---|
| 0 | Ferramentas | 30 min |
| 1 | Validação Azure | 15 min |
| 2 | Repo + collector + Actions | ~4 h |
| 3 | Bundle UC | 1 h |
| 4 | Backfill 2018→2026 | ~1 h (supervisão) |
| 5-7 | Bronze/Silver/Gold | 4-6 h |
| 8 | Job agendado | 45 min |
| **Total** | | **2 a 3 dias de trabalho** |
