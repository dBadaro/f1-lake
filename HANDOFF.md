# HANDOFF — F1 Lake (gerado em 2026-10-27, ~01:50)

Referência para a próxima sessão: o que foi feito, onde paramos, próximos passos
e os fatos técnicos que não valem redescoberta. Doc vivo — atualizar a cada fase.
(O mapa completo do projeto continua no `f1-lake-runbook.md`; aqui é o estado de
agora.)

---

## 1. Contexto em uma linha

Projeto F1 Lake (réplica do TeoMeWhy/f1-lake, custo R$ 0): FastF1 → parquet →
ADLS `raw` + volume Databricks `prod.f1_raw.landing` → bronze/silver/gold.
Fases 0–3 do runbook concluídas; **estamos na Fase 4 (backfill 2018→2026), com
schedule/results 100% fechados e laps/weather faltando 6 anos**.

## 2. Estado de agora (verificado no ADLS/volume em 26/10 ~19h15)

| Dataset | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---|---|---|---|---|---|---|---|---|
| schedule | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| results  | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| laps     | ❌ | ❌ | ❌ | ⚠️ 32* | ❌ | ✅ 104 | ✅ 120 | ✅ 120 | ❌ |
| weather  | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ 104 | ✅ 120 | ✅ 120 | ❌ |

\* laps/2021 tem 32 arquivos da era mirror (run antigo) — recoletar com
`--overwrite` para garantir proveniência do livetiming real.

**Volume Databricks sincronizado (26/10 18:55→19:12):** `sync_volume.py`
espelhou o ADLS inteiro — 1433 arquivos (laps 376, weather 344, schedule 196,
results 517), contagem conferida via Files API. Foi a primeira vez que dados
chegaram ao volume de verdade (ver §4.2/§4.3).

## 3. O que foi feito (sessão de 2026-10-26)

- **2025 laps/weather COMPLETO** (16:17→18:25, ~2h05): 120 + 120, local, zero
  reqs ao mirror, uploads imediatos confirmados no ADLS. Log:
  `data/backfill-logs/2025.log`.
- **schedule/results 2018–2026 fechados:** run #11 das Actions (cron de
  segunda, automático) fechou 2026; run #12 (dispatch manual 15:58Z) fechou
  2022. **O lado Actions do backfill acabou** — só resta a rotina semanal.
- **sync do volume executado** (histórico e correções em §4).
- **Commit `5977d55` pushed**: fix truststore (ver §4.2).

Histórico anterior (diagnóstico do run cancelado, correções do commit
`b6f6cff`, noites de 2023/2024): ver seção 5 do runbook e o git log — não
redescobrir, mas não precisa reler para prosseguir.

## 4. Próximos passos (em ordem)

1. **Antes de qualquer laps/weather: checar o token F1TV — expira
   2026-10-27 16:24** (`python -m fastf1 auth f1tv --status`; se expirado,
   `python -m fastf1 auth f1tv --authenticate`). Token vazio = horas
   desperdiçadas com parquets vazios.
2. **Noite 1 (fila local, ~8h):** laps/weather de `2021` (SEM --overwrite —
   os 32 stale foram apagados) → `2018` → `2019` → `2020`. Encadear em
   background (mesmo esquema do run de 2025, um collector por vez — nunca em
   paralelo).
3. **Noite 2 (~3-4h):** `2022` → `2026` (ano corrente, só o que já correu).
   Com isso a Fase 4 fecha.
4. **Sync final do volume:** `python collector\sync_volume.py` (cobre o que
   vier a subir sem envs/falhar; idempotente). Nota: desde o PAT novo +
   truststore, os runs LOCAIS já sobem direto ao volume junto com o ADLS — o
   sync final é garantia, não obrigação.
5. **Checkpoint F4** (runbook): volume com 4 pastas cobrindo 2018–2026;
   `LIST '/Volumes/prod/f1_raw/landing/results';`
6. **Fase 5 — Bronze: ✅ COMPLETA, 4 tabelas (28/10 ~20h).** `schedule` 196
   linhas (2018-2026), `results` 10.494 (517 arquivos, 2018-2026), `laps`
   187.182 (344, 2023-2025), `weather` 34.143 (344, 2023-2025) — colunas
   `year`/`round`/`event`/`_source_file` derivadas do nome do arquivo (event:
   FP1/FP2/FP3/Qualify/Sprint Qualifying/Sprint/Race; schedule fica
   "SCHEDULE"). Job `f1-lake-bronze` (bundle) recria tudo idempotentemente —
   **re-rodar (`databricks bundle run bronze`, na pasta `databricks/`) após
   cada noite de backfill** para incorporar os anos novos. Saga das correções
   (28/10, não redescobrir): workspace novo só lança com
   `environments.spec.client: "2"`; `python_file_task` NEM EXISTE na API
   deste workspace; `input_file_name()` proibido no UC → usar
   `_metadata.file_name/file_path`; inferência de diretório falha
   (UNABLE_TO_INFER_SCHEMA) → schemas DDL explícitos no bronze.py (SCHEMAS) +
   `recursiveFileLookup=true` (sem isto lê 0 linhas); `LapStartDate`
   timestamp[ns] ilegível no Spark → migrado para us (344 arquivos) +
   collector grava `coerce_timestamps="us"`; schedules tinham fuso por evento
   (+11:00 AUS... +03:00) + colunas null (sessões canceladas 2020) → 171
   normalizados ao schema canônico (`data/backfill-logs/normaliza_schedules.py`,
   cast preserva instantes); **laps/2021 (32 stale da era mirror) foram
   APAGADOS do ADLS/volume** — a fila noturna recoleta 2021 SEM
   `--overwrite`.
7. **Pendência opcional:** trocar o secret `DATABRICKS_TOKEN` no GitHub —
   ainda tem o token ANTIGO (inválido); o `.env` local já tem o PAT novo. Sem
   isso, runs do Actions continuam sem subir ao volume (best-effort,
   inofensivo — o sync cobre).

## 5. Fatos de referência rápida (não redescobrir)

- **laps/weather NUNCA no Actions** (WAF/IP de datacenter + mirror vazio).
  Local sempre. Ritmo calibrado: **~2 h/ano** (laps ~1 min/sessão; weather
  ~20-25 min com cache quente).
- **Kaspersky intercepta o HTTPS do python.exe** com root própria
  ("Kaspersky Anti-Virus Personal Root Certificate") — `requests`/certifi
  falha com "self-signed certificate in certificate chain" em qualquer host
  (ex.: Databricks). Fix já no repo: `truststore.inject_into_ssl()` em nível
  de módulo de `collector/upload.py` (commit `5977d55`). Diagnóstico que
  engana: `curl.exe`/`openssl` veem a cadeia limpa (interceptação é por
  processo).
- **Token Databricks:** o antigo (49 chars) NUNCA foi válido — todos os
  uploads de volume de 2024/2025 falharam silenciosamente (240 warnings por
  log; tudo foi só pro ADLS). PAT novo (`dapi...`) no `.env` desde 26/10.
- A pasta `weather/{ano}` fica com **0 arquivos até o pass de laps do ano
  inteiro terminar** (um dataset por vez) — não é bug.
- No Windows, o `.venv\Scripts\python.exe` é um launcher fino (~5 MB) que
  hospeda o interpretador real (~170 MB, caminho do sistema): **dois
  python.exe com a mesma cmdline = UM run só**.
- **Não rodar dois collectors em paralelo** (cache SQLite do FastF1 + rate
  limit compartilhados). Fila em background morre se a sessão ZCode fechar.
- Jolpica: limite real 200/h (fastf1.req também impõe 500/h geral); pacing
  embutido via `pace_apis()`.
- Repo: github.com/dBadaro/f1-lake (público) — runs consultáveis sem auth em
  `https://api.github.com/repos/dBadaro/f1-lake/actions/runs`.
- Commit HEAD: `5977d55` (origin/main em dia). Untracked fora do versionamento
  de propósito: `HANDOFF.md`, `.vscode/`, `.zcodeignore`.
- Utilitários em `data/backfill-logs/`: `adls_counts.py` (cobertura por
  dataset/ano no ADLS), `check-procs.ps1` (há collector rodando?).
- **Regra do usuário (permanente): git commit/push só após diff + aprovação
  explícita.**

## 6. Comandos prontos (PowerShell, na raiz do repo)

```powershell
.\.venv\Scripts\Activate.ps1

# status do token F1TV (CHECAR ANTES de laps/weather — expira 27/10 16:24)
python -m fastf1 auth f1tv --status
python -m fastf1 auth f1tv --authenticate   # se expirado

# ano de laps/weather (resumível; re-rodar retoma de onde parou)
python collector\main.py --years 2021 --datasets laps weather --overwrite
python collector\main.py --years 2018 2019 2020 --datasets laps weather

# espelho ADLS -> volume Databricks (idempotente)
python collector\sync_volume.py

# cobertura por dataset/ano no ADLS / processo rodando?
python data\backfill-logs\adls_counts.py
powershell -File data\backfill-logs\check-procs.ps1
```
