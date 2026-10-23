# DuckDB no f1-lake — opções de uso

DuckDB se encaixa no projeto em três pontos do pipeline onde subir um cluster
Databricks é desperdício. Dentro do Databricks ele é redundante (o Spark SQL já
faz esse papel).

## 1. Quality gate na coleta (uso mais valioso)

Hoje o collector salva o parquet e sobe sem validar nada. Um `validate.py`
poderia checar cada arquivo antes do upload:

```python
import duckdb

def validate(path: Path, dataset: str) -> bool:
    con = duckdb.connect()
    n, = con.execute(
        "SELECT count(*) FROM read_parquet(?)", [str(path)]
    ).fetchone()
    # ex.: results não pode ter posição nula, laps não pode ter 0 linhas, etc.
```

Camada de defesa barata: parquet corrompido/parcial do Jolpica não chega no
lake silenciosamente.

## 2. Validação no CI (GitHub Actions, passo 2.7)

No `collect.yml`, após a coleta diária, o runner roda DuckDB (binário único,
~60MB, zero configuração — perfeito para CI) e confere: schema esperado,
contagem de linhas > 0, colunas obrigatórias. Se algo quebrar, o workflow
falha **antes** de subir dado ruim para o ADLS — em vez de descobrir o
problema dias depois no Databricks.

## 3. Exploração local sem cluster

Com o lake em parquet local, consultas SQL instantâneas:

```sql
-- glob direto na árvore do lake
SELECT round_no, count(*)
FROM read_parquet('data/f1-lake/results/2024/*.parquet')
GROUP BY 1 ORDER BY 2 DESC;
```

Com a extensão `httpfs` dá para consultar **direto do ADLS** (URL do blob com
SAS/connection string) — o lake inteiro consultável do notebook local sem
baixar nada.

## Onde NÃO usar

- **Dentro do Databricks**: redundante — Free Edition já tem Spark SQL;
  duplicar transformações em dois motores é dívida.
- **Como substituto do lake**: o design raw-parquet + Databricks é o core do
  projeto; DuckDB é ferramenta de borda (validação/inspeção), não de
  armazenamento.

## Resumo

DuckDB = juiz de qualidade no collector e no CI + lupa de exploração local.
Custo ~zero, benefício direto na confiabilidade.

## Ferramentas já implementadas

**`scripts/inspeciona.py`** — exploração do lake (rode na raiz do repo):
- `python scripts/inspeciona.py` — cria/atualiza `lake.duckdb` com views
  `schedule`, `results`, `laps`, `weather` sobre os parquets locais (a view
  relê o glob a cada query — arquivos novos aparecem sem recriar nada)
- `python scripts/inspeciona.py --sql "SELECT ..."` — query rápida no terminal
- `python scripts/inspeciona.py --repl` — shell Python com `con` (duckdb) e
  `_adls_urls(prefixo)` (gera URLs + SAS de leitura p/ consultar o ADLS direto)
- `python scripts/inspeciona.py --list [prefixo]` — lista blobs no container raw

**Harlequin** (`pip install harlequin` no venv; TUI de SQL para DuckDB):
- `.venv\Scripts\harlequin lake.duckdb` — abre o IDE visual: esquemas à
  esquerda, editor SQL, resultados em tabela. Como o `lake.duckdb` usa views
  sobre os parquets, tudo que o collector gravar aparece ali automaticamente.
- `Ctrl+Enter` executa a query; `F5` alterna painel de resultados; `q` sai.

Nota: os parquets de results/laps/weather não têm coluna de round/ano — extraia
do `filename` (a views incluem a coluna): `regexp_extract(filename, '(\d{4}_\d{2})', 1)`.
