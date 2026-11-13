# Olist Pipeline

Pipeline de dados em lote (local) sobre o dataset público de e-commerce da [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), seguindo a arquitetura medalhão (bronze → silver → gold) com Spark e Delta Lake.

O dataset é fatiado por dia para simular a chegada incremental de dados, e cada etapa processa um único dia por vez, de forma idempotente.

## Arquitetura

```
data/raw  →  landing  →  bronze  →  silver  →  gold
 (CSV)      (CSV/dia)    (Delta)    (Delta)    (Delta)
```

| Camada | O que faz |
|---|---|
| **Landing** | CSVs "do dia", gerados a partir do dataset original (simula a fonte) |
| **Bronze** | Cópia fiel da origem em Delta, tudo como string, com `_source_file` e partição `ingestion_date` |
| **Silver** | Tipagem, padronização, qualidade e deduplicação. Upsert (`MERGE`) pela chave natural |
| **Gold** | Modelo dimensional (star em constelação): dimensões conformadas e duas tabelas fato |

### Modelo da gold

- Dimensões: `dim_date`, `dim_customer` (SCD tipo 2), `dim_seller`, `dim_product`
- Fatos: `fct_order_items` (1 linha por item) e `fct_orders` (1 linha por pedido)

### Idempotência

Reprocessar um dia não duplica dados: a bronze sobrescreve a partição do dia (`replaceWhere`) e a silver faz `MERGE` pela chave natural.

## Estrutura

```
src/
├── slicing/    # raw → landing (fatiamento por dia)
├── ingest/     # landing → bronze
├── transform/  # bronze → silver (SQL em transform/sql/)
├── model/      # silver → gold (SQL em model/sql/)
└── common/     # SparkSession com Delta
EDA/            # exploração inicial (notebook)
```

## Como rodar

Requisitos: Python 3.12 e Java (necessário para o Spark).

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

1. Baixe o dataset do [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) e coloque os CSVs em `data/raw/`.
2. Gere a landing (apaga e recria `data/landing/`):

```bash
python src/slicing/slice_raw.py
```

3. Processe um dia (a partir da raiz do projeto):

```bash
python -m src.ingest.ingest_bronze --date 2017-03-10
python -m src.transform.transform_silver --date 2017-03-10
python -m src.model.model_gold --date 2017-03-10
```

Os dados (`data/`) não são versionados no repositório.

## Stack

PySpark, Delta Lake, pandas.
