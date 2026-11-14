# Olist Pipeline

Local batch data pipeline built on the public [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) e-commerce dataset, following the medallion architecture (bronze → silver → gold) with Spark and Delta Lake.

The dataset is sliced by day to simulate incremental data arrival, and each stage processes a single day at a time, idempotently.

## Architecture

```
data/raw  →  landing  →  bronze  →  silver  →  gold
 (CSV)      (CSV/day)    (Delta)    (Delta)    (Delta)
```

| Layer | What it does |
|---|---|
| **Landing** | "Daily" CSVs generated from the original dataset (simulates the source) |
| **Bronze** | Faithful copy of the source in Delta, everything as string, with `_source_file` and `ingestion_date` partition |
| **Silver** | Typing, standardization, quality checks and deduplication. Upsert (`MERGE`) on the natural key |
| **Gold** | Dimensional model (fact constellation schema): conformed dimensions and two fact tables |

### Gold model

- Dimensions: `dim_date`, `dim_customer` (SCD type 2), `dim_seller`, `dim_product`
- Facts: `fct_order_items` (1 row per item) and `fct_orders` (1 row per order)

### Idempotency

Reprocessing a day does not duplicate data: bronze overwrites the day's partition (`replaceWhere`) and silver runs a `MERGE` on the natural key.

## Structure

```
src/
├── slicing/    # raw → landing (daily slicing)
├── ingest/     # landing → bronze
├── transform/  # bronze → silver (SQL in transform/sql/)
├── model/      # silver → gold (SQL in model/sql/)
└── common/     # SparkSession with Delta
EDA/            # initial exploration (notebook)
```

## How to run

Requirements: Python 3.12 and Java (required by Spark).

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

1. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place the CSVs in `data/raw/`.
2. Generate the landing zone (deletes and recreates `data/landing/`):

```bash
python src/slicing/slice_raw.py
```

3. Process a day (from the project root):

```bash
python -m src.ingest.ingest_bronze --date 2017-03-10
python -m src.transform.transform_silver --date 2017-03-10
python -m src.model.model_gold --date 2017-03-10
```

The data (`data/`) is not versioned in the repository.

## Stack

PySpark, Delta Lake, pandas.
