"""
Transformação bronze → silver de um dia.

Uso:
    python -m src.transform.transform_silver --date 2017-03-10
"""
import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from src.common.spark import get_spark

# caminhos a partir da raiz do projeto, não da pasta onde o comando foi rodado
ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / 'data' / 'bronze'
SILVER = ROOT / 'data' / 'silver'
SQL_DIR = Path(__file__).resolve().parent / 'sql'

# tabela → chave natural (tratamento_silver.md / D7 do architecture.md)
tabela_chaves = {
    'customers': ['customer_id'],
    'sellers': ['seller_id'],
    'products': ['product_id'],
    'orders': ['order_id'],
    'order_items': ['order_id', 'order_item_id'],
    'order_payments': ['order_id', 'payment_sequential'],
    'order_reviews': ['review_id', 'order_id'],
    'category_translation': ['product_category_name'],
    'geolocation': None,
}


def transform_table(spark: SparkSession, table: str, day: date) -> None:
    caminho = BRONZE / table
    if not caminho.exists():
        print(f'  {table}: nada chegou')
        return

    # o .sql lê de uma view chamada "bronze", que aponta só para a partição do dia
    do_dia = spark.read.format('delta').load(caminho.as_posix()).filter(f"ingestion_date = '{day}'")
    if do_dia.isEmpty():
        print(f'  {table}: nada chegou')
        return
    do_dia.createOrReplaceTempView('bronze')

    script = SQL_DIR / f'{table}.sql'
    sql = script.read_text(encoding='utf-8')

    temp_silver = spark.sql(f"SELECT *, DATE '{day.isoformat()}' AS ingestion_date FROM ({sql}\n)")
    temp_silver.createOrReplaceTempView('temp_silver')

    # upsert por chave só funciona se a chave for única e preenchida no que chegou hoje (a silver antiga já foi verificada)
    if tabela_chaves[table] is not None:
        chaves = ', '.join(tabela_chaves[table])

        # chave NULL não tem como entrar no upsert: no ROW_NUMBER, todas as NULL cairiam no mesmo grupo e só uma sobraria
        alguma_nula = ' OR '.join(f'{col} IS NULL' for col in tabela_chaves[table])
        nulas = spark.sql(f"SELECT count(*) FROM temp_silver WHERE {alguma_nula}").first()[0]
        if nulas:
            raise ValueError(f'{table}: {nulas} linhas com chave NULL em ({chaves})')

        duplicadas = spark.sql(
            f"SELECT count(*) FROM (SELECT {chaves} FROM temp_silver GROUP BY ALL HAVING count(*) > 1)"
        ).first()[0]
        if duplicadas:
            raise ValueError(f'{table}: {duplicadas} chaves duplicadas em ({chaves})')

    destino = SILVER / table
    if DeltaTable.isDeltaTable(spark, destino.as_posix()) and tabela_chaves[table] is not None:

        chaves_lista = tabela_chaves[table]
        condicao = ' AND '.join(f'destino.{col} = novo.{col}' for col in chaves_lista)

        spark.sql(f"""
                    MERGE INTO delta.`{destino.as_posix()}`  as destino
                    USING temp_silver as novo
                    ON {condicao}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
    elif tabela_chaves[table] is not None:
        temp_silver.write.format('delta').save(destino.as_posix())
    else:
        temp_silver.write.format('delta').mode('overwrite').option('replaceWhere', f"ingestion_date = '{day}'").save(destino.as_posix())

def transform_day(spark: SparkSession, day: date) -> None:
    print(f'silver {day.isoformat()}')
    for table in tabela_chaves:
        transform_table(spark, table, day)


def main() -> None:
    parser = argparse.ArgumentParser(description='Transforma a bronze de um dia na silver.')
    parser.add_argument(
        '--date',
        required=True,
        type=date.fromisoformat,
        help='dia a processar (YYYY-MM-DD)',
    )
    args = parser.parse_args()
    spark = get_spark('olist-silver')
    try:
        transform_day(spark, args.date)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
