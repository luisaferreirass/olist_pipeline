"""
Ingestão landing → bronze de um dia.

Uso (a partir da raiz do projeto):
    python -m src.ingest.ingest_bronze --date 2017-03-10
"""
import argparse
import os
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.spark import get_spark


def ingest_day(spark: SparkSession, day: date) -> None:
    dia = day.isoformat()
    caminho = f"data/landing/{dia}"
    if not os.path.exists(caminho):
        print(f"Não chegou nada em {dia}")
        return

    for file in os.listdir(caminho):
        if file.endswith(".csv"):
            tabela = os.path.splitext(file)[0] # nome do arquivo
            pasta = f"data/bronze/{tabela}" # nova pasta dentro da bronze

            # sem inferSchema, o Spark lê tudo como string (equivalente ao all_varchar do DuckDB)
            # multiLine + escape: campo entre aspas pode ter quebra de linha e aspas duplicadas ("") dentro
            df = spark.read.csv(f'{caminho}/{file}', header=True, quote='"', escape='"', multiLine=True)
            df = df.withColumn('_source_file', F.lit(f'{caminho}/{file}')).withColumn('ingestion_date', F.lit(dia).cast('date'))
            df.write.format('delta').mode('overwrite').partitionBy('ingestion_date').option('replaceWhere', f"ingestion_date = '{dia}'").save(pasta)

def main() -> None:
    parser = argparse.ArgumentParser(description='Ingere a landing de um dia na bronze.')
    parser.add_argument(
        '--date',
        required=True,
        type=date.fromisoformat,
        help='dia a processar (YYYY-MM-DD)',
    )
    args = parser.parse_args()
    spark = get_spark('olist-bronze')
    try:
        ingest_day(spark, args.date)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
