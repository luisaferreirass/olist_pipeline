"""
Criação da SparkSession com Delta Lake, usada por todas as camadas.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    # o Delta é um plugin: as configs entram no builder antes do getOrCreate
    builder = (
        SparkSession.builder.appName(app_name).master('local[*]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')  # MERGE, HISTORY, RESTORE, VACUUM
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')  # reconhece tabelas Delta
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()  # acrescenta o jar do Delta
