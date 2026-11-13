"""
Modelagem silver → gold de um dia.

Uso:
    python -m src.model.model_gold --date 2017-03-10

Modelo (architecture.md, D5, D7, D9 e D10): star em constelação.
    Dimensões: dim_date, dim_customer (SCD2), dim_seller, dim_product
    Fatos:     fct_order_items (1 linha por item), fct_orders (1 linha por pedido)

O SQL de cada tabela fica em sql/<tabela>.sql. O Python só orquestra: lê o arquivo,
executa e grava. As dimensões rodam antes das fatos, porque a fato precisa das
chaves substitutas das dimensões.
"""
import argparse
from datetime import date
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import SparkSession

from src.common.spark import get_spark

# caminhos a partir da raiz do projeto, não da pasta onde o comando foi rodado
ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / 'data' / 'silver'
GOLD = ROOT / 'data' / 'gold'
SQL_DIR = Path(__file__).resolve().parent / 'sql'

# tabelas da silver que a gold lê (cada uma vira uma view com o próprio nome)
SILVER_TABLES = [
    'customers', 'sellers', 'products', 'orders', 'order_items',
    'order_payments', 'order_reviews', 'category_translation', 'geolocation',
]


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def register_silver(spark: SparkSession) -> None:
    """Cria uma view para cada tabela da silver, para os .sql usarem `FROM orders`."""
    for table in SILVER_TABLES:
        path = (SILVER / table).as_posix()
        spark.read.format('delta').load(path).createOrReplaceTempView(table)


def register_gold(spark: SparkSession, table: str) -> None:
    """Cria uma view para uma tabela da gold já gravada (ex.: a fato lendo `dim_customer`)."""
    path = gold_path(table).as_posix()
    spark.read.format('delta').load(path).createOrReplaceTempView(table)


def register_old_gold(spark: SparkSession, table: str, schema: str) -> None:
    """Cria a view `<table>_old` com a gold da execução anterior (para manter as chaves estáveis).

    Se a tabela ainda não existe (primeira carga), a view fica vazia, com o `schema` (string DDL).
    """
    path = gold_path(table).as_posix()
    if DeltaTable.isDeltaTable(spark, path):
        df = spark.read.format('delta').load(path)
    else:
        df = spark.createDataFrame([], schema)
    df.createOrReplaceTempView(f'{table}_old')


def read_sql(table: str) -> str:
    return (SQL_DIR / f'{table}.sql').read_text(encoding='utf-8')


def gold_path(table: str) -> Path:
    return GOLD / table


def write_gold(spark: SparkSession, table: str, query: str) -> None:
    """Grava o resultado da query como a tabela da gold, de forma atômica.

    O overwrite do Delta é um único commit no _delta_log: os Parquet novos só passam a valer
    quando o commit é criado. Se cair no meio, a gold antiga fica inteira.
    """
    destino = gold_path(table)
    spark.sql(query).write.format('delta').mode('overwrite').save(destino.as_posix())


# ---------------------------------------------------------------------------
# Dimensões
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession, day: date) -> None:
    """dim_date: 1 linha por dia, de 2016-01-01 a 2019-12-31. Gerada, não vem da fonte.

    Chave: date_key (AAAAMMDD). Intervalo fixo: recriar todo dia dá sempre o mesmo resultado.
    """
    write_gold(spark, 'dim_date', read_sql('dim_date'))


def build_dim_seller(spark: SparkSession, day: date) -> None:
    """dim_seller: 1 linha por seller_id. Todos os atributos tipo 1.

    Chave: seller_sk (substituta, sequencial). seller_id fica como atributo.
    A chave precisa ser estável: lê a dim_seller anterior, mantém o seller_sk de quem já existe
    e dá max + 1, max + 2... aos novos (em ordem de seller_id, para ser determinístico).
    Os atributos (tipo 1) são sobrescritos com o estado atual da silver.
    Fonte: sellers + geolocation (lat/lng = mediana dos pontos do CEP).
    """
    # só as colunas que o SQL usa da versão anterior (o ON e o MAX); a view vazia só vale na 1ª carga
    schema = 'seller_sk BIGINT, seller_id STRING'
    register_old_gold(spark, 'dim_seller', schema)
    write_gold(spark, 'dim_seller', read_sql('dim_seller'))


def build_dim_product(spark: SparkSession, day: date) -> None:
    """dim_product: 1 linha por product_id. Todos os atributos tipo 1.

    Chave: product_sk (substituta, sequencial). product_id fica como atributo.
    A chave precisa ser estável: lê a dim_product anterior, mantém o product_sk de quem já existe
    e dá max + 1, max + 2... aos novos (em ordem de product_id, para ser determinístico).
    Os atributos (tipo 1) são sobrescritos com o estado atual da silver.
    Fonte: products + category_translation.
    Regra da categoria (sem_categoria / no_translation): tratamento_silver.md.
    """
    raise NotImplementedError


def build_dim_customer(spark: SparkSession, day: date) -> None:
    """dim_customer: 1 versão por pessoa (customer_unique_id). CEP, cidade, UF e lat/lng são tipo 2.

    Chave: customer_sk (substituta, sequencial: max + 1 para cada versão nova). customer_unique_id fica como atributo.
    Vigência: valid_from, valid_to (9999-12-31 na versão atual), is_current.
    Fonte: customers + geolocation (lat/lng = mediana dos pontos do CEP).
    Detecção de mudança: hash dos atributos tipo 2 (D7).
        hash igual     → nada muda
        hash diferente → fecha a versão atual e abre uma nova
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Fatos
# ---------------------------------------------------------------------------

def build_fct_order_items(spark: SparkSession, day: date) -> None:
    """fct_order_items: 1 linha por item (order_id, order_item_id). Transação.

    Idempotência (D7): insere só as chaves que ainda não existem.
    Chaves: customer_sk (versão vigente na compra), product_sk, seller_sk, date_key.
    Medidas: price, freight_value.
    """
    raise NotImplementedError


def build_fct_orders(spark: SparkSession, day: date) -> None:
    """fct_orders: 1 linha por pedido. Snapshot acumulado.

    Idempotência (D7): upsert pela chave, a partir do estado da silver.
    Fonte: orders + order_payments + order_reviews (+ order_items para os flags).
    Conteúdo: status, datas do ciclo, valor pago, review (a última + review_count, D10),
    flags que cruzam tabelas (flag_sem_itens, flag_pago_abaixo_itens...).
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

# ordem importa: dimensões antes das fatos
BUILDERS = [
    build_dim_date,
    build_dim_seller,
    build_dim_product,
    build_dim_customer,
    build_fct_order_items,
    build_fct_orders,
]


def model_day(spark: SparkSession, day: date) -> None:
    print(f'gold {day.isoformat()}')
    register_silver(spark)
    for build in BUILDERS:
        print(f'  {build.__name__.removeprefix("build_")}')
        build(spark, day)


def main() -> None:
    parser = argparse.ArgumentParser(description='Modela a silver de um dia na gold.')
    parser.add_argument(
        '--date',
        required=True,
        type=date.fromisoformat,
        help='dia a processar (YYYY-MM-DD)',
    )
    args = parser.parse_args()
    spark = get_spark('olist-gold')
    try:
        model_day(spark, args.date)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
