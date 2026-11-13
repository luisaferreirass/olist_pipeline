"""
Fatiamento do dataset Olist em entregas diárias (simulação da fonte).

Lê data/raw e gera data/landing/<YYYY-MM-DD>/<tabela>.csv seguindo a D6 do architecture.md:
cada registro chega no dia em que passou a existir na vida real.

- orders, order_items, order_payments: dia da compra (order_purchase_timestamp)
- customers: dia do pedido ao qual o customer_id pertence
- order_reviews: dia de criação da review (review_creation_date)
- sellers, products: dia do primeiro item em que aparecem
- geolocation, category_translation: carga inicial

Tudo o que é anterior a START vai para a carga inicial (BACKFILL_DAY).
Rodar de novo apaga e recria a landing inteira, então o resultado é sempre o mesmo.

Uso (a partir da raiz do projeto):
    python src/slicing/slice_raw.py
"""
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / 'data' / 'raw'
LANDING = ROOT / 'data' / 'landing'

START = pd.Timestamp('2017-01-01')
END = pd.Timestamp('2018-10-17')
BACKFILL_DAY = START - pd.Timedelta(days=1)

FILES = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'category_translation': 'product_category_name_translation.csv',
}


def read_raw(name: str) -> pd.DataFrame:
    # tudo como texto e sem converter vazio em NaN: a landing entrega exatamente o que está no raw
    return pd.read_csv(RAW / FILES[name], dtype=str, keep_default_na=False)


def to_delivery_day(timestamps: pd.Series) -> pd.Series:
    day = pd.to_datetime(timestamps).dt.normalize()
    return day.where(day >= START, BACKFILL_DAY)


def assign_delivery_days(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Adiciona a coluna _day (dia de entrega na landing) em cada tabela."""
    orders = raw['orders'].assign(_day=lambda d: to_delivery_day(d['order_purchase_timestamp']))
    day_by_order = orders.set_index('order_id')['_day']
    day_by_customer = orders.set_index('customer_id')['_day']

    items = raw['order_items'].assign(_day=lambda d: d['order_id'].map(day_by_order))
    first_day_by_seller = items.groupby('seller_id')['_day'].min()
    first_day_by_product = items.groupby('product_id')['_day'].min()

    deliveries = {
        'orders': orders,
        'order_items': items,
        'order_payments': raw['order_payments'].assign(_day=lambda d: d['order_id'].map(day_by_order)),
        'customers': raw['customers'].assign(_day=lambda d: d['customer_id'].map(day_by_customer)),
        'order_reviews': raw['order_reviews'].assign(_day=lambda d: to_delivery_day(d['review_creation_date'])),
        'sellers': raw['sellers'].assign(_day=lambda d: d['seller_id'].map(first_day_by_seller)),
        'products': raw['products'].assign(_day=lambda d: d['product_id'].map(first_day_by_product)),
        'geolocation': raw['geolocation'].assign(_day=BACKFILL_DAY),
        'category_translation': raw['category_translation'].assign(_day=BACKFILL_DAY),
    }

    # todo registro precisa ter um dia de entrega dentro do período simulado
    for name, df in deliveries.items():
        missing = df['_day'].isna().sum()
        if missing:
            raise ValueError(f'{name}: {missing} linhas sem dia de entrega')
        if df['_day'].max() > END:
            raise ValueError(f'{name}: dia de entrega depois de {END.date()}')

    return deliveries


def write_landing(deliveries: dict[str, pd.DataFrame]) -> None:
    # apaga só as pastas de dia, para recriar a landing do zero
    LANDING.mkdir(parents=True, exist_ok=True)
    for day_dir in LANDING.iterdir():
        if day_dir.is_dir():
            shutil.rmtree(day_dir)

    for name, df in deliveries.items():
        for day, part in df.groupby('_day'):
            day_dir = LANDING / day.strftime('%Y-%m-%d')
            day_dir.mkdir(exist_ok=True)
            part.drop(columns='_day').to_csv(day_dir / f'{name}.csv', index=False)


def main() -> None:
    raw = {name: read_raw(name) for name in FILES}
    deliveries = assign_delivery_days(raw)
    write_landing(deliveries)

    n_days = sum(1 for d in LANDING.iterdir() if d.is_dir())
    print(f'landing gerada em {LANDING} com {n_days} dias de entrega')
    for name, df in deliveries.items():
        print(f'  {name:22s} {len(df):>9,d} linhas em {df["_day"].nunique():>4d} dias')


if __name__ == '__main__':
    main()
