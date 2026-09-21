from decimal import ROUND_HALF_UP, Decimal
from datetime import timezone

import pandas as pd

from src.common.audit import record_hash
from src.common.errors import PipelineError
from src.transform.staging import join_reasons, quarantine_rows

STAGE = 'transform'
CENT = Decimal('0.01')
BASIS_POINT = Decimal('0.0001')
HASH_COLUMNS = [
    'order_id',
    'customer_id',
    'product_id',
    'order_timestamp',
    'customer_city',
    'customer_tier',
    'product_name',
    'category',
    'brand',
    'quantity',
    'unit_price',
    'discount_pct',
    'gross_amount',
    'discount_amount',
    'net_amount',
    'status',
]
CURATED_COLUMNS = HASH_COLUMNS + ['source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash']


def to_decimal(value, quantum):
    return Decimal(repr(float(value))).quantize(quantum, rounding=ROUND_HALF_UP)


def canonical(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if hasattr(value, 'astimezone'):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def row_hash(row):
    return record_hash({key: canonical(row[key]) for key in HASH_COLUMNS}, HASH_COLUMNS)


def build_curated(staging, quarantined_product_ids, run_id, processed_at):
    orders = staging['orders']
    customers = staging['customers']
    products = staging['products']
    quarantined_products = set(quarantined_product_ids)
    known_customers = set(customers['customer_id'])
    known_products = set(products['product_id']) | quarantined_products
    reasons = join_reasons(orders.index, [
        ('order_customer_not_found', ~orders['customer_id'].isin(known_customers)),
        ('order_product_not_found', ~orders['product_id'].isin(known_products)),
        ('order_product_quarantined', orders['product_id'].isin(quarantined_products)),
    ])
    quarantine = quarantine_rows('orders', orders, 'order_id', reasons, run_id, processed_at, stage='curated')
    valid = orders[reasons == '']
    joined = (
        valid
        .merge(customers[['customer_id', 'city', 'customer_tier']], on='customer_id', how='left', validate='many_to_one')
        .merge(products[['product_id', 'name', 'brand', 'category_name']], on='product_id', how='left', validate='many_to_one')
    )
    if len(joined) != len(valid):
        raise PipelineError(STAGE, f'the join changed the number of orders ({len(valid)} became {len(joined)})')
    unit_price = [to_decimal(value, CENT) for value in joined['unit_price']]
    discount_pct = [to_decimal(value, BASIS_POINT) for value in joined['discount_pct']]
    gross = [(Decimal(int(quantity)) * price).quantize(CENT, rounding=ROUND_HALF_UP) for quantity, price in zip(joined['quantity'], unit_price)]
    discount = [(amount * pct).quantize(CENT, rounding=ROUND_HALF_UP) for amount, pct in zip(gross, discount_pct)]
    net = [amount - deduction for amount, deduction in zip(gross, discount)]
    curated = pd.DataFrame({
        'order_id': joined['order_id'],
        'customer_id': joined['customer_id'],
        'product_id': joined['product_id'],
        'order_timestamp': joined['order_timestamp'],
        'customer_city': joined['city'],
        'customer_tier': joined['customer_tier'],
        'product_name': joined['name'],
        'category': joined['category_name'],
        'brand': joined['brand'],
        'quantity': joined['quantity'],
        'unit_price': unit_price,
        'discount_pct': discount_pct,
        'gross_amount': gross,
        'discount_amount': discount,
        'net_amount': net,
        'status': joined['status'],
        'source_updated_at': joined['updated_at'],
        'pipeline_run_id': run_id,
        'processed_at_utc': processed_at,
    })
    curated['record_hash'] = [row_hash(row) for row in curated[HASH_COLUMNS].to_dict('records')]
    curated = curated[CURATED_COLUMNS].sort_values('order_id').reset_index(drop=True)
    if len(orders) != len(curated) + len(quarantine):
        raise PipelineError(STAGE, f'curated: order counts do not balance ({len(orders)} != {len(curated)} + {len(quarantine)})')
    stats = {
        'orders_staged': len(orders),
        'curated_rows': len(curated),
        'quarantined_rows': len(quarantine),
        'quarantined_by_reason': quarantine['reasons'].value_counts().to_dict(),
    }
    return curated, quarantine, stats
