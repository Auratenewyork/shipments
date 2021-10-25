import os
import re

import requests

from chalicelib.fulfil import client
from chalicelib.dynamo_operations import get_multiple_sku_info


SHOPIFY_APP_CRED = os.environ.get('SHOPIFY_APP_CRED', '')

def get_shopify_products():
    products = []
    url = f'https://{SHOPIFY_APP_CRED}/admin/api/2021-01/products.json'
    new_url = url
    for i in range(20):
        r = requests.get(new_url)
        b = r.json()
        products.extend(b['products'])

        link = r.headers.get('link', '')
        if 'next' not in link:
            break

        variants = link.split(', ')
        for variant in variants:
            if 'next' in variant:
                match = re.findall(r'(\?.+)>', variant)
                new_url = url + match[0]
                break
    return products


def shopify_products():
    products = get_shopify_products()
    return lost_orders(products)


def lost_orders(prod):
    shopify_quantities = {}
    for item in prod:
        for v in item['variants']:
            shopify_quantities[v['sku']] = {
                'inventory_quantity': v['inventory_quantity'],
                'shopify_product_id': item['id'],
                'shopify_variant_id': v['id']
            }

    lost_sku = list(shopify_quantities.keys())

    Model = client.model('product.product')
    fields = ["id", "code", "quantity_available"]
    products = Model.search_read_all(
        domain=["AND",["code","in", lost_sku],],
        order=None,
        fields=fields,
    )
    products = list(products)

    for p in products:
        p['shopify_quantity'] = shopify_quantities[p['code']]['inventory_quantity']
        p['difference'] = int(p['shopify_quantity']) - int(p['quantity_available'])

        p['shopify_product_id'] = shopify_quantities[p['code']]['shopify_product_id']
        p['shopify_variant_id'] = shopify_quantities[p['code']]['shopify_variant_id']

    products = list(filter(lambda x: bool(x['difference']), products))

    return products


def filter_shopify_customer(email=None):
    base_url = f'https://{SHOPIFY_APP_CRED}/admin/api/2021-01/customers/search.json'
    params = []
    if email:
        params.append(f'email:{email}')
    query = {'query': " ".join(params)}
    response = requests.get(base_url, params=query)
    data = response.json()
    customers = data.get('customers')
    return customers and customers[0]


def get_customer_orders(customer_id, status='any'):
    base_url = f'https://{SHOPIFY_APP_CRED}/admin/api/2021-01/customers/{customer_id}/orders.json'
    response = requests.get(base_url, params={'status': status})
    data = response.json()
    return data['orders']


def get_customer_orders_with_variants(customer_id, status='any'):
    orders = get_customer_orders(customer_id, status)
    shopify_variants = []
    for order in orders:
        extracted_variants = extract_variants_from_order(order)
        if extracted_variants:
            shopify_variants.extend(extracted_variants)
    return shopify_variants


def add_sku_info(items):
    variants = get_multiple_sku_info(sku_list=[v['sku'] for v in items if v['sku']])
    for one_variant in items:
        for v in variants:
            if v['PK'] == one_variant['sku']:
                one_variant.update(v)
                break
    return items


def extract_variants_from_order(order):
    return [
        {
            'order_id': order['id'],
            'order_name': order['name'],
            'product_id': variant['product_id'],
            'variant_id': variant['variant_id'],
            'id': variant['id'],
            'sku': variant['sku'],
        }
        for variant in order['line_items']
    ]


def shopify_get_products_by_ids(ids):
    base_url = f'https://{SHOPIFY_APP_CRED}/admin/api/2021-01/products.json'
    response = requests.get(base_url, params={'ids': ','.join(str(ids))})
    data = response.json()
    return data['products']