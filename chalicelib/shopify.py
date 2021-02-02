import re

import requests

from chalicelib.fulfil import client


def shopify_products():
    products = []
    url = 'https://27f0deb29be43dcfb36028780bcae00f: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@aurate.myshopify.com/admin/api/2021-01/products.json'
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
