from chalicelib.email import send_email

from datetime import date, datetime as dt

import json
import os

import requests

from chalicelib import (AURATE_HQ_STORAGE, COMPANY, FULFIL_API_URL,
                        RUBYHAS_HQ_STORAGE)

from fulfil_client import Client

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}

CONFIG = {
    'location_ids': {
        'ruby_has': 23,
        'ruby_has_storage_zone': 26,
        'aurate_hq': 4,
        'aurate_hq_storage_zone': 3
    },
}

client = Client(os.environ.get('FULFIL_API_DOMAIN','aurate-sandbox'), os.environ.get('FULFIL_API_KEY'))


def get_engraving_order_lines():
    url = f'{FULFIL_API_URL}/model/sale.sale'
    params = {'created_at_min': date.today().isoformat()}
    order_lines = []

    response = requests.get(url, headers=headers, params=params)
    ids = [order['id'] for order in response.json()]

    for order_id in ids:
        order = get_order(order_id)

        if order.get('state') == 'processing':
            for order_line_id in order.get('lines'):
                order_line = get_order_line(order_line_id)
                has_engraving = check_if_has_engraving(order_line)

                if has_engraving:
                    order_lines.append(order_line)

    return order_lines


def get_order(order_id):
    url = f'{FULFIL_API_URL}/model/sale.sale/{order_id}'

    response = requests.get(url, headers=headers)
    order = response.json()

    return order


def get_order_line(order_line_id):
    url = f'{FULFIL_API_URL}/model/sale.line/{order_line_id}'

    response = requests.get(url, headers=headers)

    return response.json()


def check_if_has_engraving(order_line):
    note = order_line.get('note')

    if not note:
        return False

    return "engraving" in note.lower()


def get_internal_shipments():
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal'
    params = {'created_at_min': date.today().isoformat()}
    internal_shipments = []

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        send_email("Fulfil: failed to get internal shipments",
                   "Failed to get internal shipments via API")
        print(response.text)
        return []

    shipments = response.json()
    ids = [shipment['id'] for shipment in shipments]

    for shipment_id in ids:
        shipment = get_internal_shipment({'id': shipment_id})

        if shipment and shipment.get('state') in ['waiting', 'assigned']:
            internal_shipments.append(shipment)

    return internal_shipments


def get_product(movement):
    product_id = movement.get('product')
    sku = movement.get('item_blurb').get('subtitle')[0][1]
    quantity = movement.get('quantity')
    note = movement.get('note')

    return {'id': product_id, 'sku': sku, 'quantity': quantity, 'note': note}


def get_movement(movement_id):
    url = f'{FULFIL_API_URL}/model/stock.move/{movement_id}'

    response = requests.get(url, headers=headers)

    return response.json()


def get_internal_shipment(params):
    shipment_id = params.get('id')
    reference = params.get('reference')

    if shipment_id:
        url = f'{FULFIL_API_URL}/model/stock.shipment.internal/{shipment_id}'

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()

        send_email("Fulfil: failed to get internal shipment",
                   f"Failed to get IS with {shipment_id} ID")

    elif reference:
        url = f'{FULFIL_API_URL}/model/stock.shipment.internal?reference={reference}'

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()[0]

        send_email("Fulfil: failed to get internal shipment",
                   f"Failed to get {reference} IS")

    return None


def create_internal_shipment(reference, products, **kwargs):
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal'
    current_date = date.today().isoformat()
    moves = []
    state = kwargs.get('state', 'waiting')

    for product in products:
        movement = {
            'from_location': RUBYHAS_HQ_STORAGE,
            'to_location': AURATE_HQ_STORAGE,
            'unit_price': None,
            'currency': 172,
            'uom': 1,
            'quantity': int(product.get('quantity')),
            'product': product.get('id'),
            'company': COMPANY,
        }
        moves.append(movement)

    shipment = [{
        'reference': reference,
        'from_location': RUBYHAS_HQ_STORAGE,
        'to_location': AURATE_HQ_STORAGE,
        'effective_date': None,
        'company': COMPANY,
        'transit_required': False,
        'planned_date': current_date,
        'planned_start_date': current_date,
        'state': state,
        'moves': [('create', moves)],
    }]

    response = requests.post(url, headers=headers, data=json.dumps(shipment))

    if response.status_code == 201:
        return json.loads(response.text)[0]

    return None


def update_internal_shipment(shipment_id, data):
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal/{shipment_id}'

    response = requests.post(url, headers=headers, data=json.dumps(data))

    return response


def get_fulfil_model_url(model):
    return f'{FULFIL_API_URL}/model/{model}'


def get_fulfil_product_api(field, value, fieldsString, context):
    # Product = client.model('product.product')
    # product = Product.read(
    #     [_id],
    #     ['id', 'quantity_on_hand', 'quantity_available'],
    #     context={'locations': [rubyconf['location_ids']['ruby_has_storage_zone']]}
    # )
    # return product
    res = {}
    url = f'{get_fulfil_model_url("product.product")}?{field}={value}&fields={fieldsString}'

    if context:
        if type(context) is not str:
            context = json.dumps(context)
        url += f'&context={context}'

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        if isinstance(response.json(), list):
            res = response.json()[0]

    return res



def update_fulfil_inventory_api(product_id, product_quantity):

    params = [
      {
        'date': client.today(),
        'type': 'cycle',
        'lost_found': 7,
        'location': CONFIG['location_ids']['ruby_has_storage_zone'],
        'lines': [['create', [{'product': product_id, 'quantity': product_quantity}]]],
      }
    ]

    stock_inventory = client.model('stock.inventory')
    res = stock_inventory.create(params)
    return res


def update_stock_api(params):
    inventory = client.model('stock.inventory')
    inventory.complete(params)
    inventory.confirm(params)
