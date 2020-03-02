from datetime import date
import json
import os

import requests

from chalicelib import (
    AURATE_HQ_STORAGE, COMPANY, FULFIL_API_URL, RUBYHAS_HQ_STORAGE)

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}


def get_engraving_order_lines():
    url = f'{FULFIL_API_URL}/model/sale.sale'
    params = {'created_at_min': date.today().isoformat()}
    order_lines = []

    response = requests.get(url, headers=headers, params=params)
    ids = [order['id'] for order in response.json()]

    for order_id in ids:
        order = get_order(order_id)

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
    shipments = response.json()
    ids = [shipment['id'] for shipment in shipments]

    for shipment_id in ids:
        internal_shipments.append(get_internal_shipment(shipment_id))

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


def get_internal_shipment(shipment_id):
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal/{shipment_id}'

    response = requests.get(url, headers=headers)

    return response.json()


def create_internal_shipment(products):
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal'
    current_date = date.today().isoformat()
    moves = []

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
        'reference': f'eng-{current_date}',
        'from_location': RUBYHAS_HQ_STORAGE,
        'to_location': AURATE_HQ_STORAGE,
        'effective_date': None,
        'company': COMPANY,
        'transit_required': False,
        'planned_date': current_date,
        'planned_start_date': current_date,
        'moves': [
            ('create', moves)
        ],
    }]

    response = requests.post(url, headers=headers, data=json.dumps(shipment))

    if response.status_code == 201:
        return json.loads(response.text)[0]

    return None
