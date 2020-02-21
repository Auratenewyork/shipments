from datetime import date
import os

import requests

API_ENDPOINT = 'https://aurate-sandbox.fulfil.io/api/v2'
headers = {'X-API-KEY': os.environ.get('FULFIL_API_KEY')}


def get_internal_shipments():
    url = f'{API_ENDPOINT}/model/stock.shipment.internal'
    params = {'created_at_min': date.today().isoformat()}
    internal_shipments = []

    response = requests.get(url, headers=headers, params=params)
    shipments = response.json()
    ids = [shipment['id'] for shipment in shipments]

    for shipment_id in ids:
        internal_shipments.append(get_internal_shipment(shipment_id))

    return internal_shipments


def get_product(movement):
    sku = movement.get('item_blurb').get('subtitle')[0][1]
    quantity = movement.get('quantity')

    return {'sku': sku, 'quantity': quantity}


def get_movement(movement_id):
    url = f'{API_ENDPOINT}/model/stock.move/{movement_id}'

    response = requests.get(url, headers=headers)

    return response.json()


def get_internal_shipment(shipment_id):
    url = f'{API_ENDPOINT}/model/stock.shipment.internal/{shipment_id}'

    response = requests.get(url, headers=headers)

    return response.json()
