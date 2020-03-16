from chalicelib.email import send_email
import json
import os
from copy import copy

import requests

API_ENDPOINT = 'https://rby-int.deposco.com/integration/rby'
headers = {'Content-Type': 'application/json'}


def build_purchase_order(reference, created_at, products):
    order_bluprint = [{
        "businessUnit": "AURate",
        "number": None,
        "type": "Purchase Order",
        "status": "New",
        "orderPriority": "0",
        "createdDateTime": None,
        "notes": None,
        "orderLines": {
            "orderLine": None
        },
        "customMappings": {
            "@nil": "true"
        },
        "shippingStatus": "0",
    }]

    order_line_bluprint = {
        "businessUnit": "AURate",
        "lineStatus": "New",
        "itemNumber": None,
        "orderPackQuantity": 0,
        "pack": {
            "quantity": None
        },
        "createdDateTime": None,
        "notes": None,
        "customMappings": {
            "@nil": "true"
        }
    }

    order = order_bluprint[:]
    order[0]['number'] = reference
    order[0]['createdDateTime'] = created_at
    order_lines = []

    for product in products:
        order_line = copy(order_line_bluprint)
        order_line['itemNumber'] = product['sku']
        order_line['pack']['quantity'] = product['quantity']
        order_line['orderPackQuantity'] = product['quantity']
        order_line['createdDateTime'] = created_at
        order_line['notes'] = product.get('note')

        order_lines.append(order_line)

    order[0]['orderLines']['orderLine'] = order_lines

    return order


def create_purchase_order(order):
    url = f'{API_ENDPOINT}/orders'
    payload = {'order': order}

    response = requests.post(url,
                             data=json.dumps(payload),
                             headers=headers,
                             auth=(os.environ.get('RUBYHAS_USERNAME'),
                                   os.environ.get('RUBYHAS_PASSWORD')))

    return response


def get_item_quantity(item_number):
    url = f'{API_ENDPOINT}/items/AURate/{item_number}'
    error = None

    response = requests.get(url,
                            headers={
                                **headers, 'Accept': 'application/json'
                            },
                            auth=(os.environ.get('RUBYHAS_USERNAME'),
                                  os.environ.get('RUBYHAS_PASSWORD')))

    if response.status_code != 200:
        error = response.text

    try:
        item = response.json().get('item')[0]

        if item:
            return int(item.get('packs')['pack']['readyToShip'])
    except Exception as e:
        error = str(e)

    if error:
        send_email(
            "Ruby Has Report: Failed to get product quantity",
            f"Failed to get {item_number} product quantity. See logs on AWS.")

    return None
