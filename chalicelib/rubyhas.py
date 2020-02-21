import json
import os

import requests

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


def build_purchase_order(reference, created_at, products):
    order = order_bluprint.copy()
    order[0]['number'] = reference
    order[0]['createdDateTime'] = created_at
    order_lines = []

    for product in products:
        order_line = order_line_bluprint.copy()
        order_line['itemNumber'] = product['sku']
        order_line['pack']['quantity'] = product['quantity']
        order_line['orderPackQuantity'] = product['quantity']
        order_line['createdDateTime'] = created_at
        order_lines.append(order_line)

    order[0]['orderLines']['orderLine'] = order_lines

    return order


def create_purchase_order(order):
    url = 'https://rby-int.deposco.com/integration/rby/orders'
    payload = {'order': order}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(
        url,
        data=json.dumps(payload),
        headers=headers,
        auth=(
            os.environ.get('RUBYHAS_USERNAME'),
            os.environ.get('RUBYHAS_PASSWORD')
        )
    )

    return response.status_code
