from datetime import date, timedelta
import json
import os

from fulfil_client import Client
import requests

from chalicelib import (
    AURATE_HQ_STORAGE, COMPANY, FULFIL_API_URL, RUBYHAS_HQ_STORAGE,
    RUBYHAS_WAREHOUSE)
from chalicelib.email import send_email

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

    if response.status_code == 200:
        return response.json()

    print(response.text)

    return None


def get_order_data(order_id, fields):
    url = f'{FULFIL_API_URL}/model/sale.sale/search_read'

    payload = [[["id", "=", str(order_id)]], None, None, None, fields]

    response = requests.put(url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        return response.json()[0]

    print(response.text)

    return None


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
    url = f'{FULFIL_API_URL}/model/stock.shipment.internal/search_read'
    internal_shipments = []
    yesterday = date.today() - timedelta(days=1)

    payload = [[
        "AND",
        [
            "create_date", ">=", {
                "__class__": "datetime",
                "year": yesterday.year,
                "month": yesterday.month,
                "day": yesterday.day,
                "hour": 17,
                "minute": 0,
                "second": 0,
                "microsecond": 0
            }
        ], ["state", "in", ["waiting", "assigned"]]
    ], None, None, None, ["reference", "state", "moves", "create_date"]]

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        send_email("Fulfil: failed to get internal shipments",
                   "Failed to get internal shipments via API")
        print(response.text)
        return []

    shipments = response.json()
    ids = [shipment['id'] for shipment in shipments]

    for shipment_id in ids:
        shipment = get_internal_shipment({'id': shipment_id})

        if shipment:
            internal_shipments.append(shipment)

    return internal_shipments


def get_product(item):
    product_id = item.get('product')
    quantity = int(item.get('quantity'))
    note = item.get('note')
    url = f'{FULFIL_API_URL}/model/product.product/{product_id}'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        product = response.json()

        return {
            'id': product_id,
            'sku': product['code'],
            'quantity': quantity,
            'note': note
        }

    return None


def get_movement(movement_id):
    url = f"{get_fulfil_model_url('stock.move')}/{movement_id}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()

    print(response.text)

    return None


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


def find_late_orders():
    url = f'{FULFIL_API_URL}/model/stock.shipment.out/search_read'
    current_date = date.today()
    in_three_days = current_date + timedelta(days=3)
    orders = []

    payload = [[
        "AND",
        [
            "planned_date", ">", {
                "__class__": "date",
                "year": current_date.year,
                "day": current_date.day,
                "month": current_date.month,
            }
        ],
        [
            "planned_date", "<", {
                "__class__": "date",
                "year": in_three_days.year,
                "day": in_three_days.day,
                "month": in_three_days.month
            }
        ], ["state", "in", ["waiting", "packed", "assigned"]]
    ], None, None, None, ["sales"]]

    response = requests.put(url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        send_email("Fulfil: check late orders",
                   "Checking late orders wasn't successfull. See logs on AWS.")
        print(response.text)

    else:
        shipments = response.json()

        for shipment in shipments:
            for order_id in shipment.get('sales'):
                order = get_order_data(
                    order_id, ["reference", "party.name", "party.email"])

                if not order:
                    send_email(
                        "Fulfil: failed to get Sales Order",
                        f"Failed to get Sales Order with {order_id} ID.")
                    continue

                orders.append(order)

    if len(orders):
        content = """
            <table style="color: #000;">
                <tr>
                    <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Shopify Order #</td>
                    <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Customer Name/Last name</td>
                    <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Customer Email</td>
                </tr>
                {}
            </table>
        """

        rows = []

        for order in orders:
            row = """
                <tr>
                    <td style="border: 1px solid #000; padding: 10px;">{}</td>
                    <td style="border: 1px solid #000; padding: 10px;">{}</td>
                    <td style="border: 1px solid #000; padding: 10px;">{}</td>
                </tr>
            """.format(order['reference'], order['party.name'],
                       order['party.email'])
            rows.append(row)

        data = "".join([row for row in rows])

        table = content.format(data)

        send_email(f"Fulfil: found {len(orders)} late orders", table)

    else:
        send_email("Fulfil: found 0 late orders", "Found 0 late orders")


def get_global_order_lines():
    url = f'{FULFIL_API_URL}/model/sale.sale/search_read'
    order_lines = []
    yesterday = date.today() - timedelta(days=1)

    payload = [[
        "AND", ["reference", "like", "GE%"], ["state", "in", ["processing"]],
        [
            "create_date", ">=", {
                "__class__": "datetime",
                "year": yesterday.year,
                "month": yesterday.month,
                "day": yesterday.day,
                "hour": 15,
                "minute": 0,
                "second": 0,
                "microsecond": 0
            }
        ]
    ], None, None, None, ["reference", "lines"]]

    response = requests.put(url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        print(response.text)
        return None

    orders = response.json()

    for order in orders:
        for order_line_id in order.get('lines', []):
            order_line = get_order_line(order_line_id)
            has_engraving = check_if_has_engraving(order_line)

            if not has_engraving:
                order_lines.append(order_line)

    return order_lines


def get_waiting_ruby_shipments():
    url = f"{get_fulfil_model_url('stock.shipment.out')}/search_read"
    payload = [
        [
            "AND",
            ["state", "=", "waiting"],
            ["warehouse", "=", RUBYHAS_WAREHOUSE],
        ],
        None,
        None,
        None,
        ["moves"]
    ]

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        return response.json()

    print(response.text)

    return None


def update_customer_shipment(shipment_id, payload):
    url = f"{get_fulfil_model_url('stock.shipment.out')}/{shipment_id}"

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(response.text)

    return response.status_code


def change_movement_locations(movement_id, from_location, to_location):
    url = f"{get_fulfil_model_url('stock.move')}/{movement_id}"
    payload = {"to_location": to_location, "from_location": from_location}

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(response.text)


def get_empty_shipments_count():
    url = f'{get_fulfil_model_url("stock.shipment.out")}/search_count'

    payload = [[
        "AND",
        ["reference", "=", None],
        ["state", "not in", ["done", "cancel"]]
    ]]

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        return response.json()

    print(response.text)

    return None


def get_empty_shipments(offset, chunk_size):
    url = f'{get_fulfil_model_url("stock.shipment.out")}/search_read'

    payload = [
        [
            "AND",
            ["reference", "=", None],
            ["state", "not in", ["done", "cancel"]]
        ],
        offset,
        chunk_size,
        None,
        ["reference", "sales"]
    ]

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        return response.json()

    print(response.text)
    return None


def cancel_customer_shipment(shipment_id):
    url = f'{get_fulfil_model_url("stock.shipment.out")}/{shipment_id}/cancel'

    response = requests.put(url, headers=headers)

    if response.status_code != 200:
        print(response.text)

    return response.status_code == 200
