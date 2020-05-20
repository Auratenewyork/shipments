import json
import os
from datetime import date, timedelta
from retrying import retry
import pdfkit
import requests
from fulfil_client import Client
from jinja2 import Template

from chalicelib import (
    AURATE_HQ_STORAGE, COMPANY, FULFIL_API_URL, RUBYHAS_HQ_STORAGE,
    RUBYHAS_WAREHOUSE, AURATE_OUTPUT_ZONE, AURATE_STORAGE_ZONE)
from chalicelib.email import send_email

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}

client = Client(os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox'), os.environ.get('FULFIL_API_KEY'))


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
    url = f'{get_fulfil_model_url("product.product")}?{field}={value}&fields={fieldsString}'

    if context:
        if type(context) is not str:
            context = json.dumps(context)
        url += f'&context={context}'

    response = requests.get(url, headers=headers)
    res = {}
    if response.status_code == 200:
        res_json = response.json()
        res = res_json[0] if len(res_json) > 0 else {}
    return res


def update_fulfil_inventory_api(product_id, product_quantity):
    params = [
        {
            'date': client.today(),
            'type': 'cycle',
            'lost_found': 7,
            'location': RUBYHAS_WAREHOUSE,
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
    from app import BASE_DIR
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
        emails = []
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
            emails.append(order['party.email'])

        data = "".join([row for row in rows])

        table = content.format(data)

        send_email(f"Fulfil: found {len(orders)} late orders", table)

        template = open(f'{BASE_DIR}/chalicelib/template/email.html', 'r').read()
        for email in set(emails):
            send_email("Late order!", template, email)

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


def get_report_template(report_id):
    if not report_id:
        raise TypeError("Specify id!")

    reports = client.model('ir.action.report.template')
    return reports.get(report_id)


def get_supplier_shipment(ss_id):
    if not ss_id:
        raise TypeError("Specify id!")

    ss = client.model('stock.shipment.in')
    return ss.get(int(ss_id))


def update_supplier_shipment(ss_id):
    if not ss_id:
        raise TypeError("Specify id!")

    url = f'{get_fulfil_model_url("stock.shipment.in")}/{ss_id}'

    response = requests.put(url, headers=headers, data={'state': 'done'})

    if response.status_code != 200:
        print(response.text)

    return response.status_code == 200


def get_contact_from_supplier_shipment(ss):
    contact = ss['contact_address']
    cm = client.model('party.address')
    return cm.get(int(contact))


def get_po_from_shipment(po_id):
    pp = client.model('purchase.purchase')
    return pp.get(int(po_id))


def get_line_from_po(line_id):
    pl = client.model('purchase.line')
    return pl.get(int(line_id))


def create_pdf(data, template, binary_path):
    template_rendered = Template(template)
    configs = {
        'barcode_height': 1.0,
        'barcode_width': 2.0,
        'barcode_dimension_uom': 'inch',
        'barcode_dpi': 300
    }
    file = template_rendered.render(barcodes=data, **configs)
    config = pdfkit.configuration(wkhtmltopdf=binary_path)
    pdf_string = pdfkit.from_string(file, output_path=False, configuration=config,
                                    options={'debug-javascript': '', 'javascript-delay': 2000})

    return pdf_string


def get_waiting_customer_shipments(offset, chunk_size):
    url = f'{get_fulfil_model_url("stock.shipment.out")}/search_read'

    payload = [
        [
            ["state", "=", "waiting"]
        ],
        offset,
        chunk_size,
        None,
        [
            "number",
            "moves",
            "delivery_address",
            "customer",
        ]
    ]

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(response.text)
        return None

    return response.json()


def check_in_stock(product_id, location_id, quantity):
    chunk_size = 500
    url = f'{get_fulfil_model_url("stock.location")}?fields=id,quantity_on_hand&context={{"product": {product_id}}}&per_page={chunk_size}'

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(response.text)
        return False

    locations = response.json()
    for item in locations:
        if item['id'] == location_id:
            return item.get('quantity_on_hand', 0) >= quantity
    return False


def delete_movement(movement_id):
    url = f'{get_fulfil_model_url("stock.move")}/{movement_id}'

    response = requests.delete(url, headers=headers)

    print(response.text)

    return response.status_code == 204


def create_customer_shipment(number, delivery_address, customer, products, **kwargs):
    url = f'{get_fulfil_model_url("stock.shipment.out")}'
    current_date = date.today().isoformat()
    moves = []
    state = kwargs.get('state', 'waiting')

    for product in products:
        movement = {
            'from_location': AURATE_HQ_STORAGE,
            'to_location': AURATE_OUTPUT_ZONE,
            'unit_price': None,
            'currency': 172,
            'uom': 1,
            'quantity': int(product.get('quantity')),
            'product': product.get('id'),
            'company': COMPANY,
        }
        moves.append(movement)

    shipment = [{
        'number': number,
        'from_location': RUBYHAS_HQ_STORAGE,
        'delivery_address': delivery_address,
        "customer": customer,
        'effective_date': None,
        'company': COMPANY,
        'planned_date': current_date,
        'state': state,
        'moves': [('create', moves)],
    }]

    response = requests.post(url, headers=headers, data=json.dumps(shipment))

    if response.status_code == 201:
        return json.loads(response.text)[0]
    print(response.status_code, response.reason)
    return None


@retry(stop_max_attempt_number=2, wait_fixed=50)
def join_shipments(candidates):

    def result_message(error_text='', skip_text=''):
        # creating summing message
        candidate_ids = [item['id'] for item in candidates]
        message = f'Join of {candidate_ids} '
        if error_text:
            return message + f'failed with message: {error_text}. \n'
        if skip_text:
            return message + f'{skip_text}. \n'
        else:
            return message + ' got SUCCESS.\n'

    def movements_in_stock(movements):
        # check that all movements are on stock
        movements = list(set(movements))
        Move = client.model('stock.move')
        requested_movements = list(Move.search_read_all(
            domain=['AND', [("id", "in", movements), ]],
            order=None,
            fields=['product', 'quantity']
        ))
        if len(requested_movements) != len(movements):
            return False
        result = True
        for movement in requested_movements:
            in_stock = check_in_stock(movement.get('product'),
                                      AURATE_STORAGE_ZONE,
                                      movement.get('quantity'))
            result = result and in_stock
        return result


    if len(candidates) > 1:
        # filtering that all moves from merge candidates are on the store
        checked_candidates = []
        for item in candidates:
            if movements_in_stock(item['moves']):
                checked_candidates.append(item)

        if len(checked_candidates) <= 1:
            print(result_message(
                skip_text='Have no sense (products are not on stock)')

            )
            return None

        # end filtering

        create_payload = {
            "method": "wizard.stock.shipment.out.merge.create",
            "params": [{}]}
        response = requests.post(url=client.host, headers=headers,
                                 json=create_payload)
        if response.status_code != 200 or 'error' in response.json:
            return result_message(response.json['error'])
        join_id = response.json()['result'][0]
        execute_payload = {
            "method": "wizard.stock.shipment.out.merge.execute",
            "params": [join_id, {}, "merge",
                       {"action_id": 346,
                        "active_model": "stock.shipment.out",
                        "active_id": checked_candidates[0]['id'],
                        "active_ids": [item['id'] for item in
                                       checked_candidates]}]}
        response = requests.post(url=client.host, headers=headers,
                                 json=execute_payload)

        # remove current stock.shipment.out.merge anyway
        delete_payload = {
            "method": "wizard.stock.shipment.out.merge.delete",
            "params": [join_id, {}]}
        requests.post(url=client.host, headers=headers,
                      json=delete_payload)

        if response.status_code != 200 or 'error' in response.json:
            return result_message(response.json['error'])

        return result_message()


def merge_shipments():

    def compare(l, r, fields):
        res = True
        for f in fields:
            res = res and l[f] == r[f]
        return res

    Model = client.model('stock.shipment.out')
    fields = ['customer', 'delivery_address', 'state']
    res = Model.search_read_all(
        domain=['AND', [("state", "in", ["waiting", "draft"]),
                        ["planned_date", ">",
                         {"__class__": "date", "year": 2020, "month": 3,
                          "day": 1}]]],
        order=[["delivery_address", "ASC"], ["state", "ASC"]],
        fields=fields+['moves'])
    join_candidates = []
    last = {}
    join_process_context = []
    for r in res:
        if last and not (compare(last, r, fields)):
            join_process_context.append(join_candidates)
            join_candidates = []
        last = r
        join_candidates.append(r)
    join_process_context.append(join_candidates)

    join_process_context = [item for item in join_process_context
                            if len(item) > 1]
    return join_process_context
