import json
import os
from datetime import date, timedelta, datetime

import pdfkit
import requests
from fulfil_client import Client
from jinja2 import Template

from chalicelib import (
    AURATE_HQ_STORAGE, COMPANY, FULFIL_API_URL, RUBYHAS_HQ_STORAGE,
    RUBYHAS_WAREHOUSE, AURATE_OUTPUT_ZONE)
from .common import dates_with_passed_some_work_days
from chalicelib.email import send_email

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}

client = Client(os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox'),
                os.environ.get('FULFIL_API_KEY', ''))


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
    Model = client.model('stock.shipment.internal')

    yesterday = datetime.utcnow() - timedelta(days=1)
    fields = ["reference", "state", "moves", "create_date"]
    internal_shipments = Model.search_read_all(
        domain=["AND",
                ["create_date", ">=", {
                    "__class__": "datetime",
                    "year": yesterday.year,
                    "month": yesterday.month,
                    "day": yesterday.day,
                    "hour": yesterday.hour,
                    "minute": yesterday.minute,
                    "second": 0,
                    "microsecond": 0
                }],
                ["state", "in", ["assigned"]],
                ["reference", "ilike", "automatic%"], ],
        order=None,
        fields=fields
    )
    return list(internal_shipments)


def get_movement(_id):
    Model = client.model('stock.move')
    move = Model.get(_id)
    return move


def get_product(_id):
    Model = client.model('product.product')
    product = Model.get(_id)
    return product


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
    if not products:
        return None
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

      # functionality moved into ""sync_sku"" REMOVE
# def update_fulfil_inventory_api(product_id, product_quantity):
#     params = [
#         {
#             'date': client.today(),
#             'type': 'cycle',
#             'lost_found': 7,
#             'location': RUBYHAS_HQ_STORAGE,
#             'lines': [['create', [{'product': product_id, 'quantity': product_quantity}]]],
#         }
#     ]
#
#     stock_inventory = client.model('stock.inventory')
#     res = stock_inventory.create(params)
#     stock_inventory.complete(res)
#     stock_inventory.confirm(res)
#
#     return res
#
#
# def update_stock_api(params):
#     inventory = client.model('stock.inventory')
#     inventory.complete(params)
#     inventory.confirm(params)
       # TO REMOVE TO




def find_late_orders():
    from app import BASE_DIR
    url = f'{FULFIL_API_URL}/model/stock.shipment.out/search_read'
    dates = dates_with_passed_some_work_days(3)

    orders = []
    shipments = []
    for d in dates:
        payload = [[
            "AND",
            [
                "planned_date", "=", {
                "__class__": "date",
                "year": d.year,
                "day": d.day,
                "month": d.month,
            }
            ],
            ["state", "in", ["waiting", "packed", "assigned"]]
        ], None, None, None, ["sales", "order_numbers"]]

        response = requests.put(url, data=json.dumps(payload), headers=headers)

        if response.status_code != 200:
            send_email("Fulfil: check late orders",
                       "Checking late orders wasn't successfull. See logs on AWS.")
            print(response.text)
        else:
            shipments.extend(response.json())
    if shipments:
        for shipment in shipments:
            if 'exe' not in shipment.get('order_numbers', ''):
                for order_id in shipment.get('sales'):
                    order = get_order_data(
                        order_id, ["reference", "party.name", "party.email"])

                    # if not order:
                    #     send_email(
                    #         "Fulfil: failed to get Sales Order",
                    #         f"Failed to get Sales Order with {order_id} ID.")
                    #     continue

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

        send_email(f"Fulfil: found {len(orders)} late orders", table, dev_recipients=True)

        template = open(f'{BASE_DIR}/chalicelib/template/email.html', 'r').read()
        template = template.replace('{{2020}}', str(date.today().year))
        for email in set(emails):
            send_email("A small hiccup on our end.", template, email)
            # send_email("A small hiccup on our end.", template, email=['roman.borodinov@uadevelopers.com'])
            # break

    else:
        send_email("Fulfil: found 0 late orders", "Found 0 late orders", dev_recipients=True)


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

def get_late_shipments():
    Shipment = client.model('stock.shipment.out')
    day_before = date.today() - timedelta(days=1)
    fields = ['number', 'order_numbers', 'total_quantity', 'sale_date',
              'planned_date']

    shipments = Shipment.search_read_all(
        domain=['AND', [["state", "!=","done"],["state","!=","cancel"],
                        ["planned_date","<",{"__class__":"date",
                                             "year":day_before.year,
                                             "month":day_before.month,
                                             "day":day_before.day}],
                        ]],
        order=None,
        fields=fields
    )
    result = []
    for item in sorted(shipments, key=lambda x: x['planned_date'], reverse=True):
        res = {}
        res['number'] = item['number']
        res['order_numbers'] = item['order_numbers']
        res['total_quantity'] = item['total_quantity']
        res['sale_date'] = item['sale_date']
        res['planned_date'] = item['planned_date']
        if item['planned_date'] - item['sale_date'] > timedelta(days=17):
            res['instock_or_MTO'] = 'MTO'
        else:
            res['instock_or_MTO'] = 'instock'
        result.append(res)

    return result


def get_items_waiting_allocation(d):
    url = 'https://aurate.fulfil.io/'
    data = {"method":"model.shipment.items_waiting_allocation.ireport.execute","params":[{"category":None,"end_date":{"__class__":"date","year":d.year,"month":d.month,"day":d.day},"template":None,"warehouse":None,"start_date":{"__class__":"date","year":d.year,"month":d.month,"day":d.day}},{"company":1,"allowed_read_channels":[1,3],"company.rec_name":"AUrate New York"}]}
    response = requests.post(url, headers=headers, json=data)
    a = response.json()
    return a['result']['data']


def get_inventory_by_warehouse():
    url = 'https://aurate.fulfil.io/'
    data = {"method":"model.inventory.by_warehouse.report.execute","params":[{"category":None,"quantity_type":"quantity_available"},{"workstation":None,"language":"en_US","roles":[],"employee.rec_name":"Roman","locale":{"date":"%m/%d/%Y","thousands_sep":",","decimal_point":".","grouping":[3,3,0]},"company":1,"allowed_read_channels":[1,3],"company.rec_name":"AUrate New York","currency_symbol":"$","employee":39,"warehouses":[4]}]}
    response = requests.post(url, headers=headers, json=data)
    a = response.json()
    return a['result']['data'], a['result']['columns']


def sale_with_discount(code, time_delta):
    d = datetime.utcnow() - time_delta + timedelta(seconds=2)
    result = []
    Sale = client.model('sale.sale')
    fields = ['id', 'rec_name', 'reference', 'comment', 'create_date', 'number']
    sales = Sale.search_read_all(
        domain=['AND', ["create_date", ">",
                        {"__class__": "datetime", "year": d.year,
                         "month": d.month, "day": d.day, "hour": d.hour,
                         "minute": d.minute, "second": d.second,
                         "microsecond": 0}]],
        order=[["create_date", "DESC"]],
        fields=fields
    )
    for sale in sales:
        if sale['comment'] and code in sale['comment']:
            result.append(sale)
    return result

def waiting_allocation():
    Model = client.model('stock.shipment.out')
    fields = ['id', "number", "moves", "delivery_address", "customer",
                'order_numbers', 'number', 'sales']
    shipments = Model.search_read_all(
        domain=[["AND",["on_hold","=",False],["state","=","waiting"]]],
        order=[["create_date", "DESC"]],
        fields=fields
    )
    return list(shipments)
