from datetime import datetime, date, timedelta

import requests

from chalicelib import AURATE_HQ_STORAGE, RUBYHAS_WAREHOUSE, \
    WAREHOUSE_TO_STORAGE, AURATE_WAREHOUSE
from chalicelib.common import listDictsToHTMLTable
from chalicelib.email import send_email
from chalicelib.fulfil import client, get_fulfil_product_api, headers
from chalicelib.sync_sku import new_inventory, complete_inventory, \
    confirm_inventory
from collections import defaultdict
import traceback

boxes = None


def notify_end_up_boxes(few_boxes):
    info = str(listDictsToHTMLTable(few_boxes))
    send_email(
        f"Boxes are ending up",
        f"{date.today().strftime('%Y-%m-%d')}<br>" + info,
        dev_recipients=True,
        email=['maxwell@auratenewyork.com'],
    )


def check_end_up_boxes(updated_sku_by_warehouse):
    few_boxes = []
    for storage, updated_sku in updated_sku_by_warehouse.items():
        for i in updated_sku:
            if i['_to'] < 100:
                few_boxes.append(dict(
                    warehouse=storage,
                    code=i['code'],
                    id=i['_id'],
                    current_quantity=i['_to']
                ))
    if few_boxes:
        notify_end_up_boxes(few_boxes)


def process_boxes():
    shipments = collect_info()
    get_boxes()
    col_boxes = collect_boxes(shipments)
    updated_sku_by_warehouse = sku_for_update(col_boxes)
    try:
        sku_for_report = []
        count = 'Nothing changed'
        for storage, updated_sku in updated_sku_by_warehouse.items():
            sku_for_report.extend(updated_sku)

            count = new_inventory(updated_sku,  storage)
            complete_inventory(count)
            confirm_inventory(count)

        add_box_comment(shipments)

        info = str(listDictsToHTMLTable(sku_for_report))
        info += f'<br> processed CS {[item["rec_name"] for item in shipments]}'
        send_email(
            f"Fullfill: Sync boxes",
            f"{date.today().strftime('%Y-%m-%d')}, done. Inventory id {count}<br>" + info,
            dev_recipients=True,
            email=['maxwell@auratenewyork.com'],
        )
    except Exception as e:
        info = str(listDictsToHTMLTable(sku_for_report))
        info += f'processed CS {[item["rec_name"] for item in shipments]}'
        send_email(
            f"Fullfill: Sync boxes Fail",
            f"{date.today().strftime('%Y-%m-%d')}, fail <br>" + info,
            dev_recipients=True,
            email=['maxwell@auratenewyork.com'],
        )
        print(traceback.format_exc())


def create_box_comment(small=0, big=0, c_small=0, c_big=0):
    box_comment = []
    if small:
        box_parts = ['BX0001', 'PC0001', 'SH0001']
        for part in box_parts:
            for box in boxes[AURATE_WAREHOUSE]:
                if box['code'] == part:
                    box_comment.append('|'.join([box['rec_name'], str(small)]))
    if big:
        box_parts = ['BX0002', 'PC0002', 'SH0002']
        for part in box_parts:
            for box in boxes[AURATE_WAREHOUSE]:
                if box['code'] == part:
                    box_comment.append('|'.join(['', '', box['rec_name'], str(big)]))
    if c_small:
        box_parts = ['PC0001', 'SH0001']
        for part in box_parts:
            for box in boxes[AURATE_WAREHOUSE]:
                if box['code'] == part:
                    box_comment.append('|'.join([box['rec_name'], str(c_small)]))
    if c_big:
        box_parts = ['PC0002', 'SH0002']
        for part in box_parts:
            for box in boxes[AURATE_WAREHOUSE]:
                if box['code'] == part:
                    box_comment.append(
                        '|'.join(['', '', box['rec_name'], str(c_big)]))

    box_comment = '\n'.join(box_comment)
    return box_comment


def have_big_box(line):
    if 'chain' in line['product.rec_name'].lower():
        return True
    starts = ['AU0040', 'AU0150', 'AU0151', 'AU0233', 'AU0347', 'AU0031']
    for s in starts:
        if line['product.code'].startswith(s):
            return True


def collect_boxes(shipments):
    col_boxes = defaultdict(lambda: {'big': 0, "small": 0,
                                     "c_big": 0, "c_small": 0})
    for shipment in shipments:
        quantity = 0
        b = col_boxes[shipment['warehouse']]
        big_box = False
        package_separately = False
        sustainable_packaging = False

        for sale in shipment['sales_info']:
            sustainable_packaging = 'Sustainable packaging' in sale.get('shipping_instructions', '')
            package_separately = 'Package my items separately' in sale.get('shipping_instructions', '')
            for line in sale['lines_info']:
                if have_big_box(line):
                    big_box = True
                if package_separately and have_big_box(line):
                    b['big'] += 1
                elif package_separately:
                    b['small'] += 1
                quantity += line['quantity']

        if not package_separately:
            if sustainable_packaging:
                if quantity > 3 or big_box:
                    b['c_big'] += 1
                else:
                    b['c_small'] += 1
            else:
                if quantity > 3 or big_box:
                    b['big'] += 1
                else:
                    b['small'] += 1
        shipment['box'] = create_box_comment(small=b['small'], big=b['big'],
                                             c_small=b['c_small'], c_big=b['c_big'])
    return col_boxes


def collect_info():
    shipments = get_customer_shipments()
    sale_ids = []
    for item in shipments:
        sale_ids.extend(item['sales'])
    sales = get_sales(sale_ids)
    line_ids = []
    for sale in sales:
        line_ids.extend(sale['lines'])
    lines = get_order_lines(line_ids)
    for sale in sales:
        sale['lines_info'] = []
        for sale_line_id in sale['lines']:
            for line in lines:
                if sale_line_id == line['id'] and line['product.code'] != 'SHIPPING':
                    sale['lines_info'].append(line)
                    break
    for shipment in shipments:
        shipment['sales_info'] = []
        for sale in sales:
            if sale['id'] in shipment['sales']:
                shipment['sales_info'].append(sale)
    return shipments


def get_order_lines(ids):
    Line = client.model('sale.line')
    fields = ['note', 'product', 'quantity', 'product.code', 'note',
              'product.quantity_available', 'metadata', 'product.rec_name']
    lines = Line.search_read_all(
        domain=['AND', ['id', 'in', ids]],
        order=None,
        fields=fields
    )
    lines = list(lines)
    # self.mark_engraving_lines(lines)
    # self.mark_bundle_lines(lines)
    return list(lines)


# def get_customer_shipments():
#     Model = client.model('stock.shipment.out')
#     day_before = date.today() - timedelta(days=1)
#     fields = ['packed_date', 'sales']
#     CS = Model.search_read_all(
#         domain=[["AND", ["create_date", "=",
#                          {"__class__": "date", "year": day_before.year,
#                           "month": day_before.month, "day": day_before.day}],
#                  ["warehouse", "=", AURATE_WAREHOUSE]
#                  ]],
#         order=None,
#         fields=fields
#     )
#     return list(CS)


def get_customer_shipments():
    d = datetime.utcnow() - timedelta(days=1)
    filter_ = ['AND', ["create_date", ">",
                      {"__class__": "datetime", "year": d.year,
                       "month": d.month, "day": d.day,
                       "hour": d.hour,
                       "minute": d.minute, "second": d.second,
                       "microsecond": 0}],
              ]
    # filter_ = ['AND', ['id', '=', 67032]]
    fields = ['sales', 'contents_explanation', 'rec_name', 'warehouse']
    Shipment = client.model('stock.shipment.out')
    shipments = Shipment.search_read_all(
        domain=filter_,
        order=None,
        fields=fields
    )
    return list(shipments)


def get_sales(ids):
    Sale = client.model('sale.sale')
    fields = ['number', 'lines', 'reference', 'shipping_instructions']
    sales = Sale.search_read_all(
        domain=[["AND", ['id', 'in', ids]]],
        order=None,
        fields=fields
    )
    return list(sales)


def get_boxes():
    global boxes
    boxes = defaultdict(list)
    box_codes = ['BX0001', 'BX0002', 'PC0001', 'PC0002', 'SH0001', 'SH0002']
    for warehose in WAREHOUSE_TO_STORAGE.keys():
        for key in box_codes:
            product = get_fulfil_product_api(
                'code', key, 'id,quantity_on_hand,quantity_available,code,rec_name',
                {"locations": [warehose, ]}
            )
            boxes[warehose].append(product)


def sku_for_update(col_boxes):
    inventory = defaultdict(list)
    global boxes
    # for warehouse, b in col_boxes.items:
    for warehouse, box_s in boxes.items():
        for product in box_s:
            fulfil_inventory = product['quantity_on_hand']

            if product['code'][-1] == '1' and col_boxes[warehouse]['small']:
                _to = fulfil_inventory - col_boxes[warehouse]['small']
            elif product['code'][-1] == '2'and col_boxes[warehouse]['big']:
                _to = fulfil_inventory - col_boxes[warehouse]['big']
            elif product['code'][-1] == '1' and col_boxes[warehouse]['c_small']:
                if product['code'].startswith('BX'):
                    continue
                _to = fulfil_inventory - col_boxes[warehouse]['c_small']
            elif product['code'][-1] == '2' and col_boxes[warehouse]['c_big']:
                if product['code'].startswith('BX'):
                    continue
                _to = fulfil_inventory - col_boxes[warehouse]['c_big']
            else:
                continue

            inventory[WAREHOUSE_TO_STORAGE[warehouse]].append(
                dict(SKU=product['code'],
                     _id=product['id'],
                     _from=int(fulfil_inventory),
                     _to=int(_to),
                     warehouse=warehouse)
            )
    return inventory


def add_box_comment(shipments):
    contents_explanation = ''
    for shipment in shipments:
        if shipment['box']:
            contents_explanation += shipment['box']
            if shipment['contents_explanation']:
                contents_explanation += ';' + shipment['contents_explanation']
            update_shipment(shipment, {'contents_explanation': contents_explanation})


def update_shipment(shipment, context):
    Model = client.model('stock.shipment.out')
    url = Model.path + f'/{shipment["id"]}'
    response = requests.put(url, json=context, headers=headers)
    if response.status_code == 200:
        return True


