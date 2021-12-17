import json
import os
from datetime import date, timedelta, datetime

import requests
from fulfil_client import Client
from jinja2 import Template

from chalicelib import FULFIL_API_URL, DEV_EMAIL
from chalicelib.easypsot_tracking import get_n_days_old_orders
from .common import dates_with_passed_some_work_days, listDictsToHTMLTable
from chalicelib.email import send_email


client = Client(os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox'),
                os.environ.get('FULFIL_API_KEY', ''))


LATE_ORDER_TEXT = {
    'late':"""
<p>First off, thank you so much for getting your gold with us. Your support means the world.</p>
<p>Your order has unfortunately missed its estimated "ship by" date. Our deepest apologies here  -- we know you’ve got places to be, people to see, and new looks to rock (even if it’s just virtually). Your gold is currently in good hands and should be ready shortly, but for an exact ETA our team at care@auratenewyork.com is here for you.</p>
<p>Since we always strive for 100% perfection and don’t like being late, ever, please use the code IOU10 for 10% off your next order.</p>
<p>Thanks so much for your patience. We promise we’re doing everything we can to make sure your gold is worth the wait.</p>
<p>Warmest</p>
<p>X</p>
<p>The A-Team</p>
    """,
    'mto':"""
<p>First off, thank you so much for getting your gold with us. Your support means the world.</p>
<p>Your order has unfortunately missed its estimated "ship by" date. Our deepest apologies here  -- we know you’ve got places to be, people to see, and new looks to rock (even if it’s just virtually). Your gold just needs a little extra attention; we expect to ship it out within the next 2-10 business days. If this causes any questions or concerns, please reach out to us at care@auratenewyork.com and we will help in any way we can.</p>
<p>Since we always strive for 100% perfection and don’t like being late, ever, please use the code IOU10 for 10% off your next order.</p>
<p>Thanks so much for your patience. We promise we’re doing everything we can to make sure your gold is worth the wait.</p>
<p>Warmest</p>
<p>X</p>
<p>The A-Team</p>
    """,
    'vermeil':"""
<p>First off, thank you so much for getting your gold with us. Your support means the world.</p>
<p>Your order has unfortunately missed its estimated "ship by" date. Our deepest apologies here  -- we know you’ve got places to be, people to see, and new looks to rock (even if it’s just virtually). Your gold just needs a little extra attention, however, we expect to ship it out within the next 7-15 business days. If this causes any questions or concerns, please reach out to us at care@auratenewyork.com and we will help in any way we can.</p>
<p>Since we always strive for 100% perfection and don’t like being late, ever, please use the code IOU10 for 10% off your next order.</p>
<p>Thanks so much for your patience. We promise we’re doing everything we can to make sure your gold is worth the wait.</p>
<p>Warmest</p>
<p>X</p>
<p>The A-Team</p>
    """
}


def find_email_type(shipment):
    if shipment['mto']:
        if any(m['vermeil'] for m in shipment.get('all_moves', [])):
            return 'vermeil'
        else:
            return 'mto'
    else:
        return 'late'


def get_oldest_shipment(sale):
    shipment = sorted(sale['c'], key=lambda s: s['planned_date'], reverse=True)[0]
    planned_date = shipment['planned_date']
    moves = shipment.get('all_moves', [])
    email_type = find_email_type(shipment)
    return shipment, email_type, moves, planned_date


def find_late_orders():
    from app import BASE_DIR
    days_delay = 2
    emails = get_n_days_old_orders(days_delay, late=True)
    template = Template(open(f'{BASE_DIR}/chalicelib/template/late_order.html').read())

    if emails:
        late_orders_report = []
        for sale in emails:
            shipment, email_type, moves, planned_date = get_oldest_shipment(sale)
            if planned_date in dates_with_passed_some_work_days(days_delay) \
                    and (shipment['shipping_instructions'] == None or
                    'Planned date delayed' not in shipment['shipping_instructions']):
                report = {
                    'Shopify Order': sale['reference'],
                    'Customer Name': sale['party.name'],
                    'Customer Email': sale['party.email'],
                    'Planned Date Changed': 'No',
                }

                data = {
                    'YEAR': str(date.today().year),
                    'FINISH_DATE': planned_date,
                    # 'TRACK_LINK': get_link(sale['reference']),
                    'items': moves,
                    'TEXT': LATE_ORDER_TEXT[email_type]
                }
                result = template.render(**data)

                send_email(f"A small hiccup on our end",
                            result,
                            email=sale['party.email'],
                           # email=['maxwell@auratenewyork.com'],
                           # dev_recipients=True,
                           from_email='care@auratenewyork.com',
                           )
                # break
                if email_type in ['mto', 'vermeil']:
                    update_planned_date(shipment, email_type)
                    report['Planned Date Changed'] = 'Yes'
                late_orders_report.append(report)
        if late_orders_report:
            send_email(f"Fulfil: found {len(late_orders_report)} late orders",
                       str(listDictsToHTMLTable(late_orders_report)),
                       email=['maxwell@auratenewyork.com', 'jenny@auratenewyork.com'],
                       # email=['roman.borodinov@uadevelopers.com'],
                       dev_recipients=True)


def update_planned_date(shipment, email_type):
    if email_type == 'mto':
        delta = 10
    elif email_type == 'vermeil':
        delta = 15
    else:
        return
    Model = client.model("stock.shipment.out")
    planned_date = shipment['planned_date'] + timedelta(days=delta)
    shipping_instructions = f"{shipment['shipping_instructions']}" \
                            f"\r\nPlanned date delayed at {str(delta)} days."
    changes = {
               # 'planned_date': planned_date,
               'shipping_instructions': shipping_instructions}
    Model.write([shipment['id']], changes)


# headers = {
#     'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
#     'Content-Type': 'application/json'
# }

# def send_late_order_email(template, email, variant='late'):
#     text = LATE_ORDER_TEXT[variant]
#     data = {
#         'YEAR': str(date.today().year),
#         'TEXT': text,
#
#     }
#     result = template.render(**data)
#     send_email("A small hiccup on our end.", result, email)
#     # send_email("A small hiccup on our end.", result, email=['roman.borodinov@uadevelopers.com'])

# def get_order_data(order_id, fields):
#     url = f'{FULFIL_API_URL}/model/sale.sale/search_read'
#
#     payload = [[["id", "=", str(order_id)]], None, None, None, fields]
#
#     response = requests.put(url, data=json.dumps(payload), headers=headers)
#
#     if response.status_code == 200:
#         return response.json()[0]
#
#     print(response.text)
#
#     return None

#
# def find_late_orders_():
#     from app import BASE_DIR
#     url = f'{FULFIL_API_URL}/model/stock.shipment.out/search_read'
#     dates = dates_with_passed_some_work_days(3)
#
#     orders = []
#     shipments = []
#     for d in dates:
#         payload = [[
#             "AND",
#             [
#                 "planned_date", "=", {
#                 "__class__": "date",
#                 "year": d.year,
#                 "day": d.day,
#                 "month": d.month,
#             }
#             ],
#             ["state", "in", ["waiting", "packed", "assigned"]]
#         ], None, None, None, ["sales", "order_numbers"]]
#
#         response = requests.put(url, data=json.dumps(payload), headers=headers)
#
#         if response.status_code != 200:
#             send_email("Fulfil: check late orders",
#                        "Checking late orders wasn't successfull. See logs on AWS.")
#             print(response.text)
#         else:
#             shipments.extend(response.json())
#     if shipments:
#         for shipment in shipments:
#             if 'exe' not in shipment.get('order_numbers', ''):
#                 for order_id in shipment.get('sales'):
#                     order = get_order_data(
#                         order_id, ["reference", "party.name", "party.email"])
#
#                     # if not order:
#                     #     send_email(
#                     #         "Fulfil: failed to get Sales Order",
#                     #         f"Failed to get Sales Order with {order_id} ID.")
#                     #     continue
#
#                     orders.append(order)
#
#     if len(orders):
#         content = """
#             <table style="color: #000;">
#                 <tr>
#                     <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Shopify Order #</td>
#                     <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Customer Name/Last name</td>
#                     <td style="border: 1px solid #000; font-weight: bold; padding: 10px;">Customer Email</td>
#                 </tr>
#                 {}
#             </table>
#         """
#
#         rows = []
#         emails = []
#         for order in orders:
#             row = """
#                 <tr>
#                     <td style="border: 1px solid #000; padding: 10px;">{}</td>
#                     <td style="border: 1px solid #000; padding: 10px;">{}</td>
#                     <td style="border: 1px solid #000; padding: 10px;">{}</td>
#                 </tr>
#             """.format(order['reference'], order['party.name'],
#                        order['party.email'])
#             rows.append(row)
#             emails.append(order['party.email'])
#
#         data = "".join([row for row in rows])
#
#         table = content.format(data)
#
#         # send_email(f"Fulfil: found {len(orders)} late orders", table, dev_recipients=True)
#
#         template = Template(open(f'{BASE_DIR}/chalicelib/template/late_order.html').read())
#         for email in set(emails):
#             send_late_order_email(template, email, variant='late')
#             # break
#
#     else:
#         send_email("Fulfil: found 0 late orders", "Found 0 late orders", dev_recipients=True)
