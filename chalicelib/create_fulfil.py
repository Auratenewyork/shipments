import os
from datetime import datetime
from decimal import Decimal

import requests
from fulfil_client.client import dumps
from fulfil_client import Client

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}

client = Client(os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox'),
                os.environ.get('FULFIL_API_KEY', ''))



SERVICE_SKU = {
    'ring reshaping': 53974,
    'earring posts backs': 53975,
    'clasp repair/replacement': 53976,
    'jewelry polishing': 53977,
    'rhodium plating': 53978,
    'gem stone / diamond replacement services': 53979,
    'stone tightening': 53980,
}
"""
53974, '[SU0004] Ring Reshaping	Ring Reshaping', 'SU0004',
53975, '[SU0005] Earring Posts Backs	Earring Posts Backs', 'SU0005',
53976, '[SU0006] Clasp Repair/Replacement	Clasp Repair/Replacement', 'SU0006',
53977, '[SU0007] Jewelry Polishing	Jewelry Polishing', 'SU0007',
53978, '[SU0008] Rhodium Plating	Rhodium Plating', 'SU0008',
53979, '[SU0009] Gemstone/Diamond Replacement Services	Gemstone/Diamond Replacement Services', 'SU0009',
53980, '[SU0010] Stone Tightening Stone Tightening', 'SU0010',
"""



def create_fullfill_order(item):
    import ipdb; ipdb.set_trace()
    item['service'] = 'ring reshaping'

    subdomain = 'aurate'
    # subdomain = 'aurate-sandbox'
    token = 'ee41ebf87f4a4fd29696f8b5db6b8cfc'
    # token = '43cf9ddb7acc4ac69586b8f1081d65ab'
    client = Client(subdomain, token)
    headers = {
        'X-API-KEY': token,
        'Content-Type': 'application/json'
    }

    def get_fulfil_model_url(param):
        FULFIL_API_URL = f'https://{subdomain}.fulfil.io/api/v2'
        return f'{FULFIL_API_URL}/model/{param}'

    errors = []
    Model = client.model('sale.sale')
    sale = Model.search_read_all(
        domain=[["AND", ["reference", "=", item['order_name']]]],
        order=None,
        fields=['id', 'lines'],
        # batch_size=5,
    )
    DT = str(int(item['DT']))
    sale = sale.__next__()
    order_id = sale['id']

    Model = client.model('sale.line')
    f_lines = Model.search_read_all(
        domain=[["AND", ["id", "in", sale['lines']]]],
        order=None,
        fields=['product', 'product.code', 'quantity'],
    )
    f_lines = list(f_lines)
    # from .loopreturns import get_fulfil_model_url
    url = f'{get_fulfil_model_url("sale.sale")}/{order_id}/return_order'

    lines = []
    if True:
        ll = filter(lambda x: x['product.code'] == item['sku'], f_lines)
        line = ll.__next__()
        line_id = line['id']

        lines.append({
            "order_line_id": line_id,
            # Optional fields on line
            # ==================
            # "return_quantity": body_l[''],
            # defaults to the order line returnable quantity
            # "unit_price": "320.45",
            # defaults to the original order line unit price. Change this amount if the refund is not the full amount of the original order line.

            # If the return was created on an external returns platform,
            # the ID of the line
            "channel_identifier": DT,
            "note": "tracking_number " + item['tracking_number'],
            "return_reason": item['service'],  # Created if not exists
            'exchange_quantity': 1,
        })

    # if True:
    #     Model = client.model('product.product')
    #     # exchanges created through shopify
    #     product = Model.search_read_all(
    #         domain=[["AND", ["code", "=", item['sku']]]],
    #         order=None,
    #         fields=['id'],
    #     )
    #     product_id = product.__next__()['id']
    #     lines[i]['exchange_quantity'] = 1
    #     lines[i]['exchange_product'] = product_id
    #     lines[i]['exchange_unit_price'] = item['total']
    #     # # Exchange fields
    #     # # ==================
    #     # # +ve quantity of replacement item to ship to customer
    #     # "exchange_quantity": 1,
    #     # # ID of the product being sent.
    #     # # If replacement item is not specified, the same outbound item will be shipped.
    #     # "exchange_product": 1234,
    #     # # If the unit price is not specified, the unit price of the exchanged item is used.
    #     # "exchange_unit_price": "320.45",  # Unit price for outbound item

    if True:
        service_line = {
            "order_line_id": line_id,
            "return_quantity": 0,
            "note": item['service'],
            "exchange_quantity": 1,
            "exchange_product": SERVICE_SKU[item['service']],

        }
        lines.append(service_line)
    payload = [{
            "channel_identifier":  DT,  # Unique identifier for the return in the channel. This will be used as idempotency key to avoid duplication.
            "reference": f'repair-{item["order_name"]}',  # Return order reference, RMA
            "lines": lines,
            "warehouse": 57,
        }]
    payload = dumps(payload)
    response = requests.put(url, data=payload, headers=headers)
    if response.status_code != 500:
        print("success")
    # if response.status_code != 200:
    #     content = f'''
    #     error response from fullfill: {response.status_code}<br/>
    #     text: {response.text}<br/>
    #     url {url}<br/>
    #     payload: <br/>
    #     {json.dumps(payload)}
    #     '''
    #     send_email("Loop webhook error!!!!", content, dev_recipients=True)

    return response.text, errors

#
#
# def create_fullfill_order_(item):
#     subdomain = 'aurate-sandbox'
#     token = '43cf9ddb7acc4ac69586b8f1081d65ab'
#     client = Client(subdomain, token)
#
#     CHANNEL_ID = 37
#
#     order_details = get_order_details(item)
#     SaleChannel = client.model('sale.channel')
#     record = SaleChannel.create_order(CHANNEL_ID, order_details)
#     print()
#
#
# def get_order_details(item):
#     a = item['address']
#     channel_identifier = str(int(item['DT']))
#     order_details = {
#         'channel_identifier': channel_identifier,
#         'reference': channel_identifier,
#         "confirmed_at": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000+00:00'),
#         # "confirmed_at": "2021-05-11T08:20:23.251-05:00",
#         'customer': {
#             'name': a['name'],
#             'contacts': [
#                 ['email', item['email']]
#             ],
#         },
#
#         'billing_address': {
#             'name': a['name'],
#             'address1': a['address1'],
#             'address2': a['address2'],
#             'city': a['city'],
#             'zip': '50001',
#             'subdivision_code': a['province_code'],
#             'country_code': a['country_code'],
#             'email': item['email'],
#             'phone': a['phone'].replace('(', '').replace(')', '').replace('-', '').replace(' ', ''),
#         },
#         'shipping_address': {
#             'name': a['name'],
#             'address1': a['address1'],
#             'address2': a['address2'],
#             'city': a['city'],
#             'zip': '',
#             'subdivision_code': a['province_code'],
#             'country_code': a['country_code'],
#             'email': item['email'],
#             'phone': a['phone'].replace('(', '').replace(')', '').replace('-', '').replace(' ', ''),
#         },
#         'sale_lines': [
#             {
#                 'sku': item['sku'],
#                 'quantity': 1,
#                 'unit_price': Decimal('1.00'),
#                 'amount': Decimal('1.00'),
#                 'comment': 'Repearment'
#             },
#         ],
#         'shipping_lines': [
#         ],
#         'amount': Decimal('1.00'),
#         'currency_code': 'USD',
#         'payment_term': 'NET 30',
#         'priority': 2,
#         'status': 'pending',
#         'financial_status': 'paid',
#         'fulfillment_status': 'unshipped',
#     }
#     return order_details
#
#
#
#
#
#
# def create_fullfill_return_(item):
#     subdomain = 'aurate-sandbox'
#     token = '43cf9ddb7acc4ac69586b8f1081d65ab'
#     client = Client(subdomain, token)
#
#     CHANNEL_ID = 37
#     item()
#     order_details = get_order(item)
#     SaleChannel = client.model('sale.channel')
#     return_created(body)
#     print()
# def return_created(body):
#     errors = []
#     Model = client.model('sale.sale')
#     sale = Model.search_read_all(
#         domain=[["AND", ["reference", "=", body['order_name']]]],
#         order=None,
#         fields=['id', 'lines'],
#         # batch_size=5,
#     )
#     sale = list(sale)
#     if not sale:
#         errors.append(f"Can't create return, didn't find any sale with reference {body['order_name']}")
#         return errors
#     sale = sale[0]
#
#     Model = client.model('sale.line')
#     f_lines = Model.search_read_all(
#         domain=[["AND", ["id", "in", sale['lines']]]],
#         order=None,
#         fields=['product', 'product.code', 'quantity'],
#     )
#     f_lines = list(f_lines)
#     order_id = sale['id']
#     url = f'{get_fulfil_model_url("sale.sale")}/{order_id}/return_order'
#
#     lines = []
#     for body_l in body['line_items']:
#         ll = filter(lambda x: x['product.code'] == body_l['sku'], f_lines)
#         if not ll:
#             errors.append(f"Line not found {body_l}\n")
#             continue
#         line = ll.__next__()
#         line_id = line['id']
#
#         lines.append({
#             "order_line_id": line_id,
#             # Optional fields on line
#             # ==================
#             # "return_quantity": body_l[''],
#             # defaults to the order line returnable quantity
#             # "unit_price": "320.45",
#             # defaults to the original order line unit price. Change this amount if the refund is not the full amount of the original order line.
#
#             # If the return was created on an external returns platform,
#             # the ID of the line
#             "channel_identifier": body_l['line_item_id'],
#
#
#             # "note": "tracking_number " + body['tracking_number'],
#             "return_reason": body_l["return_reason"],  # Created if not exists
#         })
#     if not lines:
#         errors.append("Can't create return, didn't find any line")
#         return errors
#     if body['exchanges']:
#         Model = client.model('product.product')
#
#     # exchanges created through shopify
#     for i, item in enumerate(body['exchanges']):
#         if len(lines) > i:
#             product = Model.search_read_all(
#                 domain=[["AND", ["code", "=", item['sku']]]],
#                 order=None,
#                 fields=['id'],
#             )
#             product_id = product.__next__()['id']
#             lines[i]['exchange_quantity'] = 1
#             lines[i]['exchange_product'] = product_id
#             lines[i]['exchange_unit_price'] = item['total']
#             # # Exchange fields
#             # # ==================
#             # # +ve quantity of replacement item to ship to customer
#             # "exchange_quantity": 1,
#             # # ID of the product being sent.
#             # # If replacement item is not specified, the same outbound item will be shipped.
#             # "exchange_product": 1234,
#             # # If the unit price is not specified, the unit price of the exchanged item is used.
#             # "exchange_unit_price": "320.45",  # Unit price for outbound item
#         else:
#             errors.append(f"failed to add exchange for {item}\n "
#                           f"there is more exchanges than returns")
#             break
#     payload = [{
#             "channel_identifier":  body['id'],  # Unique identifier for the return in the channel. This will be used as idempotency key to avoid duplication.
#             "reference": body["order_name"],  # Return order reference, RMA
#             "lines": lines,
#             "warehouse": 140,
#         }]
#
#     response = requests.put(url, json=payload, headers=headers)
#
#     if response.status_code != 200:
#         content = f'''
#         error response from fullfill: {response.status_code}<br/>
#         text: {response.text}<br/>
#         url {url}<br/>
#         payload: <br/>
#         {json.dumps(payload)}
#         '''
#         send_email("Loop webhook error!!!!", content, dev_recipients=True)
#
#     return response.text, errors