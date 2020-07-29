from chalicelib.email import send_email
import json
import os
from copy import copy

import requests
from urllib.parse import urlencode


API_ENDPOINT = 'https://rby-int.deposco.com/integration/rby'
headers = {'Content-Type': 'application/json'}


def api_call(url, method='post', payload=None):
    url = f'{API_ENDPOINT}/{url}'

    try:
        _method = getattr(requests, method)
    except AttributeError as e:
        print(f'{method} not found, using get')
        _method = getattr(requests, 'get')

    kwargs = dict(headers=headers,
                  auth=(os.environ.get('RUBYHAS_USERNAME'),
                        os.environ.get('RUBYHAS_PASSWORD')))
    kwargs['url'] = url
    if payload and method == 'post':
        kwargs['data'] = json.dumps(payload)

    if payload and method == 'get':
        kwargs['url'] += f'?{urlencode(payload)}'

    print(f'Calling {url} link with params : {kwargs}')
    return _method(**kwargs)


def build_sales_order(reference, created_at, products):
    order_bluprint = [{
        "businessUnit": "AURate",
        "number": None,
        "type": "Sales Order",
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
        "shipVia": "UPS Next Day Air Saver",
        "shippingStatus": "0",
        "shipToAddress": {
            "name": "Nick",
            "contactName": "Nick",
            "attention": "Nick",
            "email": "Nick@auratenewyork.com",
            "phone": "2012890154",
            "addressLine1": "580 5th Ave",
            "addressLine2": "Suite 2300",
            "addressLine3": "",
            "city": "New York",
            "stateProvinceCode": "NY",
            "postalCode": "10036",
            "countryCode": "US"
        },
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
    url = 'orders'
    payload = {'order': order}

    return api_call(url, payload=payload)


def get_item_quantity(item_number):
    url = f'{API_ENDPOINT}/items/AURate/{item_number}'

    response = requests.get(url,
                            headers={'Accept': 'application/json'},
                            auth=(os.environ.get('RUBYHAS_USERNAME'),
                                  os.environ.get('RUBYHAS_PASSWORD')))

    if response.status_code != 200:
        print(response.text)

    try:
        item = response.json().get('item')[0]

        return int(item.get('packs')['pack']['readyToShip'])
    except Exception as e:
        print(response.text)
        print(str(e))

    return None


def get_full_inventory():
    page = 1
    inventories = {}
    while True:
        res = api_call('inventory/full',
                       method='get',
                       payload={
                           'pageNo': page,
                           'pageSize': 999,
                           'facilityNumber': 'RHNY'
                       })

        if res.status_code == 200:
            itemsinventory = res.json()
            if not itemsinventory:
                break

            for i in itemsinventory['itemInventory']:
                if i['itemNumber'].startswith('C-'):
                    continue
                if not i['itemNumber'] in inventories.keys():
                    inventories[i['itemNumber']] = {'rubyhas': 0}
                inventories[i['itemNumber']]['rubyhas'] = int(
                    i['facilityInventory']['inventory']['total'])

            page += 1
    return inventories
