import json
import os
from datetime import datetime
from decimal import Decimal

import pytz
import requests
from fulfil_client import ClientError, ServerError, Client

from .fulfil import client, get_fulfil_model_url, headers
from .utils import fill_rollback_file, make_rollbaсk_filename


def close_running_production_orders():
    DOMAIN = os.environ.get('FULFIL_API_DOMAIN', "aurate-sandbox")

    Production = client.model('production')
    orders = Production.search_read_all(
        ['state', '=', 'running'], None, fields=['id', 'inputs', 'outputs'])
    orders = [order for order in orders]
    fill_rollback_file(orders, 'close_running_orders', server_name=DOMAIN)
    errors = []
    done = []
    for order in orders:  # had to change it one by one because of errors
        try:
            Production.write([order['id']], {'state': 'cancel'})
            done.append(order['id'])
        except Exception as e:
            errors.append({'id': order['id'], 'err': e.message})

    if errors:
        fill_rollback_file(errors, 'close_running_orders_errors', 'w+', server_name=DOMAIN)
        errors = []

    if done:
        filename = make_rollbaсk_filename('close_running_orders', server_name=DOMAIN)
        with open(filename, 'r') as rollback_file:
            data = json.loads(rollback_file.read())

        for order in data:
            try:
                prod_order = Production.get(order['id'])
            except Exception as e:
                errors.append({'id': order['id'], 'err': e.message})
            else:
                if order['inputs'] != prod_order['inputs'] or order['outputs'] != prod_order['outputs']:
                    errors.append(order)
        if errors:
            fill_rollback_file(errors, 'close_running_orders_changes', 'w+', server_name=DOMAIN)


def open_runnig_orders(filename='rollback_data/close_running_orders_04_30_2021_at_09PM.json'):  # rollback
    Production = client.model('production')
    with open(filename, 'r') as rollback_file:
        ids = json.loads(rollback_file.read())
    domain = [['AND', ["id", "in", ids]]]
    orders = Production.search_read_all(domain, None, fields=['id'])
    errors = []
    for order in orders:  # had to change it one by one because of errors
        try:
            Production.write([order['id']], {'state': 'running'})
        except Exception as e:
            errors.append({'id': order['id'], 'err': e.message})

    if errors:
        fill_rollback_file(errors, 'open_running_orders_errors', 'w+', server_name=DOMAIN)


def create_fulfill_order(data, channel_id='1'):
    # client = Client('aurate-sandbox', '43cf9ddb7acc4ac69586b8f1081d65ab')  # for sandbox
    # channel_id = 17  # for sandbox and Tmall channel
    # channel_id = 16  # for prod and Tmall channel
    channel_id = 3  # for Shopify channel
    # convert China time to utc
    confirmed_at = data['confirmed_at']
    tz = pytz.timezone('Asia/Shanghai')
    china_confirmed_at = datetime.strptime(confirmed_at, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
    utc_confirmed_at = china_confirmed_at.astimezone(pytz.timezone('utc'))
    data['confirmed_at'] = utc_confirmed_at.strftime('%Y-%m-%d %H:%M:%S')

    SaleChannel = client.model('sale.channel')
    return SaleChannel.create_order(channel_id, data)


def cancel_fulfill_order(data):
    reference = data['reference']
    Shipment = client.model('stock.shipment.out')
    fields = ['id', 'state']
    shipments = Shipment.search_read_all(
        domain=["AND", ['order_numbers', 'ilike', "%{}%".format(reference)]],
        order=None,
        fields=fields,
    )
    shipments = list(shipments)
    if shipments and shipments[0]['state'] != 'done':
        shipment = shipments[0]
        url = f'{get_fulfil_model_url("stock.shipment.out")}/{shipment["id"]}/cancel'
        requests.put(url, headers=headers)

    Sale = client.model('sale.sale')
    fields = ['id', 'state']
    sales = Sale.search_read_all(
        domain=['AND', [("reference", "=", reference,)]],
        order=[],
        fields=fields
    )
    sales = list(sales)
    if sales and sales[0]['state'] != 'done':
        sale = sales[0]
        url = f'{get_fulfil_model_url("sale.sale")}/{sale["id"]}/cancel'
        requests.put(url, headers=headers)

    return True

