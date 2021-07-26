import json
import os
from decimal import Decimal

from fulfil_client import ClientError, ServerError, Client

from .fulfil import client
from .utils import fill_rollback_file, make_rollbaсk_filename


DOMAIN = os.environ.get('FULFIL_API_DOMAIN', "aurate-sandbox")


def close_running_production_orders():
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
    client = Client('aurate-sandbox', '43cf9ddb7acc4ac69586b8f1081d65ab')  # for sandbox
    channel_id = 4  # for sandbox and shopify channel

    # data['amount'] = Decimal(data['amount'])
    # data['currency_code'] = 'USD'
    data['payment_term'] = 'Due on receipt'
    # for line in data['sale_lines']:
    #     line['amount'] = Decimal(line['amount'])
    #     line['unit_price'] = Decimal(line['unit_price'])

    SaleChannel = client.model('sale.channel')
    try:
        return SaleChannel.create_order(channel_id, data)
    except (ClientError, ServerError) as e:
        return e
