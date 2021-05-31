import json
import os

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
