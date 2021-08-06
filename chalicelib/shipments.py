import csv
from datetime import timedelta
from fulfil_client import ClientError
from retrying import retry
import datetime
import json
import os
import requests

from chalicelib import (AURATE_STORAGE_ZONE)
from chalicelib.decorators import try_except
from chalicelib.common import listDictsToHTMLTable
from .fulfil import client, headers, check_in_stock, get_movement, get_fulfil_model_url
from .utils import fill_rollback_file, make_rollbaсk_filename, fill_csv_file, capture_to_sentry


DOMAIN = os.environ.get('FULFIL_API_DOMAIN', "aurate-sandbox")


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
        if response.status_code != 200 or 'error' in response.json():
            return result_message(response.json()['error'])
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

        if response.status_code != 200 or 'error' in response.json():
            return result_message(response.json()['error'])

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


def pull_shipments_by_date(end_datetime):
    Model = client.model('stock.shipment.out')
    start_datetime = end_datetime - timedelta(days=1)
    res = Model.search_read_all(
        domain=['AND', [  # ("state", "in", ["waiting", "draft"]),
            ["create_date", ">",
             {"__class__": "datetime", "year": start_datetime.year,
              "month": start_datetime.month, "day": start_datetime.day,
              "hour": start_datetime.hour, "minute": start_datetime.minute,
              "second": start_datetime.second, "microsecond": 0}],
            ["create_date", "<",
             {"__class__": "datetime", "year": end_datetime.year,
              "month": end_datetime.month, "day": end_datetime.day,
              "hour": end_datetime.hour, "minute": end_datetime.minute,
              "second": end_datetime.second, "microsecond": 0}]
        ]],
        order=None,
        fields=['number', 'state', 'order_numbers', ])
    return list(res)


def get_split_candidates():
    Model = client.model('stock.shipment.out')
    res = Model.search_read_all(
        domain=['AND', [("state", "=", "waiting")]],
        order=None,
        fields=["number", "moves", "delivery_address", "customer",
                'order_numbers', 'number', 'sales']
    )
    split_candidates = []
    for shipment in res:
        if len(shipment.get('moves')) > 2:
            split_candidates.append(shipment)
    return split_candidates


def split_shipment(shipment):

    # check Has Engraving
    Sale = client.model('sale.sale')
    for number in shipment['sales']:
        sale = Sale.get(number)
        if 'Has Engraving' in sale['comment']:
            return f"Skip product {shipment['number']} as it have " \
                   f"'Has Engraving' comment " \
                   f"in sale order {shipment['order_numbers']}"
    # check global orders
    for number in shipment['sales']:
        sale = Sale.get(number)
        if sale['reference'].startswith('GE'):
            return f"Skip product {shipment['number']} as it is " \
                   f"Global order."\
                   f"Sale order {shipment['order_numbers']}"

    Shipment = client.model('stock.shipment.out')
    instance = Shipment.get(shipment['id'])

    products_in_stock = []

    for movement_id in shipment.get('moves'):
        movement = get_movement(movement_id)

        if not movement:
            return (f"Failed to get [{movement_id}] movement. "
                              f"For product {instance['number']}")

        product_id = movement.get('product')
        quantity = movement.get('quantity')

        in_stock = check_in_stock(product_id, AURATE_STORAGE_ZONE, quantity)

        if in_stock:
            products_in_stock.append({
                'id': product_id,
                'quantity': quantity,
                'movement': movement_id,
            })

    if len(products_in_stock) and \
            len(products_in_stock) != len(shipment.get('moves')):
        move_quantity_to_split = [
            {"id": item['movement'],
             "quantity": int(item['quantity'])}
            for item in products_in_stock
        ]

        planned_date = instance['planned_date']
        shipment_id = instance['id']
        try:
            new_shipment_id = Shipment.split(shipment_id, move_quantity_to_split, planned_date)
        except ClientError as e:
            return f"An error occurred during split shipment № {shipment_id} : " \
                   + str(e)

        return (f'Modify shipment № {shipment_id}. '
                          f'Created new shipment № {new_shipment_id}. '
                          f'Movements moved to the new shipment: {move_quantity_to_split}')
    else:
        print(f"Shipment № {instance['id']} don't need split.")
        return None


def run_split_shipments():
    candidates = get_split_candidates()
    email_body = []
    for shipment in candidates:
        split_result = split_shipment(shipment)
        email_body.append(split_result)
        print(split_result)
    return "\n".join(email_body)


def weekly_pull():
    today = datetime.date.today()
    idx = (today.weekday() + 1) % 7
    end_date = today - datetime.timedelta(idx-1)
    start_date = end_date - timedelta(days=9)

    fields = ['state', 'number', 'planned_date', 'assigned_time','carrier', 'carrier_service', 'company', 'create_date', 'delivery_address_datetime', 'delivery_mode', 'full_delivery_address', 'id',  'insurance_amount', 'is_international_shipping', 'is_shippo', 'on_hold', 'order_confirmation_time', 'requested_delivery_date', 'requested_shipping_service', 'sale_date', 'tracking_number', 'warehouse']
    Model = client.model('stock.shipment.out')
    res = Model.search_read_all(
        domain=['AND', [
            ["planned_date", ">=",
             {"__class__": "date", "year": start_date.year,
              "month": start_date.month, "day": start_date.day,}],
            ["planned_date", "<",
             {"__class__": "date", "year": end_date.year,
              "month": end_date.month, "day": end_date.day,}]
        ]],
        order=None,
        fields=fields)
    finished, unfinished = [], []
    for item in res:
        if item['state'] == 'done':
            finished.append(item)
        else:
            delay = datetime.date.today() - item['planned_date']
            item['delay'] = delay.days
            unfinished.append(item)
    prefix = f'{start_date}-{end_date}'

    return finished, unfinished, fields, prefix


def customer_shipments_pull():
    end_datetime = datetime.datetime.combine(datetime.date.today(), datetime.datetime.max.time()) - datetime.timedelta(days=2)
    planned_date = end_datetime + datetime.timedelta(days=4)
    start_datetime = end_datetime - timedelta(days=1)

    Model = client.model('stock.shipment.out')
    fields = ['number', 'state', 'order_numbers', 'create_date', 'planned_date']
    res = Model.search_read_all(
        domain=['AND', [  ("state", "in", ["waiting", "assigned"]),
            ["create_date", ">",
             {"__class__": "datetime", "year": start_datetime.year,
              "month": start_datetime.month, "day": start_datetime.day,
              "hour": start_datetime.hour, "minute": start_datetime.minute,
              "second": start_datetime.second, "microsecond": 0}],
            ["create_date", "<",
             {"__class__": "datetime", "year": end_datetime.year,
              "month": end_datetime.month, "day": end_datetime.day,
              "hour": end_datetime.hour, "minute": end_datetime.minute,
              "second": end_datetime.second, "microsecond": 0}],
            ["planned_date", "<",
             {"__class__": "date", "year": planned_date.year,
              "month": planned_date.month,
              "day": planned_date.day}]
        ]],
        order=None,
        fields=fields
    )
    return list(res)


def get_previous_state(state):
    states = {
        'done': 'packed',
        'packed': 'assigned',
        'draft': 'assigned'
    }
    return states.get(state, state)


def build_shipment_url(shipment_id, state):
    suffixes = {
        'done': 'done',
        'packed': 'pack',
        'cancel': 'cancel',
    }
    suffix = suffixes.get(state)
    return f'{get_fulfil_model_url("stock.shipment.out")}/{shipment_id}/{suffix}'


def complete_customer_shipments(state='done', excludes=("done", "cancel"), move_location_id=None, get_from_file=True):
    Shipment = client.model('stock.shipment.out')
    Move = client.model('stock.move')

    domain = ["AND", ("state", "not in", excludes),
        ["planned_date", "<", {"__class__": "date", "year": 2021, "month": 1, "day": 1}]
    ]
    fields = ['id', 'state', 'moves']
    shipments = list(Shipment.search_read_all(domain, None, fields=fields))
    print('Found {} incomplete shipments'.format(str(len(shipments))))

    if get_from_file:
        filename = 'rollback_data/05_18_2021_at_03PM_shipments_to_complete_production.json'
        with open(filename, 'r') as rollback_file:
            ids = json.loads(rollback_file.read())
            domain = [['AND', ["id", "in", ids]]]
            shipments = list(Shipment.search_read_all(domain, None, fields=fields))
            print('{} shipments found by ids'.format(str(len(shipments))))
    else:
        ids = [sh['id'] for sh in shipments]
    excludes = list(excludes)
    excludes.append(state)
    already_done = Shipment.search_read_all([['AND', ["id", "in", ids], ["state", "in", excludes]]], None, fields=['id'])
    already_done = [sh['id'] for sh in already_done]
    print(already_done)

    if not shipments:
        return

    fill_rollback_file(shipments, 'complete_shipments', server_name=DOMAIN)
    errors = []
    done = []
    changed = []
    skipped = []
    for shipment in shipments:
        shipment_id = shipment['id']
        if shipment_id in already_done:
            skipped.append(shipment_id)

        if skipped and shipment_id != skipped[-1]:
            try:
                resp = requests.put(build_shipment_url(shipment_id, state), headers=headers)
            except Exception as e:
                errors.append({'id': shipment_id, 'err': e.message})
            else:
                content = str(resp.content)
                sh_state = shipment['state']
                print(f'{resp.status_code}: {resp.url} == {shipment_id}-{sh_state} - {content}')
                if resp.status_code != 200 or 'Error' in content:
                    errors.append({'id': shipment_id, 'err': content})

        if not errors or errors[-1]['id'] != shipment_id:
            new_sh = Shipment.get(shipment_id)
            new_state = new_sh['state']
            if new_state not in excludes:
                errors.append({'id': shipment_id, 'err': "not performed!"})
            else:
                done.append(shipment_id)
                move_id = new_sh['moves'][-1]  # the latest stock move
                move = Move.get(move_id)
                sh = shipment.copy()
                sh.pop('moves')
                if shipment_id in already_done:
                    sh['state'] = get_previous_state(state)
                    if move_location_id and move['to_location'] != move_location_id:
                        print(f'Wrong stock move for {shipment_id}')
                sh['stock_move'] = move_id
                sh['product_id'] = move['product']
                sh['product_sku'] = move['item_blurb']['subtitle'][0][1]
                sh['quantity'] = move['quantity']
                sh['quantity_available'] = move['quantity_available']
                changed.append(sh)

    file_prefix = 'shipments_{}'.format(state)
    if done:
        fill_rollback_file(done, file_prefix, 'w+', server_name=DOMAIN)

    if errors:
        fill_rollback_file(errors, file_prefix + '_errors', 'w+', server_name=DOMAIN)

    if changed:
        fill_csv_file(changed, 'moves_report', server_name=DOMAIN)

    print('Found {} shipments: \n{} - done; \n{} - skipped, \n{} - errors'.format(
        len(shipments), len(done), len(skipped), len(errors)))


def get_shipments_by_location(location, gt_id=None, fields=('id',)):
    Shipment = client.model('stock.shipment.out')
    domain = [
        "AND",
        ("moves.from_location.name", "=", location),
        ("state", "=", "assigned"),
        ("picking_status", "!=", "in-progress"),
        ('shipping_batch', '=', None)
    ]
    if gt_id:
        domain.append(("id", "<", gt_id))

    return list(Shipment.search_read_all(domain, None, fields=fields))


def create_shipments_batch(ids, warehouse, batch_name):
    Batch = client.model('stock.shipment.out.batch')
    value_list = [{'name': batch_name, 'shipments': [('add', ids)], 'warehouse': warehouse}]
    new_record_ids = Batch.create(value_list)
    return new_record_ids


@try_except(task='Rocio shipments batch creation')
def make_batch_for_rocio_shipments():
    ids = get_shipments_by_location(location='Rocio', fields=('id', 'moves'))
    if ids:
        batch_name = 'Rocio-{}'.format(ids[0]['id'])
        moves = ids[0]['moves']
        if moves:
            Move = client.model('stock.move')
            moves = list(Move.search_read_all(
                domain=["AND", ("id", "=", moves[0])],
                order=None,
                fields=['product.code']
            ))
            if moves:
                batch_name = moves[0]['product.code']
        ids = [shipment['id'] for shipment in ids]
        batch_data = create_shipments_batch(ids, 4, batch_name)
        capture_to_sentry(
            'Rocio Batch created',
            shipments=ids,
            batch_data=batch_data,
            email=['aurate2021@gmail.com'])
        print('Rocio Batch created for {}'.format(str(ids)))
