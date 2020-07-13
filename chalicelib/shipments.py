from datetime import timedelta
import datetime
import requests
from fulfil_client import ClientError
from retrying import retry

from chalicelib import (AURATE_STORAGE_ZONE)
from chalicelib.common import listDictsToHTMLTable
from .fulfil import client, headers, check_in_stock, get_movement


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
