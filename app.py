import csv
import io
import json
import math
import os
import pickle
from datetime import date, datetime, timedelta
from functools import lru_cache

import boto3
from chalice import Chalice, Cron, Response

from chalicelib import (
    AURATE_OUTPUT_ZONE, AURATE_STORAGE_ZONE, AURATE_WAREHOUSE, PRODUCTION,
    RUBYHAS_WAREHOUSE, easypost)
from chalicelib.common import listDictsToHTMLTable, CustomJsonEncoder
from chalicelib.email import send_email
from chalicelib.fulfil import (
    change_movement_locations, create_internal_shipment, find_late_orders,
    get_engraving_order_lines, get_fulfil_product_api, get_global_order_lines,
    get_internal_shipment, get_internal_shipments, get_movement, get_product,
    get_waiting_ruby_shipments, update_customer_shipment,
    update_fulfil_inventory_api, update_internal_shipment, update_stock_api,
    get_report_template,
    get_supplier_shipment, update_supplier_shipment,
    get_contact_from_supplier_shipment, create_pdf,
    get_po_from_shipment, get_line_from_po,
    get_empty_shipments_count, get_empty_shipments, cancel_customer_shipment,
    client as fulfill_client, get_late_shipments, get_items_waiting_allocation)
from chalicelib.rubyhas import (
    api_call, build_sales_order, create_purchase_order, get_item_quantity,
    get_full_inventory)
from chalicelib.shipments import (
    get_split_candidates, split_shipment, join_shipments, merge_shipments,
    pull_shipments_by_date, weekly_pull)
from chalicelib import web_scraper

app_name = 'aurate-webhooks'
env_name = os.environ.get('ENV', 'sandbox')
TIMEOUT = int(os.environ.get('TIMEOUT', 500))

app = Chalice(app_name=app_name)
s3 = boto3.client('s3', region_name='us-east-2')
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
app.debug = True


def get_lambda_prefix():
    return f'{app_name}-{env_name}-'


@lru_cache(maxsize=12)
def get_lambda_name(name, boto_client=None):
    stack_name = "aurate-stack-" + env_name + '-'
    converted_name = ''.join(word.title() for word in name.split('_'))
    lambda_name = stack_name + converted_name
    if not boto_client:
        boto_client = boto3.client('lambda')
    lambdas = boto_client.list_functions()
    for fn in lambdas['Functions']:
        if fn['FunctionName'].startswith(lambda_name):
            return fn['FunctionName']


@app.schedule(Cron(0, 8, '*', '*', '?', '*'))
def create_pos(event):
    internal_shipments = get_internal_shipments()
    orders = []
    errors = []
    success = []
    email_body = []

    for shipment in internal_shipments:
        state = 'assigned'
        products = []
        for movement_id in shipment.get('moves'):
            movement = get_movement(movement_id)
            product = get_product(movement)

            if not product:
                email_body.append(
                    f"Failed to get the product [{movement.get('product')}]")
                continue

            quantity = get_item_quantity(product['sku'])

            if quantity is None:
                email_body.append(
                    f"Failed to get the product [{movement.get('product')}] quantity"
                )
                continue

            # if at least one product out of stock
            # then set state of IS to waiting
            if quantity < product.get('quantity'):
                state = 'waiting'

            products.append(product)

        # update IS status if all products are in stock
        if state == 'assigned' and state != shipment.get('state'):
            shipment = update_internal_shipment(shipment.get('id'),
                                                {'state': state})

        sales_order = build_sales_order(
            shipment.get('reference'),
            shipment.get('create_date').get('iso_string'), products)

        orders.append(sales_order)

    for order in orders:
        response = create_purchase_order(order)

        if response.status_code > 207:
            print('response.status_code', response.status_code)
            print('response.text', response.text)
            errors.append(order)
        else:
            success.append(order)

    if len(errors):
        references = ", ".join([o[0]['number'] for o in errors])
        email_body.append(f"Failed to create purchase orders: {references}")

    if len(success):
        references = ", ".join([o[0]['number'] for o in success])
        email_body.append(
            f"Successfully created {len(success)} purchase orders: {references}"
        )

    if not len(errors) and not len(success):
        email_body.append("No Purchase orders created. No errors.")

    send_email(f"Ruby Has Report: Purchase orders",
               "<br />".join(email_body), dev_recipients=True)


# Terminated!!!!
# @app.schedule(Cron(0, 18, '*', '*', '?', '*'))
def engravings_orders(event):
    engravings = get_engraving_order_lines()
    products_in_stock = []
    products_out_of_stock = []
    current_date = date.today().isoformat()
    email_body = []

    product_check = {}
    for engraving in engravings:
        product = get_product(engraving)

        if not product:
            email_body.append(
                f"Failed to get the product [{engraving.get('product')}]")
            continue

        quantity = get_item_quantity(product['sku'])

        if product['id'] not in product_check.keys():
            product_check[product['id']] = quantity
        if quantity and quantity >= product['quantity']:
            product_check[product['id']] -= product['quantity']
        if product_check[product['id']] < 0:
            send_email("!!!IMPORTANT: Internal shipments (product check result)",
                       f"problem with {product['id']} created internal shipment "
                       f"with values more then available on rubyhas (app.py:157)",
                       dev_recipients=True)

        if quantity is None:
            email_body.append(
                f"Failed to get the product [{engraving.get('product')}] quantity"
            )
            continue
        if quantity >= product['quantity']:
            products_in_stock.append(product)
        elif quantity > 0:
            # split product quantity into two internal shipments
            # one for product quantity which is in the stock
            # another for product quantity which is out of stock
            quantity_out_of_stock = product['quantity'] - quantity
            product_in_stock = {**product, 'quantity': quantity}
            product_out_of_stock = {
                **product, 'quantity': quantity_out_of_stock
            }
            products_in_stock.append(product_in_stock)
            products_out_of_stock.append(product_out_of_stock)
        else:
            products_out_of_stock.append(product)

    if len(products_in_stock):
        reference = f'eng-{current_date}'
        shipment = create_internal_shipment(reference,
                                            products_in_stock,
                                            state='assigned')

        if not shipment:
            email_body.append(
                f"Failed to create \"{reference}\" IS for engravings in stock")
        else:
            email_body.append(
                f"Successfully created \"{reference}\" IS for engravings in stock"
            )

    if len(products_out_of_stock):
        reference = f'waiting-eng-{current_date}'
        shipment = create_internal_shipment(reference, products_out_of_stock)

        if not shipment:
            email_body.append(
                f"Failed to create \"{reference}\" IS for engravings out of stck"
            )
        else:
            email_body.append(
                f"Successfully created \"{reference}\" IS for engravings out of stock"
            )

    if not len(products_in_stock) and not len(products_out_of_stock):
        email_body.append(f"No engravings orders found today")

    send_email("Fulfil Report: Internal shipments",
               "<br />".join(email_body), dev_recipients=True)


@app.route('/rubyhas', methods=['POST'])
def purchase_order_webhook():
    request = app.current_request
    order = request.json_body[0]

    status_mapping = {
        'receiving': 'waiting',
        'picking': 'assigned',
        'complete': 'done',
        'released': 'done',
        'canceled': 'canceled',
    }

    if order.get('type') == 'PURCHASE_ORDER':
        number = order.get('number')
        internal_shipment = get_internal_shipment({'reference': number})
        order_status = order.get('status', '').lower()

        if internal_shipment and order_status in status_mapping.keys():
            update_internal_shipment(internal_shipment.get('id'),
                                     {'state': status_mapping[order_status]})
            send_email(
                "Fulfil Report: Internal shipment status changed",
                f"\"{number}\" IS status has been changed to {status_mapping[order_status]}"
            )

        else:
            send_email(
                "Fulfil Report: Failed to update Internal shipment status",
                f"Can't find {number} IS to update the status.")

    elif order.get('type') == 'SALES_ORDER':
        number = order.get('number')
        Model = fulfill_client.model('stock.shipment.out')
        shipment = Model.get(int(number.replace("CS", "", 1)))
        moves = shipment['moves']
        product_ids = set()
        for movement_id in moves:
            movement = get_movement(movement_id)
            for item in movement['item_blurb']['subtitle']:
                product_ids.add(item[1])
        for number in product_ids:
            syncinventories_id(number)

    return Response(status_code=200, body=None)


@app.schedule(Cron(0, 17, '*', '*', '?', '*'))
def find_late_orders_view(event):
    find_late_orders()


@app.schedule(Cron(0, 19, '*', '*', '?', '*'))
def handle_global_orders(event):
    order_lines = get_global_order_lines()
    current_date = date.today().isoformat()
    products_in_stock = []
    products_out_of_stock = []
    email_body = []

    if order_lines:
        for order_line in order_lines:
            product = get_product(order_line)

            if not product:
                email_body.append(
                    f"Failed to get the product [{order_line.get('product')}]")
                continue

            quantity = get_item_quantity(product['sku'])

            if quantity is None:
                email_body.append(
                    f"Failed to get the product [{order_line.get('product')}] quantity"
                )
                continue

            if quantity >= product['quantity']:
                products_in_stock.append(product)
            elif quantity > 0:
                # split product quantity into two internal shipments
                # one for product quantity which is in the stock
                # another for product quantity which is out of stock
                quantity_out_of_stock = product['quantity'] - quantity
                product_in_stock = {**product, 'quantity': quantity}
                product_out_of_stock = {
                    **product, 'quantity': quantity_out_of_stock
                }
                products_in_stock.append(product_in_stock)
                products_out_of_stock.append(product_out_of_stock)
            else:
                products_out_of_stock.append(product)

        if len(products_in_stock):
            shipment = create_internal_shipment(f'GE-{current_date}',
                                                products_in_stock,
                                                state='assigned')

            if not shipment:
                email_body.append(
                    "Failed to create IS for global orders in the stock")
            else:
                email_body.append(
                    "Successfully created IS for global orders in the stock")

        if len(products_out_of_stock):
            shipment = create_internal_shipment(f'GE-{current_date}-waiting',
                                                products_out_of_stock,
                                                state='waiting')

            if not shipment:
                email_body.append(
                    "Failed to create IS for global orders out stock")
            else:
                email_body.append(
                    "Successfully created IS for global orders out of stock")

    elif order_lines is not None:
        email_body.append("Found 0 global orders")
    else:
        email_body.append("Failed to get global orders. See logs on AWS.")

    send_email(f"Fulfil Report: Global orders",
               "<br />".join(email_body), dev_recipients=True)

    return None


@app.lambda_function(name='get_full_inventory_rubyhas')
def get_full_inventory_rubyhas(event, context):
    def chunks(dictionary, size):
        items = dictionary.items()
        return (dict(items[i:i + size]) for i in range(0, len(items), size))

    inventories = get_full_inventory()
    client = boto3.client('lambda')

    send_email("Fulfil Report: Sync Pipeline",
               "Parsed succesfully. Going to sync. You will be notified about the results via email.")

    for sub_inventory in chunks(inventories, 50):
        client.invoke(
            FunctionName=get_lambda_name('sync_fullfill_rubyhas'),
            InvocationType='Event',
            Payload=json.dumps(sub_inventory)
        )


@app.lambda_function(name='sync_fullfill_rubyhas')
def sync_fullfill_rubyhas(inventories):
    synced = 0
    not_founded_sku = []
    for _id, i in inventories.items():
        product = get_fulfil_product_api(
            'code', _id, 'id,quantity_on_hand,quantity_available',
            {"locations": [RUBYHAS_WAREHOUSE, ]})

        if 'quantity_on_hand' not in product:
            not_founded_sku.append(_id)
            continue

        fulfil_inventory = product['quantity_on_hand']

        # No need to update
        if int(i['rubyhas']) == int(fulfil_inventory):
            continue

        stock_inventory = update_fulfil_inventory_api(product['id'],
                                                      i['rubyhas'])
        if stock_inventory:
            update_stock_api(stock_inventory)
            synced += 1

    data = {
        synced:
            f'Finished inventory update script - updated {synced} stock levels in Fulfil'
    }
    if not_founded_sku:
        data['not_founded'] = ':'.join([
            'List SKU of not founded in fulfil products',
            ', '.join(not_founded_sku)
        ])

    send_email(
        f'Results for syncing inventories at {datetime.today().strftime("%d/%m/%y")}',
        '\r\n'.join(
            '{} : {}'.format(key, value) for key, value in data.items()))


@app.schedule(Cron(59, 23, '?', '*', '*', '*'))
def syncinventories_event(event):
    syncinventories_all()


@app.route('/syncinventories', methods=['GET'])
def syncinventories_all():
    client = boto3.client('lambda')

    response = client.invoke(
        FunctionName=get_lambda_name('get_full_inventory_rubyhas'),
        InvocationType='Event',
    )

    if response['StatusCode'] == 202:
        body = "The function has been successfully started. You will be notified about the results via email."
    else:
        body = f"Something went wrong during the function invokaction. See logs on AWS. Response : \n " \
               f"Status : {response['StatusCode']}" \
               f"LogResult : {response['LogResult']}" \
               f"FunctionError : {response['FunctionError']}"

    return Response(status_code=200, body=body)


@app.route('/syncinventories/{item_number}',
           methods=['GET'],
           api_key_required=False)
def syncinventories_id(item_number):
    page = 1
    inventory = 0
    need_stop = False
    while not need_stop:
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
                if i['itemNumber'] == item_number:
                    inventory = i['facilityInventory']['inventory']['total']
                    need_stop = True

        if inventory:
            break
        page += 1

    product = get_fulfil_product_api(
        'code', item_number, 'id,quantity_on_hand,quantity_available',
        {"locations": [RUBYHAS_WAREHOUSE, ]})

    if 'quantity_on_hand' not in product:
        send_email(
            f'Unabled to sync inventory for {item_number} at {datetime.today().strftime("%d/%m/%y")}',
            'Server unabled to run query', dev_recipients=True)
    else:
        fulfil_inventory = product['quantity_on_hand']

        # No need to update
        if int(inventory) != int(fulfil_inventory):
            stock_inventory = update_fulfil_inventory_api(product['id'],
                                                          i['rubyhas'])
            if stock_inventory:
                update_stock_api(stock_inventory)


@app.route('/waiting-ruby/re-assign', methods=['GET'])
def invoke_waiting_ruby():
    client = boto3.client('lambda')
    body = None

    response = client.invoke(
        FunctionName=get_lambda_name('reassign_waiting_ruby_prod'),
        InvocationType='Event',
    )

    if response['StatusCode'] == 202:
        body = "The function has been successfully started. You will be notified about the results via email."
    else:
        body = "Something went wrong during the function invokaction. See logs on AWS."

    return Response(status_code=200, body=body)


def reassign_waiting_ruby():
    def update_movement(movement):
        if movement['from_location'] != PRODUCTION and movement[
            'to_location'] != PRODUCTION:
            change_movement_locations(movement_id,
                                      from_location=AURATE_STORAGE_ZONE,
                                      to_location=AURATE_OUTPUT_ZONE)

    shipments = get_waiting_ruby_shipments()
    email_body = []

    if shipments is None:
        email_body.append("Failed to get waiting Ruby shipments. See logs on AWS.")

    elif shipments:
        for shipment in shipments:
            status_code = update_customer_shipment(
                shipment.get('id'), {'warehouse': AURATE_WAREHOUSE})

            if status_code == 200:
                email_body.append(
                    f"[{shipment.get('id')}] CS has been successfully updated!")

                for movement_id in shipment.get('moves'):
                    movement = get_movement(movement_id)

                    if not movement:
                        email_body.append(f"Failed to get [{movement_id}] movement")
                        continue

                    update_movement(movement)

                    for child_id in movement.get('children'):
                        child = get_movement(child_id)

                        if not child:
                            email_body.append(f"Failed to get [{child_id}] movement")
                            continue

                        update_movement(child)

            else:
                email_body.append(
                    f"Something went wrong during CS [{shipment.get('id')}] update. See logs on AWS."
                )
    else:
        email_body.append("No waiting Ruby shipments have been found")

    send_email("Fulfil Report: Re-assign waiting Ruby shipments",
               "<br />".join(email_body))


@app.route('/close-empty-shipments', methods=['GET'])
def close_empty_shipments():
    email_body = []
    count = get_empty_shipments_count()
    empty_shipments = []

    if count is None:
        email_body.append('Faied to retrieve shipments count. See logs on AWS.')

    else:
        chunk_size = 500
        offset = 0
        chunks_count = math.ceil(count / chunk_size)

        print(chunks_count)

        for _ in range(1, chunks_count + 1):
            shipments = get_empty_shipments(offset, chunk_size)

            if shipments is None:
                email_body.append('Failed to retrieve a chunk of shipments. See logs on AWS.')
                continue

            else:
                without_sales = list(filter(lambda x: not x['sales'], shipments))
                empty_shipments += [o['id'] for o in without_sales]

            offset += chunk_size

        if len(empty_shipments):
            empty_shipments_log = [str(item) for item in empty_shipments]
            email_body.append(f'Found {len(empty_shipments)} empty shipments: {", ".join(empty_shipments_log)}')

            for shipment_id in empty_shipments:
                success = cancel_customer_shipment(shipment_id)

                if success:
                    email_body.append(f'[{shipment_id}] Shipment has been successfully canceled')

                else:
                    email_body.append(f'Failed to cancel [{shipment_id}] shipment')

        else:
            email_body.append('Found 0 empty shipments')

    send_email(
        "Fulfil Report: Close empty customer shipments",
        "<br />".join(email_body)
    )


@app.route('/shipped/{ss_number}',
           methods=['GET'],
           api_key_required=False)
def set_shipped(ss_number):
    BUCKET = 'auratebarcodes'
    barcode = get_report_template(11)
    ss = get_supplier_shipment(ss_number)
    update_supplier_shipment(ss_number)
    address = get_contact_from_supplier_shipment(ss)
    ss = get_supplier_shipment(ss_number)
    barcode_data = []
    for po_id in ss['purchases']:
        po = get_po_from_shipment(po_id)
        for line in po['lines']:
            product = get_line_from_po(line)
            barcode_data.append({
                'quantity': int(product['quantity']),
                'code': product['supplier_product_code'],
                'subtext': product['supplier_product_name']
            })

    binary_path = os.path.join(BASE_DIR, 'bin', 'wkhtmltopdf')
    file = create_pdf(barcode_data, barcode['template'], binary_path=binary_path)
    attachment = dict(name='barcode.pdf', data=file, type='application/pdf')
    send_email('Checking barcodes', content="Test", file=attachment)

    s3.put_object(Body=file, Bucket=BUCKET, Key=f'{ss_number}_barcode.pdf')
    file_url = '%s/%s/%s' % (s3.meta.endpoint_url, BUCKET, f'{ss_number}_barcode.pdf')
    return Response(status_code=200, body={'file': file_url})


@app.schedule(Cron(59, 9, '?', '*', '*', '*'))
def split_shipments_job(event):
    split_customer_shipments_api()


@app.route('/split-customer-shipments', methods=['GET'])
def split_customer_shipments_api():
    client = boto3.client('lambda')

    response = client.invoke(
        FunctionName=get_lambda_name('split_customer_shipments'),
        InvocationType='Event',
    )

    if response['StatusCode'] == 202:
        body = "The function has been successfully started. You will be notified about the results via email."
    else:
        body = f"Something went wrong during the function invokaction. See logs on AWS. Response : \n " \
               f"Status : {response['StatusCode']}" \
               f"LogResult : {response['LogResult']}" \
               f"FunctionError : {response['FunctionError']}"

    return Response(status_code=200, body=body)


@app.lambda_function(name='split_customer_shipments')
def split_customer_shipments(event, context):
    client = boto3.client('lambda')
    split_candidates = get_split_candidates()
    if split_candidates:
        client.invoke(
            FunctionName=get_lambda_name('split_customer_shipments_chunk'),
            InvocationType='Event',
            Payload=json.dumps({'shipments': split_candidates,
                                'email_body': []})
        )
        message = (f"Job for {len(split_candidates)} shipments "
                   f"with more than 2 moves planned. ")
        send_email(f"Fulfil Report: Split Customer Shipments (env {env_name})",
                   message)


@app.lambda_function(name='split_customer_shipments_chunk')
def split_customer_shipments_chunk(event, context):
    shipments = event['shipments']
    email_body = event['email_body']
    shipment = shipments.pop()

    split_result = split_shipment(shipment)
    if split_result:
        email_body.append(split_result)

    if len(email_body) == 20 or not shipments:
        email_body.append(f'{len(shipments)} records in process')
        send_email(
            f"Fulfil Report: Split Customer Shipments (env {env_name})",
            "<br />".join(email_body), dev_recipients=True,
        )
        email_body = []
    if shipments:
        boto_client = boto3.client('lambda')
        boto_client.invoke(
            FunctionName=get_lambda_name('split_customer_shipments_chunk'),
            InvocationType='Event',
            Payload=json.dumps({'shipments': shipments,
                                'email_body': email_body})
        )
    return None


@app.schedule(Cron(59, 10, '?', '*', '*', '*'))
def merge_shipments_event(event):
    merge_shipments_api()


@app.route('/merge_shipments', methods=['GET'])
def merge_shipments_api():
    candidates = merge_shipments()

    boto_client = boto3.client('lambda')
    boto_client.invoke(
        FunctionName=get_lambda_name('merge_shipments_chunk'),
        InvocationType='Event',
        Payload=json.dumps({'candidates': candidates,
                            'email_body': []})
    )
    return f"Planned job from {len(candidates)} potential candidates"


@app.lambda_function(name='merge_shipments_chunk')
def merge_shipments_chunk(event, context):
    candidates = event['candidates']
    email_body = event['email_body']

    current = candidates.pop()
    try:
        message = join_shipments(current)
    except Exception as e:
        message = str(e)

    if message:
        email_body.append(message)

    if len(email_body) == 20 or not candidates:
        email_body.append(f'{len(candidates)} records in process')
        send_email(
            f"Fulfil Report: Merge Customer Shipments (env {env_name})",
            "<br />".join(email_body), dev_recipients=True,
        )
        email_body = []

    if candidates:
        boto_client = boto3.client('lambda')
        boto_client.invoke(
            FunctionName=get_lambda_name('merge_shipments_chunk'),
            InvocationType='Event',
            Payload=json.dumps({'candidates': candidates,
                                'email_body': email_body})
        )
    return None


@app.schedule(Cron(0, 12, '?', '*', '*', '*'))
def easypost_in_transit_event(event):
    easypost_in_transit_api()


@app.route('/easypost_in_transit', methods=['GET'])
def easypost_in_transit_api():

    params = easypost.get_transit_shipment_params()
    boto_client = boto3.client('lambda')
    boto_client.invoke(
        FunctionName=get_lambda_name('easypost_in_transit_chuck'),
        InvocationType='Event',
        Payload=json.dumps({'params': params,
                            'shipments': []})
    )
    return f"Planned job to pull late 'in_transit' shipments from easypost. " \
           f"In messages from {params['start_datetime']} " \
           f"to {params['end_datetime']}"


@app.lambda_function(name='easypost_in_transit_chuck')
def easypost_in_transit_chuck(event, context):
    try:
        result = easypost.pull_in_transit_shipments(
            event['params'],
        )
    except Exception as e:
        result = dict(next_page=False, params={},
                      messages=[str(e)])
    shipments = event['shipments'] + result['shipments']

    if result['next_page']:
        boto_client = boto3.client('lambda')
        boto_client.invoke(
            FunctionName=get_lambda_name('easypost_in_transit_chuck'),
            InvocationType='Event',
            Payload=json.dumps({'params': result['params'],
                                'shipments': shipments})
        )
    else:
        send_email(
            f"Easypost Report: Pull late in_transit shipments (env {env_name})",
            str(listDictsToHTMLTable(shipments)), dev_recipients=True,
            email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com',
                   'nancy@auratenewyork.com'],
        )
    return None


@app.schedule(Cron(59, 11, '?', '*', '*', '*'))
def pull_daily_shipments_event(event):
    _datetime = datetime.utcnow()
    result = pull_shipments_by_date(_datetime)
    message = "Shipments by last 24 hours from " + \
              _datetime.isoformat()[:-7] + "<br>"
    message += str(listDictsToHTMLTable(result))
    send_email(
        f"Fulfil Report: Pull daily shipments (env {env_name})",
        message, dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com'],
    )
    return message


@app.route('/pull_daily_shipments', methods=['GET'])
def pull_daily_shipments_api():
    request = app.current_request
    if not request.query_params or "date" not in request.query_params:
        return 'Please, specify dates in query string with "date" as a key. \n' \
               'Example:  /pull_daily_shipments?date=2020-05-27,2020-05-28  '
    dates_str = request.query_params.get('date', None)
    dates = [item.strip() for item in dates_str.split(",") if item]
    result = []
    for d in dates:
        _date = date.fromisoformat(d)
        _time = datetime.max.time()
        _datetime = datetime.combine(_date, _time)
        _datetime += timedelta(hours=-3) # Indent to transform to EST
        res = pull_shipments_by_date(_datetime)
        result.append(f"Shipments by date {d}")
        # result.append(listDictsToHTMLTable(res))
        result += res
        result.append("")
    return "\n".join([str(item) for item in result])


@app.schedule(Cron(59, 3, '?', '*', '*', '1'))
def weekly_pull_shipments_ev(event):
    return weekly_pull_shipments()


@app.route('/weekly_pull_shipments_run', methods=['GET'])
def weekly_pull_shipments_api():
    return weekly_pull_shipments()


def weekly_pull_shipments():
    finished, unfinished, fields, prefix = weekly_pull()

    writer_file = io.StringIO()
    writer = csv.DictWriter(writer_file, fieldnames=fields)
    writer.writeheader()
    writer.writerows(finished)
    attachment_1 = dict(name=f'weekly-done-shipments-{prefix}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    writer_file.close()

    writer_file = io.StringIO()
    writer = csv.DictWriter(writer_file, fieldnames=['delay']+fields)
    writer.writeheader()
    writer.writerows(unfinished)
    attachment_2 = dict(name=f'weekly-delayed-shipments-{prefix}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    writer_file.close()
    send_email(
        f"Fulfil Report: Weekly pull of customer shipments (env {env_name})",
        prefix, dev_recipients=True, file=[attachment_1, attachment_2],
        email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com'],
    )
    return "Done"


@app.schedule(Cron(30, 9, '?', '*', '*', '*'))
def pull_sku_quantities_event(event):
    pull_sku_quantities_api()


@app.route('/pull_sku_quantities', methods=['GET'])
def pull_sku_quantities_api():
    boto_client = boto3.client('lambda')
    boto_client.invoke(
        FunctionName=get_lambda_name('pull_sku_quantities'),
        InvocationType='Event',
        Payload=json.dumps({'offset': 0})
    )
    return f"Pull SKU quantities started."


@app.lambda_function(name='pull_sku_quantities')
def pull_sku_quantities(event, context):
    start_time = datetime.now()
    offset = event['offset']
    first_circle = (offset == 0)
    create_report = False
    data = []
    BUCKET = 'aurate-sku'
    i = 0
    fields = ['id', 'quantity_available', 'quantity_on_hand', 'code',
              'sale_order_count', 'categories', 'quantity_waiting_consumption',
              'rec_name', 'name', 'average_price', 'quantity_outbound',
              'quantity_on_confirmed_purchase_orders', 'quantity_returned',
              'quantity_inbound', 'cost_price', 'average_daily_sales',
              'cost_value', 'customs_value_used', ]
    Model = fulfill_client.model('product.product')
    products = Model.search_read_all(
        domain=['AND', [("active", "=", 'true'), ]],
        order=None,
        fields=fields,
        offset=offset)

    def convert_product(p):
        for key, value in p.items():
            p[key] = json.dumps(value, cls=CustomJsonEncoder)
        return p

    for p in products:
        i += 1
        if p['quantity_available']: # or p['quantity_on_hand']:
            data.append(convert_product(p))
        if datetime.now() - start_time > timedelta(seconds=TIMEOUT - 30) \
                and i and not (i % 500):
            print("Stop moment achieved")
            break
    else:
        create_report = True
        print("Finish iterating in products.")

    if first_circle:
        previous_data = []
    else:
        # read previous result from the S3 bucket.
        response = s3.get_object(Bucket=BUCKET, Key=f'sku_quantities')
        previous_data = pickle.loads(response['Body'].read())

    data = previous_data + data

    if create_report:
        writer_file = io.StringIO()
        writer = csv.DictWriter(writer_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
        attachment = dict(name=f'sku_quantities-{date.today().isoformat()}.csv',
                          data=str.encode(writer_file.getvalue()),
                          type='text/csv')
        send_email(
            f"Fulfil Report: daily pull of SKU quantities (env {env_name})",
            "SKU quantities are in the attached csv file",
            email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com',
                   'operations.aurate+Inventory@emailitin.com'],
            file=attachment,
            dev_recipients=True,
        )
    elif i:
        # put the result to the S3 bucket.
        s3.put_object(Body=pickle.dumps(data), Bucket=BUCKET, Key=f'sku_quantities')
        offset += i
        print(f'Invoke function again with offset {offset}.'
              f' {len(data)} items pulled')

        boto_client = boto3.client('lambda')
        boto_client.invoke(
            FunctionName=get_lambda_name('pull_sku_quantities'),
            InvocationType='Event',
            Payload=json.dumps({'offset': offset})
        )


@app.schedule(Cron(30, 9, '?', '*', '*', '*'))
def rubyhas_quantity_event(event):
    # pull_rubyhas_inventories_event
    pull_rubyhas_inventories_api()


@app.route('/pull_rubyhas_inventories', methods=['GET'])
def pull_rubyhas_inventories_api():
    inventories = get_full_inventory()
    fields = ['№', 'code', 'total_quantity']
    writer_file = io.StringIO()
    writer = csv.writer(writer_file)
    writer.writerow(fields)
    i = 0
    converted_inventories = []
    for key, value in inventories.items():
        if 'rubyhas' in value and value['rubyhas']:
            i += 1
            converted_inventories.append((i, key, value['rubyhas']))
    writer.writerows(converted_inventories)
    attachment = dict(name=f'rubyhas_quantities-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    send_email(
        f"Rubyhas Report: daily pull of quantities (env {env_name})",
        "Rubyhas quantities are in the attached csv file",
        email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com',
               'operations.aurate+Inventory@emailitin.com'],
        file=attachment,
        dev_recipients=True,
    )
    return f"Pull rubyhas quantities finished."


@app.schedule(Cron(0, 10, '?', '*', '*', '*'))
def late_shipments_event(event):
    late_shipments_api()


@app.route('/late_shipments', methods=['GET'])
def late_shipments_api():
    items = get_late_shipments()
    send_email(
        f"Fulfil Report: late customer shipments (env {env_name})",
        str(listDictsToHTMLTable(items)),
        email=['maxwell@auratenewyork.com', 'operations@auratenewyork.com',],
        dev_recipients=True,
    )
    return f"Pull late customer shipments finished"


@app.schedule(Cron(0, 6, '?', '*', '*', '*'))
def items_waiting_allocation(event):
    items_waiting_allocation_api()


@app.route('/items_waiting_allocation', methods=['GET'])
def items_waiting_allocation_api():
    d = date.today() - timedelta(days=1)
    items = get_items_waiting_allocation(d)
    send_email(
        f"Fulfil Report: items_waiting_allocation.ireport {d}",
        str(listDictsToHTMLTable(items)), dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'operations+allocation@emailitin.com'],
    )
    return None


@app.schedule(Cron(0, 5, '?', '*', '*', 0))
def parse_sites_event(event):
    parse_sites()


@app.route('/parse_sites', methods=['GET'])
def parse_sites():
    sites = ['mejuri', 'vrai', 'catbirdnyc', 'thisisthelast']
    for site in sites:
        boto_client = boto3.client('lambda')
        boto_client.invoke(
            FunctionName=get_lambda_name('run_parse_task'),
            InvocationType='Event',
            Payload=json.dumps({'site': site})
        )


@app.lambda_function(name='run_parse_task')
def run_parse_task(event, context):
    scripts = dict(
        mejuri=web_scraper.mejuri,
        vrai=web_scraper.vrai,
        catbirdnyc=web_scraper.catbirdnyc,
        thisisthelast=web_scraper.thisisthelast,
    )
    site = event['site']
    print(site, 'parsing started')
    scripts[site]()
    print(site, 'parsing finished')


@app.route('/scraper_data', methods=['GET'])
def scraper_data():
    def get_data(key):
        response = s3.get_object(Bucket=web_scraper.BUCKET, Key=key)
        return pickle.loads(response['Body'].read())

    mejuri = get_data('mejuri')
    vrai = get_data('vrai')
    catbirdnyc = get_data('catbirdnyc')
    thisisthelast = get_data('thisisthelast')

    result = dict(
        mejuri=mejuri,
        vrai=vrai,
        catbirdnyc=catbirdnyc,
        thisisthelast=thisisthelast,
    )
    for key, value in result.items():
        for item in value:
            item['site'] = key
    return json.dumps(result)

@app.route('/scraper', methods=['GET'])
def scraper_api():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    template_path = os.path.join(dir_path, 'chalicelib', 'template', 'scraper.html')
    with open(template_path, 'r') as f:
        page = f.read()
    page = page.replace('http://127.0.0.1:8000', 'https://4p9vek36rc.execute-api.us-east-2.amazonaws.com/api')
    return Response(body=page,
                    status_code=200,
                    headers={'Content-Type': 'text/html'})