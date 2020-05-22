import json
import math
import os
from datetime import date
from datetime import datetime as dt
from functools import lru_cache

import boto3
from chalice import Chalice, Cron, Response

from chalicelib import (
    AURATE_OUTPUT_ZONE, AURATE_STORAGE_ZONE, AURATE_WAREHOUSE, PRODUCTION, RUBYHAS_WAREHOUSE)
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
    get_waiting_customer_shipments, check_in_stock, client, join_shipments,
    merge_shipments)

from chalicelib.rubyhas import (
    api_call, build_purchase_order, create_purchase_order, get_item_quantity)

app_name = 'aurate-webhooks'
env_name = os.environ.get('ENV', 'sandbox')

app = Chalice(app_name=app_name)
s3 = boto3.client('s3', region_name='us-east-2')
BUCKET = 'auratebarcodes'
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


@app.schedule(Cron(0, 21, '*', '*', '?', '*'))
def create_pos(event):
    internal_shipments = get_internal_shipments()
    orders = []
    state = 'assigned'
    errors = []
    success = []
    email_body = []

    for shipment in internal_shipments:
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

        purchase_order = build_purchase_order(
            shipment.get('reference'),
            shipment.get('create_date').get('iso_string'), products)

        orders.append(purchase_order)

    for order in orders:
        response = create_purchase_order(order)

        if response.status_code > 207:
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
               "<br />".join(email_body))


@app.schedule(Cron(0, 18, '*', '*', '?', '*'))
def engravings_orders(event):
    engravings = get_engraving_order_lines()
    products_in_stock = []
    products_out_of_stock = []
    current_date = date.today().isoformat()
    email_body = []

    for engraving in engravings:
        product = get_product(engraving)

        if not product:
            email_body.append(
                f"Failed to get the product [{engraving.get('product')}]")
            continue

        quantity = get_item_quantity(product['sku'])

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
               "<br />".join(email_body))


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
               "<br />".join(email_body))

    return Response(status_code=200, body=None)


@app.lambda_function(name='get_full_inventory_rubyhas')
def get_full_inventory_rubyhas(event, context):
    def chunks(dictionary, size):
        items = dictionary.items()
        return (dict(items[i:i + size]) for i in range(0, len(items), size))

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

                if i['itemNumber'] in inventories:
                    inventories[i['itemNumber']]['rubyhas'] = int(
                        i['facilityInventory']['inventory']['total'])
                else:
                    inventories[i['itemNumber']] = {'rubyhas': 0}
                    inventories[i['itemNumber']]['rubyhas'] += int(
                        i['facilityInventory']['inventory']['total'])

            page += 1

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
        if i['rubyhas'] == fulfil_inventory:
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
        f'Results for syncing inventories at {dt.today().strftime("%d/%m/%y")}',
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
                if i['itemNumber'] == item_number:
                    inventory = i['facilityInventory']['inventory']['total']

        if inventory:
            break

    product = get_fulfil_product_api(
        'code', item_number, 'id,quantity_on_hand,quantity_available',
        {"locations": [RUBYHAS_WAREHOUSE, ]})

    if 'quantity_on_hand' not in product:
        send_email(
            f'Unabled to sync inventory for {item_number} at {dt.today().strftime("%d/%m/%y")}',
            'Server unabled to run query')

    fulfil_inventory = product['quantity_on_hand']

    # No need to update
    if inventory != fulfil_inventory:
        stock_inventory = update_fulfil_inventory_api(product['id'],
                                                      i['rubyhas'])
        if stock_inventory:
            update_stock_api(stock_inventory)
    else:
        send_email(
            f'No need to sync for {item_number} at {dt.today().strftime("%d/%m/%y")}',
            f'Stocks are match ( fulfil - {fulfil_inventory} | rubyhas - {inventory}'
        )


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

    send_email('Checking barcodes', content="Test", attachment=file, email='srglvk3@gmail.com')
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

    chunk_size = 10
    offset = 0
    email_body = []

    shipments = get_waiting_customer_shipments(offset, chunk_size)

    if shipments is None:
        email_body.append("Something went wrong while retrieving customer shipments. See logs on AWS.")

    elif not shipments:
        email_body.append("Found 0 waiting internal shipments")

    else:
        # if shipments count equals chunk_size then try to get more shipments
        if len(shipments) == chunk_size:
            while len(shipments) == chunk_size + offset:
                offset += chunk_size
                shipments += get_waiting_customer_shipments(offset, chunk_size)

        split_candidates = []
        for shipment in shipments:
            if len(shipment.get('moves')) > 2:
                split_candidates.append(shipment)

        client.invoke(
            FunctionName=get_lambda_name('split_customer_shipments_chunk'),
            InvocationType='Event',
            Payload=json.dumps({'shipments': split_candidates,
                                'email_body': []})
        )
        email_body.append(f"Job for {len(split_candidates)} shipments "
                          f"with more than 2 moves planned. ")

    send_email(
        f"Fulfil Report: Split Customer Shipments (env {env_name})",
        "<br />".join(email_body)
    )


@app.lambda_function(name='split_customer_shipments_chunk')
def split_customer_shipments_chunk(event, context):

    shipments = event['shipments']
    email_body = event['email_body']

    Shipment = client.model('stock.shipment.out')
    shipment = shipments.pop()
    instance = Shipment.get(shipment['id'])

    products_in_stock = []
    skip_product = False
    for movement_id in shipment.get('moves'):
        movement = get_movement(movement_id)

        if not movement:
            email_body.append(f"Failed to get [{movement_id}] movement. "
                              f"For product {instance['number']}")
            skip_product = True
            break

        product_id = movement.get('product')
        quantity = movement.get('quantity')

        in_stock = check_in_stock(product_id, AURATE_STORAGE_ZONE, quantity)

        if in_stock:
            products_in_stock.append({
                'id': product_id,
                'quantity': quantity,
                'movement': movement_id,
            })

    if not skip_product and \
            len(products_in_stock) and \
            len(products_in_stock) != len(shipment.get('moves')):
        move_quantity_to_split = [
            {"id": item['movement'],
             "quantity": int(item['quantity'])}
            for item in products_in_stock
        ]

        planned_date = instance['planned_date']
        shipment_id = instance['id']
        new_shipment_id = Shipment.split(shipment_id, move_quantity_to_split, planned_date)
        email_body.append(f'Modify shipment № {shipment_id}. '
                          f'Created new shipment № {new_shipment_id}. '
                          f'Movements moved to the new shipment: {move_quantity_to_split}')
    else:
        email_body.append(
            f"Shipment № {instance['id']} don't need split.")
    if len(email_body) == 20 or not shipments:
        email_body.append(f'{len(shipments)} records in process')
        send_email(
            f"Fulfil Report: Split Customer Shipments (env {env_name})",
            "<br />".join(email_body)
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
        message = str(Exception)

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
