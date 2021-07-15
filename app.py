import time
import base64
import csv
import io
import json
import math
import os
import pickle
import sentry_sdk
import traceback
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache

import boto3
from chalice import Chalice, Cron, Response
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from chalicelib import (
    AURATE_OUTPUT_ZONE, AURATE_STORAGE_ZONE, AURATE_WAREHOUSE, PRODUCTION,
    RUBYHAS_WAREHOUSE, easypost, RUBYHAS_HQ_STORAGE, EASYPOST_API_KEY)
from chalicelib import web_scraper, loopreturns
from chalicelib.add_tags_in_comments import add_AOV_tag_to_shipments, \
    add_EXE_tag_to_ship_instructions
from chalicelib.common import listDictsToHTMLTable, CustomJsonEncoder
from chalicelib.count_boxes import process_boxes
from chalicelib.create_fulfil import create_fullfill_order
from chalicelib.delivered_orders import delivered_orders, send_repearment_email
from chalicelib.dynamo_operations import save_easypost_to_dynamo, \
    get_dynamo_last_id, save_shopify_sku, get_shopify_sku_info, \
    get_multiple_sku_info, save_repearment_order, list_repearment_orders, \
    update_repearment_order, get_repearment_order, \
    update_repearment_tracking_number, list_repearment_by_date, \
    update_repearment_order_info
from chalicelib.easypost import get_easypost_record, \
    scrape_easypost__match_reference, get_easypost_record_by_reference, \
    get_shipment, get_easypost_record_by_reference_
from chalicelib.easypsot_tracking import fulfill_tracking, _fulfill_tracking_, \
    get_n_days_old_orders, get_shipments, add_product_info
from chalicelib.email import send_email
from chalicelib.fulfil import (
    change_movement_locations, create_internal_shipment,
    get_internal_shipment, get_internal_shipments, get_movement,
    get_waiting_ruby_shipments, update_customer_shipment,
    update_internal_shipment, get_report_template,
    get_supplier_shipment, update_supplier_shipment,
    get_contact_from_supplier_shipment, create_pdf,
    get_po_from_shipment, get_line_from_po,
    get_empty_shipments_count, get_empty_shipments, cancel_customer_shipment,
    client as fulfill_client, get_late_shipments, get_items_waiting_allocation,
    sale_with_discount, get_product, get_inventory_by_warehouse,
    waiting_allocation, add_exe_comment)
from chalicelib.internal_shipments import ProcessInternalShipment
from chalicelib.late_order import find_late_orders
from chalicelib.repearments import create_repearments_order, \
    get_sales_order_info
from chalicelib.rubyhas import (
    api_call, build_sales_order, create_purchase_order, get_item_quantity,
    get_full_inventory)
from chalicelib.send_sftp import send_loop_report
from chalicelib.shipments import (
    get_split_candidates, split_shipment, join_shipments, merge_shipments,
    pull_shipments_by_date, weekly_pull, customer_shipments_pull)
from chalicelib.shopify import shopify_products, get_shopify_products, \
    filter_shopify_customer, get_customer_orders, extract_variants_from_order
from chalicelib.sync_sku import get_inventory_positions, \
    sku_for_update, dump_inventory_positions, \
    complete_inventory, confirm_inventory, new_inventory, dump_updated_sku
from chalicelib.delivered_orders import return_orders
from chalicelib.utils import capture_to_sentry
from jinja2 import Template
from chalice import CORSConfig
from sentry_sdk import capture_message, configure_scope
from sentry_sdk.integrations.flask import FlaskIntegration
from chalicelib.decorators import try_except


SENTRY_PUB_KEY = os.environ.get('SENTRY_PUB_KEY')
sentry_sdk.init(
    dsn="https://{}@o878889.ingest.sentry.io/5831104".format(SENTRY_PUB_KEY),
    integrations=[FlaskIntegration(), AwsLambdaIntegration()],
    traces_sample_rate=1.0,
    environment=os.environ.get('ENV', 'sandbox')
)


app_name = 'aurate-webhooks'
env_name = os.environ.get('ENV', 'sandbox')
TIMEOUT = int(os.environ.get('TIMEOUT', 500))

app = Chalice(app_name=app_name)
s3 = boto3.client('s3', region_name='us-east-2')
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
app.debug = True


cors_config = CORSConfig(
    allow_origin='*',
    # allow_credentials=True
)


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


def invoke_lambda(name, payload):
    boto_client = boto3.client('lambda')
    boto_client.invoke(
        FunctionName=get_lambda_name(name),
        InvocationType='Event',
        Payload=json.dumps(payload)
    )


@app.schedule(Cron(0, 8, '*', '*', '?', '*'))
def create_pos(event):
    internal_shipments = get_internal_shipments()
    orders = []
    errors = []
    success = []
    email_body = []

    for shipment in internal_shipments:
        products = []
        for movement_id in shipment.get('moves'):
            movement = get_movement(movement_id)
            p = get_product(movement['product'])
            product = {
                'id': movement['product'],
                'sku': p['code'],
                'quantity': int(movement['quantity']),
                'notes': movement['public_notes'],
            }
            # dont know why we are
            quantity = get_item_quantity(product['sku'])
            if quantity is None:
                continue
            if quantity < product['quantity']:
                product['quantity'] = quantity
            products.append(product)

        sales_order = build_sales_order(
            shipment.get('reference'),
            shipment.get('create_date').isoformat(), products)
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


@app.route('/rubyhas', methods=['POST'])
def purchase_order_webhook():
    request = app.current_request
    order = request.json_body[0]
    print(order)

    status_mapping = {
        'receiving': 'waiting',
        'picking': 'assigned',
        'complete': 'done',
        'released': 'done',
        'canceled': 'canceled',
        'archived': 'canceled',
    }

    def process_internal_shipment(order):
        return
        number = order.get('number')
        internal_shipment = get_internal_shipment({'reference': number})
        order_status = order.get('status', '').lower()

        if internal_shipment and order_status in status_mapping.keys():
            update_internal_shipment(internal_shipment.get('id'),
                                     {'state': status_mapping[order_status]})
            # send_email(
            #     "Fulfil Report: Internal shipment status changed",
            #     f"\"{number}\" IS status has been changed to {status_mapping[order_status]}",
            #     email=['roman.borodinov@uadevelopers.com']
            # )
            # if status_mapping[order_status] == 'canceled':
            #     send_email(
            #         "Fulfil Report: Internal shipment canceled",
            #         f"\"{number}\" IS has been canceled"
            #         f"Deposco sales order reference {number}",
            #         dev_recipients=True,
            #         email=['maxwell@auratenewyork.com']
            #     )
        # else:
        #     print("Fulfil Report: Failed to update Internal shipment status",
        #           f"Can't find {number} IS to update the status.")
            # send_email(
            #     "Fulfil Report: Failed to update Internal shipment status",
            #     f"Can't find {number} IS to update the status.",
            #     email=['roman.borodinov@uadevelopers.com']
            # )

    if order.get('type') == 'PURCHASE_ORDER':
        process_internal_shipment(order)

    elif order.get('type') == 'SALES_ORDER':
        number = order.get('number')
        Model = fulfill_client.model('stock.shipment.out')
        if number.startswith("CS"):
            shipment = Model.get(int(number.replace("CS", "", 1)))
            if shipment:
                moves = shipment['moves']
                product_ids = set()
                for movement_id in moves:
                    movement = get_movement(movement_id)
                    for item in movement['item_blurb']['subtitle']:
                        product_ids.add(item[1])
                # syncinventories_ids(product_ids)
        else:
            try:
                process_internal_shipment(order)
            except Exception:
                # send_email(
                #     "Webhook",
                #     f"Error during processing {number} internal shipment ",
                #     email=['roman.borodinov@uadevelopers.com']
                # )
                pass

    return Response(status_code=200, body=None)


@app.route('/syncinventories/{product_ids}',
           methods=['GET'],
           api_key_required=False)
def syncinventories_ids(product_ids):
    inventory = {}
    for item_number in product_ids:
        ruby_quantity = get_item_quantity(item_number)
        if ruby_quantity is not None:
            inventory[item_number] = {'rubyhas': ruby_quantity}
    if inventory:
        updated_sku = []
        for item in sku_for_update(inventory):
            if item:
                updated_sku.append(item)

        if updated_sku:
            count = new_inventory(updated_sku, RUBYHAS_HQ_STORAGE)
            complete_inventory(count)
            confirm_inventory(count)

            send_email(
                f"Sync inventory by webhook",
                str(listDictsToHTMLTable(updated_sku)),
                email=['roman.borodinov@uadevelopers.com'],
            )


@app.schedule(Cron(0, 23, '*', '*', '?', '*'))
def find_late_orders_view(event):
    find_late_orders()

@app.route('/find_late_orders', methods=['GET'])
def find_late_orders_api():
    find_late_orders()

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


@app.schedule(Cron(0, '11-22/5', '?', '*', '*', '*'))
def inventory_by_warehouse(event):
    get_inventory_by_warehouse_api()


@app.route('/inventory_by_warehouse', methods=['GET'])
def get_inventory_by_warehouse_api():
    items, columns = get_inventory_by_warehouse()
    writer_file = io.StringIO()
    writer = csv.writer(writer_file)
    header = [item['display_name'] for item in columns]
    fields = [item['name'] for item in columns]
    writer.writerow(header)
    d = datetime.now().strftime('%Y-%m-%d at %H')
    for item in items:
        row = [item[f] for f in fields]
        writer.writerow(row)
    attachment = dict(name=f'inventory_by_warehouse-{d}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    writer_file.close()

    send_email(
        f"Fulfil Report: inventory.by_warehouse.report {date.today()}",
        'inventory by warehouse report is in the attached csv file',
        dev_recipients=True, file=[attachment, ],
        email=['maxwell@auratenewyork.com', 'aurateinventorydailypull@gmail.com',
               'brian@auratenewyork.com', 'operations.aurate+allInventory@emailitin.com',
               'operations@auratenewyork.com'],
        # email=['roman.borodinov@uadevelopers.com'],
    )
    # send_email(
    #     f"Fulfil Report: inventory.by_warehouse.report {date.today()}",
    #     'inventory by warehouse report is in the attached csv file',
    #     dev_recipients=False, file=[attachment, ],
    #     email=['operations.aurate+allInventory@emailitin.com'],
    # )
    return None


@app.schedule(Cron(0, 5, '?', '*', '*', 0))
def parse_sites_event(event):
    parse_sites()


@app.route('/parse_sites', methods=['GET'])
def parse_sites():
    sites = ['mejuri', 'vrai', 'catbirdnyc', 'thisisthelast', 'stoneandstrand']
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
        stoneandstrand=web_scraper.stoneandstrand,
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
    stoneandstrand = get_data('stoneandstrand')

    result = dict(
        mejuri=mejuri,
        vrai=vrai,
        catbirdnyc=catbirdnyc,
        thisisthelast=thisisthelast,
        stoneandstrand=stoneandstrand,
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


@app.schedule(Cron('0/30', '*', '*', '*', '?', '*'))
def investor_order_event(event):
    sales = sale_with_discount(code='F&FLOVE20', time_delta=timedelta(minutes=30))
    message = ''
    for s in sales:
        message += f"Shopify order: {s['reference']}, Sales order: {s['number']}, <br>"

    if message:
        send_email(
            f"INVESTOR ORDER",
            "",
            email=['operations@auratenewyork.com', 'tai@auratenewyork.com'],
            dev_recipients=True,
        )
    else:
        print("No Sales orders was found with such discount code")


# @app.schedule(Cron(0, 7, '?', '*', '*', '*'))
def sync_inventory_event(event):
    sync_inventory_api()


@app.route('/sync_inventory', methods=['GET'])
def sync_inventory_api():
    inventory = get_full_inventory()
    dump_inventory_positions(inventory)
    invoke_lambda(name='sync_inventory_rubyhas',
                  payload={'updated_sku': []})


@app.lambda_function(name='sync_inventory_rubyhas')
def sync_inventory_rubyhas(event, context):
    updated_sku = event['updated_sku']
    sync_inventory(updated_sku)


@app.route('/sync_inventory_sku', methods=['GET'])
def sync_inventory(updated_sku=[]):
    inventory = get_inventory_positions()
    for item in sku_for_update(inventory):
        if item:
            updated_sku.append(item)

    if inventory:
        dump_inventory_positions(inventory)
        invoke_lambda(name='sync_inventory_rubyhas',
                      payload={'updated_sku': updated_sku})
    else:
        if updated_sku:
            dump_updated_sku(updated_sku)
            try:
                count = new_inventory(updated_sku, RUBYHAS_HQ_STORAGE)
                complete_inventory(count)
                confirm_inventory(count)
            except Exception:
                send_email(
                    f"Fail: Sync inventories {date.today().strftime('%Y-%m-%d')}",
                    'dump of info "ryby_updated_sky"',
                    email=['roman.borodinov@uadevelopers.com'],
                )
            for item in updated_sku:
                item['warehouse'] = RUBYHAS_HQ_STORAGE
                item['inventory'] = count
        send_email(
            f"Sync inventories {date.today().strftime('%Y-%m-%d')}",
            str(listDictsToHTMLTable(updated_sku)),
            email=['maxwell@auratenewyork.com'],
            dev_recipients=True,
        )


# @app.schedule(Cron(0, 2, '?', '*', '*', '*'))
# def internal_shipments_event(event):
#     internal_shipments_api()


@app.route('/internal_shipments', methods=['GET'])
def internal_shipments_api():
    d = datetime.today() - timedelta(days=1)
    p = ProcessInternalShipment(d)
    p.process_internal_shipments()


@app.schedule(Cron(0, 6, '?', '*', '*', '*'))
def count_boxes_event(event):
    count_boxes_api()


@app.route('/count_boxes', methods=['GET'])
def count_boxes_api():
    process_boxes()


@app.schedule(Cron(0, 6, '?', '*', '*', '*'))
def add_exe_comment_event(event):
    add_exe_comment_api()


@app.route('/exe_comment', methods=['GET'])
def add_exe_comment_api():
    add_exe_comment()


@app.schedule(Cron(0, 22, '?', '*', '*', '*'))
def mto_notifications_event(event):
    mto_notifications_api()


def remove_unsubscribed(emails):
    BUCKET = 'aurate-unsubscribe'
    response = s3.get_object(Bucket=BUCKET, Key=f'unsubscribe')
    previous_data = pickle.loads(response['Body'].read())
    clean_emails = []
    for email in emails:
        if email['party.email'] not in previous_data:
            clean_emails.append(email)
    return clean_emails


@app.route('/check_mto_notifications', methods=['GET'])
def mto_notifications_api():
    def get_link(reference):
        return 'http://tracking.auratenewyork.com/' + reference.replace('#', '')

    def get_moves(c):
        moves = []
        for i in c:
            moves.extend(i.get('all_moves', []))
        return moves

    def create_report(_type, sale):
        return {
                  'NotificationType': _type,
                  'Shopify Order': sale['reference'],
                  'Customer Name': sale['party.name'],
                  'Customer Email': sale['party.email'],
                }

    report = []
    emails_3 = get_n_days_old_orders(10)
    template = Template(open(f'{BASE_DIR}/chalicelib/template/mto_3_days.html').read())

    if emails_3 and isinstance(emails_3, list):
        for sale in emails_3:
            report.append(create_report('casting', sale))
            data = {
                'YEAR': str(date.today().year),
                'FINISH_DATE': sale['planned_date'],
                'TRACK_LINK': get_link(sale['reference']),
                'items': get_moves(sale['c']),
            }
            result = template.render(**data)

            send_email( f"An update on your gold",
                        result,
                        email = sale['party.email'],
                        from_email='care@auratenewyork.com',
                      )
            # send_email( f"An update on your gold",
            #             result,
            #             email=['maxwell@auratenewyork.com'],
            #             # email = sale['party.email'],
            #             dev_recipients=True,
            #             from_email='care@auratenewyork.com',
            #           )
            # break

    emails_12 = get_n_days_old_orders(15)
    template = Template(open(f'{BASE_DIR}/chalicelib/template/mto_12_days.html').read())
    if emails_12  and isinstance(emails_12, list):
        for sale in emails_12:
            report.append(create_report('polishing', sale))
            data = {
                'YEAR': str(date.today().year),
                'FINISH_DATE': sale['planned_date'],
                'TRACK_LINK': get_link(sale['reference']),
                'items': get_moves(sale['c']),
            }
            result = template.render(**data)

            send_email( f"Quality Control = Check",
                        result,
                        email = sale['party.email'],
                        from_email='care@auratenewyork.com',
                      )
            # send_email( f"Quality Control = Check",
            #             result,
            #             email=['maxwell@auratenewyork.com'],
            #             # email = sale['party.email'],
            #             dev_recipients=True,
            #             from_email='care@auratenewyork.com',
            #           )
            # break

    emails_17 = get_n_days_old_orders(20, vermeil=True)
    template = Template(open(f'{BASE_DIR}/chalicelib/template/mto_17_days.html').read())
    if emails_17 and isinstance(emails_17, list):
        for sale in emails_17:
            report.append(create_report('vermeil', sale))
            data = {
                'YEAR': str(date.today().year),
                'FINISH_DATE': sale['planned_date'],
                'TRACK_LINK': get_link(sale['reference']),
                'items': get_moves(sale['c']),
            }
            result = template.render(**data)
            send_email( f"Next steps for your gold vermeil",
                        result,
                        email = sale['party.email'],
                        from_email='care@auratenewyork.com',
                      )
            # send_email( f"Next steps for your gold vermeil",
            #             result,
            #             email=['maxwell@auratenewyork.com'],
            #             # email = sale['party.email'],
            #             dev_recipients=True,
            #             from_email='care@auratenewyork.com',
            #           )
            # break
            # return Response(result, status_code=200, headers={'Content-Type': 'text/html'})

    send_email( f"Fullfill MTO notifications:",
                str(listDictsToHTMLTable(report)),
                email=['maxwell@auratenewyork.com', 'jenny@auratenewyork.com'],
                dev_recipients=True,
              )


@app.route('/api/unsubscribe', methods=['GET'])
def unsubscribe_api():
    request = app.current_request
    if request.query_params and 'email' in request.query_params:
        email = request.query_params.get('email', None)
        BUCKET = 'aurate-unsubscribe'
        response = s3.get_object(Bucket=BUCKET, Key=f'unsubscribe')
        previous_data = pickle.loads(response['Body'].read())
        previous_data.append(email)
        s3.put_object(Body=pickle.dumps(previous_data), Bucket=BUCKET,
                      Key=f'unsubscribe')
        with open(f'{BASE_DIR}/chalicelib/template/unsubscribe.html', 'r') as f:
            template = f.read()
    return Response(status_code=200, body=template)


@app.route('/tracking_information_/{sale_reference}',
           methods=['GET'])
def tracking_information(sale_reference):
    request = app.current_request
    test = False
    if request.query_params and 'test' in request.query_params:
        test = True

    try:
        int(sale_reference)
        sale_reference = "#" + sale_reference
    except ValueError:
        pass
    shipments, lines, sale_number = get_shipments(sale_reference)
    tracking = []
    shipments = add_product_info(shipments, lines)

    e_shipment_ids = get_easypost_record_by_reference_(sale_reference,
                                                       sale_number)
    e_shipments = []
    if e_shipment_ids:
        if not isinstance(e_shipment_ids, list):
            e_shipment_ids = [e_shipment_ids]
        for e_shipment_id in e_shipment_ids:
            if e_shipment_id:
                e_shipment = get_shipment(e_shipment_id)
                e_shipments.append(e_shipment)

    match_tracking = []
    for item in shipments:
        link, status, carrier, tracking_title = None, None, None, None
        tracking_blurb = item['tracking_number_blurb']
        if tracking_blurb:
            if tracking_blurb.get('title', 'N/A') != 'N/A':
                tracking_title = tracking_blurb['title']
            for blurb in tracking_blurb.get('subtitle', []):
                if blurb[0] == 'link':
                    link = blurb[-1]
                if blurb[0] == 'Status':
                    status = blurb[-1]
                if blurb[0] == 'Carrier':
                    if 'USPS' in blurb[-1]:
                        carrier = 'USPS'
                    if 'FedEx' in blurb[-1]:
                        carrier = 'FedEx'
            item['n'] = dict(
                carrier=carrier,
                link=link,
                status=status,
                tracking_title=tracking_title,
            )
        for e_ship in e_shipments:
            if e_ship.tracking_code == tracking_title:
                match_tracking.append([item, e_ship.tracker.tracking_details, None])
                break
        else:
            match_tracking.append([item, [], link])
            # if tracking_title and carrier == 'FedEx':
            #     match_tracking.append([item, [], link])
            # else:
            #     match_tracking.append([item, [], None])

    for shipment, e_tracking, link in match_tracking:
        if test:
            f_tracking, estimated_date, shipment_number = _fulfill_tracking_(shipment)
        else:
            f_tracking, estimated_date, shipment_number = fulfill_tracking(shipment)
        track = []
        for item in f_tracking:
            item.update(dict(
                time='',
                city='NEW YORK',
                country='US',
                state='NY',
                zip='10036',
                source='AURATE',
            ))
            track.append(item)

        for item in e_tracking:
            d = datetime.fromisoformat(item.datetime[0:-1])
            a = dict(
                message=item.message,
                city=item.tracking_location.city,
                country=item.tracking_location.country,
                state=item.tracking_location.state,
                zip=item.tracking_location.zip,
                date=d.strftime('%m/%d/%Y'),
                time=d.strftime("%I:%M %p").lower(),
                source=item.source,
            )
            track.append(a)
        if link:
            a = dict(
                link=link,
                message='Tracking link',
                source='AURATE',
            )
            track.append(a)

        track.reverse()
        # Add here estimated date and other info according to the stock.shipment.out...
        data = {'estimated_date': estimated_date,
                'items': track,
                }
        p = []
        show_metadata = True
        if (shipment['shipping_instructions'] != None and
             'Planned date delayed' in shipment['shipping_instructions']):
            show_metadata = False

        for s in shipment['all_moves']:
            i = {
                'name': s['name'],
                'image': s['image'],
                'quantity': s['quantity'],
                'title': s['title'],
                'metadata': s.get('metadata', []) if show_metadata else [],
            }
            p.append(i)
        data['products'] = p

        tracking.append(data)
    return Response(status_code=200, headers={'Access-Control-Allow-Origin': '*'},
                    body=json.dumps(tracking))


@app.schedule(Cron(0, '11-22/5', '?', '*', '*', '*'))
def scrape_easypost_event(event):
    scrape_easypost_api()


@app.route('/scrape_easypost', methods=['GET'])
def scrape_easypost_api():
    # BUCKET = 'aurate-sku'
    # response = s3.get_object(Bucket=BUCKET, Key=f'easypost_reference_match')
    # previous_data = pickle.loads(response['Body'].read())

    last_id = get_dynamo_last_id()
    info = scrape_easypost__match_reference(last_id)
    save_easypost_to_dynamo(info)

    # previous_data['shipments'].update(info['shipments'])
    # previous_data['last_id'] = info['last_id']
    # s3.put_object(Body=pickle.dumps(previous_data), Bucket=BUCKET,
    #               Key=f'easypost_reference_match')


@app.route('/loopreturns', methods=['POST'])
def loopreturns_api():
    request = app.current_request
    body = request.json_body
    # trigger = body['trigger']
    # BUCKET = 'aurate-loopreturns'
    # key = f'{trigger}-{date.today().strftime("%Y-%m-%d")}'
    # s3.put_object(Body=json.dumps(body), Bucket=BUCKET, Key=key)


    try:
        result = loopreturns.process_request(request)
    except Exception as err:
        traceback.print_exc()
        result = "some error occurred, check logs please"

    # send_email(subject="loopreturns: webhook", content=str(result),
    #            dev_recipients=True)
    return Response(status_code=200, body=None)


@app.schedule(Cron(0, 12, '?', '*', 'FRI', '*'))
def delivered_orders_event(event):
    delivered_orders_api()


@app.route('/delivered_orders', methods=['GET'])
def delivered_orders_api():
    orders, message = delivered_orders()

    writer_file = io.StringIO()
    writer = csv.writer(writer_file)

    header = ['Email', 'Name', 'Reference']
    writer.writerow(header)
    for item in orders:
        row = [item['party.email'], item['party.name'], item['reference']]
        writer.writerow(row)
    attachment = dict(name=f'delivered_orders-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    send_email(
        f"Fullfill Delivered Orders:",
        message, dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'nancy@auratenewyork.com'],
        file=attachment
    )


@app.schedule(Cron(0, 5, '?', '*', 'MON', '*'))
def return_orders_event(event):
    return_orders_api()


@app.route('/return_orders', methods=['GET'])
def return_orders_api():
    orders, message = return_orders()

    writer_file = io.StringIO()
    writer = csv.writer(writer_file)

    header = ['Loop id', 'Email', 'Order ID',  'Reference', 'Order Number', 'status', 'Created At', "return Total", "RETURNS (sku)","RETURNS (variantID)"]
    writer.writerow(header)

    # exchanges = []
    # for item in orders:
    #     if 'exe' in item['order_name']:
    #         exchanges.append(item['order_name'])
    #         exchanges.append('#' + item['order_name'][4:-2])

    for item in orders:
        if item['exchanges']: # or item['order_name'] in exchanges:
            continue
        for ret in item['line_items']:
            # row = [item['customer'], item['order_name'], item['created_at'], item[']:return_total'], item['exchange_total'], ret['sku'], ret['variant_id']]
            row = [item['id'], item['customer'], item['order_id'], item['order_name'], item['order_number'], item['state'], item['created_at'], item['return_total'], ret['sku'], ret['variant_id']]
            writer.writerow(row)

    send_loop_report(writer_file)
    attachment = dict(name=f'returned_orders-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')

    send_email(
        f"Loopreturns returned orders:",
        message, dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'nancy@auratenewyork.com'],
        # email=['roman.borodinov@uadevelopers.com'],
        file=attachment
    )
    writer_file.close()


# @app.schedule(Cron(0, 5, '?', '*', '*', '*'))
# def lost_shopify_event(event):
#     lost_shopify_api()


@app.route('/lost_shopify', methods=['GET'])
def lost_shopify_api():
    products = shopify_products()
    writer_file = io.StringIO()
    writer = csv.DictWriter(writer_file, products[0].keys())
    writer.writeheader()
    writer.writerows(products)

    attachment = dict(name=f'lost-shopify-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    send_email(
        "Shopify quantity compare:",
        "data in attached file", dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'brian@auratenewyork.com'],
        # email=['roman.borodinov@uadevelopers.com'],
        file=attachment
    )


@app.schedule(Cron(0, 6, '?', '*', '*', '*'))
def waiting_allocation_event(event):
    waiting_allocation_api()


@app.route('/waiting_allocation', methods=['GET'])
def waiting_allocation_api():
    products = waiting_allocation()
    writer_file = io.StringIO()
    writer = csv.DictWriter(writer_file, products[0].keys())
    writer.writeheader()
    writer.writerows(products)

    attachment = dict(name=f'waiting_allocation-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    send_email(
        "Items waiting allocation:",
        "data in attached file", dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'brian@auratenewyork.com'],
        # email=['roman.borodinov@uadevelopers.com'],
        file=attachment
    )


@app.route('/email_tests', methods=['GET'])
def email_tests_api():
    from chalicelib.email_tests import run_email_tests
    request = app.current_request

    email = 'maxwell@auratenewyork.com'
    order_number = None
    if request.query_params:
        email = request.query_params.get('email', 'maxwell@auratenewyork.com')
        order_number = request.query_params.get('order_number', None)

    run_email_tests(email, order_number)


@app.schedule(Cron(0, 9, '?', '*', '*', '*'))
def c_s_pull_event(event):
    customer_shipments_pull_api()


@app.route('/customer_shipments_pull', methods=['GET'])
def customer_shipments_pull_api():
    products = customer_shipments_pull()
    writer_file = io.StringIO()
    writer = csv.DictWriter(writer_file, products[0].keys())
    writer.writeheader()
    writer.writerows(products)

    attachment = dict(name=f'customer_shipments_pull-{date.today().isoformat()}.csv',
                      data=str.encode(writer_file.getvalue()),
                      type='text/csv')
    send_email(
        "Customer Shipments Pull:",
        "data in attached file", dev_recipients=True,
        email=['maxwell@auratenewyork.com', 'brian@auratenewyork.com'],
        # email=['roman.borodinov@uadevelopers.com'],
        file=attachment
    )

@app.route('/sync_shopify', methods=['GET'])
def sync_shopify():
    products = get_shopify_products()
    save_shopify_sku(products)

# @app.route('/sync_shopify_fix', methods=['GET'])
# def sync_shopify():
#     from chalicelib.dynamo_operations import scan_without_images, update_shopify_image
#     without_images = scan_without_images()
#     products = get_shopify_products()
#     # with open('products', 'rb') as pickle_file:
#     #     products = pickle.load(pickle_file)
#
#     for item in without_images:
#         for product in products:
#             if item['product_id'] == product['id']:
#                 image = product.get('image', {})
#                 if image:
#                     update_shopify_image(item['PK'], image.get('src', ''))


@app.route('/customer_orders', methods=['POST'], cors=cors_config)
def customer_orders_api():
    request = app.current_request
    email = request.json_body['email']
    customer = filter_shopify_customer(email=email)
    orders = get_customer_orders(customer['id'], status='closed')
    shopify_variants = []
    for order in orders:
        shopify_variants.extend(extract_variants_from_order(order))
    variants = get_multiple_sku_info(sku_list=[v['sku'] for v in shopify_variants])

    for one_variant in shopify_variants:
        for v in variants:
            if v['PK'] == one_variant['sku']:
                one_variant.update(v)
                break
    address = customer['default_address']
    address.pop('id')
    address.pop('customer_id')
    return {'orders': shopify_variants, 'address': customer['default_address'],
            'customer_id': customer['id'], 'email':email}


def save_repearment_files(_date, files):
    BUCKET = 'aurate-repearment-images'
    added_files = []
    delta = 0
    for f in files:
        image = base64.b64decode(f['file'].split(',')[1])
        extension = f['filename'].split('.')[-1]
        filename = f"{str(int(_date)+delta)}.{extension}"
        s3.put_object(Body=image, Bucket=BUCKET, Key=filename)
        file_path = f"https://{BUCKET}.s3.us-east-2.amazonaws.com/{filename}"
        delta += 1
        added_files.append({'filename': f['filename'], 'image_url': file_path})
    return added_files


@app.route('/repairmen_request', methods=['POST'], cors=cors_config)
def repairmen_request_api():
    request = app.current_request
    body = request.json_body
    _date = int(time.time())
    files = body.get('files', [])
    added_files = save_repearment_files(_date, files)
    order = body['order']
    if body.get('added_files', None):
        added_files.extend(body['added_files'])
    info = dict(
        DT=_date,
        order_id=order['order_id'],
        order_name=order['order_name'],
        sku=order['sku'],
        variant_id=order['variant_id'],
        product_id=order['product_id'],
        sku_name=order['name'],
        description=body.get('message', ''),
        files=added_files,
        email=body.get('email', ''),
        mailingAddress=body.get('mailingAddress', ''),
        service=body.get('service', ''),
        address=body.get('address', None),
        customer_id=body.get('customer_id', None),
        approve='pending'
        # image_url=file_path
    )
    info = save_repearment_order(info)

    email = body.get('email')
    send_repearment_email(email, 'confirmed')
    return info


@app.route('/repairmen_request', methods=['PUT'], cors=cors_config)
def repairmen_request_api():
    request = app.current_request
    body = request.json_body
    _date = body.get('DT', None) or int(time.time())
    files = [body['file']]
    added_files = save_repearment_files(_date, files)
    return added_files[0]


@app.route('/repairments', methods=['GET'], cors=cors_config)
def repairmen_list_api():
    # https://4p9vek36rc.execute-api.us-east-2.amazonaws.com/api/repairments?search=2345454&filter=Pending
    request = app.current_request
    last_id, search, filter_ = None, None, None
    if request.query_params:
        last_id = request.query_params.get('last_id', None)
        search = request.query_params.get('search', None)
        filter_ = request.query_params.get('filter', None)
        if filter_ == 'Denied':
            filter_ = 'declined'
    else:
        filter_ = 'pending'
    items, last_key = list_repearment_orders(last_id, search, filter_)
    return {'items': items, 'last_id': last_key.get('DT', None)}


@app.route('/repairments', methods=['POST'], cors=cors_config)
def repairmen_update_api():
    request = app.current_request
    body = request.json_body
    order = body['order']
    update_repearment_order(order['DT'], order['approve'], order['note'])

    if not body.get('email'):
        body['email'] = 'maxwell@auratenewyork.com'
    if order['approve'] == 'accepted':
        send_repearment_email(body.get('email'), 'accepted', DT=order['DT'])
    elif order['approve'] == 'declined':
        send_repearment_email(body.get('email'), 'declined',  NOTE=order['note'])
    return order


def send_exception():
    fp = io.StringIO()
    traceback.print_exc(file=fp)
    message = fp.getvalue()
    send_email('Repearment exception!!!!!',
               message,
               email='roman.borodinov@uadevelopers.com',
               dev_recipients=True,
               )

@app.route('/repairmen_tracking', methods=['POST'], cors=cors_config)
def repairmen_list_api():
    request = app.current_request
    body = request.json_body
    update_repearment_tracking_number(int(body['DT']), body['tracking_number'])

    item = get_repearment_order(body['DT'])
    if 'repearment_id' not in item:
        try:
            order = create_repearments_order(item)
        except Exception as e:
            order = None
            send_exception()

        if order:
            try:
                update_repearment_order_info(int(body['DT']), order)
            except Exception as e:
                send_exception()

        try:
            create_fullfill_order(item)
        except Exception as e:
            send_exception()

        send_email("Repearment: added tracking number",
                   f"Current info {body['tracking_number']}, {body['DT']}",
                   email='maxwell@auratenewyork.com',
                   dev_recipients=True,)
    return body


@app.schedule(Cron(0, 12, '?', '*', '*', '*'))
def repearment_reminder_event(event):
    repearment_reminder_api()


@app.route('/repearment_reminder', methods=['GET'])
def repearment_reminder_api():
    end = datetime.utcnow() - timedelta(days=2)
    start = end - timedelta(days=1)
    items, _ = list_repearment_by_date(datetime.timestamp(start), datetime.timestamp(end))
    for item in items:
        if not item.get('tracking_number', None):
            send_repearment_email(item.get('email'), 'reminder', DT=item['DT'])


@app.route('/repearment_report', methods=['GET'])
def repearment_report_api():
    request = app.current_request
    last_id = None
    if request.query_params:
        last_id = request.query_params.get('last_id', None)
        approve = request.query_params.get('filter', 'pending')
    items, last_key = list_repearment_orders(last_id, approve=approve)
    for item in items:
        if item['order_id']:
            order_info = get_sales_order_info(item['order_id'])
        if order_info:
            item['order_info'] = order_info
            break
    return items


@app.schedule(Cron(0, 12, '?', '*', '*', '*'))
def add_AOV_tag_event(event):
    add_AOV_tag_to_shipments_api()


@app.route('/aov_tag', methods=['GET'])
def add_AOV_tag_to_shipments_api():
    add_AOV_tag_to_shipments()
    # add_EXE_tag_to_ship_instructions()


@app.route('/debug-sentry', methods=['GET'])
def trigger_api():
    trigger_error()


@try_except(test_tag='debug-sentry')  # transaction='debug-sentry',
def trigger_error():
    division_by_zero = 1 / 0
    return 1


@app.schedule(Cron(0, 12, '?', '*', '*', '*'))
@try_except()
def test_cron_with_error(event):
    raise Exception('POOR WORLD11')


@app.route('/test_sentry', methods=['GET'])
def r_error():
    division_by_zero = 1 / 0
    return 1

@app.route('/ourplace', methods=['POST'])
def ger_error():
    return 1


@app.route('/tmall-hook', methods=['GET', 'POST', 'PUT'])
def tmall_api():
    request = app.current_request
    data = {'request.raw_body': request.raw_body}
    capture_to_sentry('Tmall request!', data, method=request.method)
