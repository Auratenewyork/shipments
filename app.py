from datetime import date
from datetime import datetime as dt

import boto3
from chalice import Chalice, Cron, Response

from chalicelib import (
    AURATE_OUTPUT_ZONE, AURATE_STORAGE_ZONE, AURATE_WAREHOUSE, PRODUCTION)
from chalicelib.email import send_email
from chalicelib.fulfil import CONFIG as rubyconf
from chalicelib.fulfil import (
    change_movement_locations, create_internal_shipment, find_late_orders,
    get_engraving_order_lines, get_fulfil_product_api, get_global_order_lines,
    get_internal_shipment, get_internal_shipments, get_movement, get_product,
    get_waiting_ruby_shipments, update_customer_shipment,
    update_fulfil_inventory_api, update_internal_shipment, update_stock_api)
from chalicelib.rubyhas import (
    api_call, build_purchase_order, create_purchase_order, get_item_quantity)

app = Chalice(app_name='aurate-webhooks')
app.debug = True


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
               "<br />".join([line for line in email_body]))


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
               "<br />".join([line for line in email_body]))


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
               "<br />".join([line for line in email_body]))

    return Response(status_code=200, body=None)


@app.schedule(Cron(0, 0, 'SUN', '*', '*', '*'))
def syncinventories_all(event):
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

    synced = 0
    not_founded_sku = []
    for _id, i in inventories.items():
        product = get_fulfil_product_api(
            'code', _id, 'id,quantity_on_hand,quantity_available',
            {"locations": [rubyconf['location_ids']['ruby_has_storage_zone'],]})

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
        {"locations": [rubyconf['location_ids']['ruby_has_storage_zone'],]})

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
        FunctionName='reassign_waiting_ruby_prod',
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
               "<br />".join([line for line in email_body]))
