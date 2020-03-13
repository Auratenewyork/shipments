from datetime import date

from chalice import Chalice, Response, Cron

from chalicelib.fulfil import (create_internal_shipment,
                               get_engraving_order_lines, get_internal_shipment,
                               get_internal_shipments, get_movement,
                               get_product, update_internal_shipment,
                               find_late_orders, get_global_order_lines)
from chalicelib.rubyhas import (build_purchase_order, create_purchase_order,
                                get_item_quantity)
from chalicelib.email import send_email

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
            quantity = get_item_quantity(product['sku'])

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
               "\n".join([line for line in email_body]))


@app.schedule(Cron(0, 18, '*', '*', '?', '*'))
def engravings_orders(event):
    engravings = get_engraving_order_lines()
    products_in_stock = []
    products_out_of_stock = []
    current_date = date.today().isoformat()
    email_body = []

    for engraving in engravings:
        product = get_product(engraving)
        quantity = get_item_quantity(product['sku'])

        if quantity > 0:
            if quantity >= product['quantity']:
                products_in_stock.append(product)
            else:
                # split product quantity into two internal shipments
                # one for product quantity which is in the stock
                # another for product quantity which is out of stock
                quantity_out_of_stock = quantity - product['quantity']
                product_in_stock = {**product, 'quantity': quantity}
                product_out_of_stock = {
                    **product, 'quantity': quantity_out_of_stock
                }
                products_in_stock.append(product_in_stock)
                products_out_of_stock.append(product_out_of_stock)

    if len(products_in_stock):
        reference = f'eng-{current_date}'
        shipment = create_internal_shipment(reference,
                                            products_in_stock,
                                            state='assigned')

        if not shipment:
            email_body.append(
                f"Failed to create \"{reference}\" IS for engravings")

    if len(products_out_of_stock):
        reference = f'waiting-eng-{current_date}'
        shipment = create_internal_shipment(reference, products_out_of_stock)

        if not shipment:
            email_body.append(
                f"Failed to create \"{reference}\" IS for engravings")
        else:
            email_body.append(
                f"Successfully created \"{reference}\" IS for engravings")

    if not len(products_in_stock) and not len(products_out_of_stock):
        email_body.append(f"No engravings orders found today")

    send_email("Fulfil Report: Internal shipments",
               "\n".join([line for line in email_body]))


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
            quantity = get_item_quantity(product['sku'])

            if quantity > 0:
                if quantity >= product['quantity']:
                    products_in_stock.append(product)
                else:
                    # split product quantity into two internal shipments
                    # one for product quantity which is in the stock
                    # another for product quantity which is out of stock
                    quantity_out_of_stock = quantity - product['quantity']
                    product_in_stock = {**product, 'quantity': quantity}
                    product_out_of_stock = {
                        **product, 'quantity': quantity_out_of_stock
                    }
                    products_in_stock.append(product_in_stock)
                    products_out_of_stock.append(product_out_of_stock)

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
                                                state='assigned')

            if not shipment:
                email_body.append(
                    "Failed to create IS for global orders in the stock")
            else:
                email_body.append(
                    "Successfully created IS for global orders out of stock")

    elif order_lines is not None:
        email_body.append("Found 0 global orders")
    else:
        email_body.append("Failed to get global orders. See logs on AWS.")

    send_email(f"Fulfil Report: Global orders",
               "\n".join([line for line in email_body]))

    return Response(status_code=200, body=None)