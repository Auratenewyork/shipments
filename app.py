from datetime import date

from chalice import Chalice, Response, Cron

from chalicelib.fulfil import (create_internal_shipment,
                               get_engraving_order_lines, get_internal_shipment,
                               get_internal_shipments, get_movement,
                               get_product, update_internal_shipment)
from chalicelib.rubyhas import (build_purchase_order, create_purchase_order,
                                get_item_quantity)
from chalicelib.email import send_email

app = Chalice(app_name='aurate-webhooks')
app.debug = True


@app.schedule(Cron(0, 23, '*', '*', '?', '*'))
def index(event):
    internal_shipments = get_internal_shipments()
    orders = []
    state = 'assigned'
    errors = []
    success = []

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
        send_email("Ruby Has: Failed to create purchase orders",
                   f"Failed to create purchase orders: {references}")

    if len(success):
        references = ", ".join([o[0]['number'] for o in success])
        send_email(f"Ruby Has: {len(success)} purchase orders created",
                   f"Successfully created purchase orders: {references}")


@app.schedule(Cron(0, 18, '*', '*', '?', '*'))
def engravings_orders(event):
    engravings = get_engraving_order_lines()
    products_in_stock = []
    products_out_of_stock = []
    current_date = date.today().isoformat()

    for engraving in engravings:
        product = get_product(engraving)
        quantity = get_item_quantity(product['sku'])

        if quantity >= product.get('quantity'):
            products_in_stock.append(product)
        else:
            products_out_of_stock.append(product)

    if len(products_in_stock):
        reference = f'eng-{current_date}'
        shipment = create_internal_shipment(reference,
                                            products_in_stock,
                                            state='assigned')

        if not shipment:
            send_email(
                "Fulfil: failed to create an IS for engravings",
                f"Failed to create {reference} IS for engravings for {current_date}"
            )

    if len(products_out_of_stock):
        reference = f'waiting-eng-{current_date}'
        shipment = create_internal_shipment(reference, products_out_of_stock)

        if not shipment:
            send_email(
                "Fulfil: failed to create an IS for engravings",
                f"Failed to create {reference} IS for engravings for {current_date}"
            )
        else:
            send_email(
                "Fulfil: IS for engravings have been successfully created",
                f"Successfully created {reference} IS for engravings for {current_date}"
            )

    if not len(products_in_stock) and not len(products_out_of_stock):
        send_email(f"Fulfil: no engravings orders found today",
                   f"No engravings orders found today")


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
                f"Fulfil: {number} IS status changed",
                f"{number} IS status was changed to {status_mapping[order_status]}"
            )

        else:
            send_email(f"Fulfil: can't update {number} IS status",
                       f"Can't find {number} IS to update the status.")

    return Response(status_code=200, body=None)
