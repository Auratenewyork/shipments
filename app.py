from datetime import date

from chalice import Chalice, Response, Cron

from chalicelib.fulfil import (create_internal_shipment,
                               get_engraving_order_lines, get_internal_shipment,
                               get_internal_shipments, get_movement,
                               get_product, update_internal_shipment)
from chalicelib.rubyhas import (build_purchase_order, create_purchase_order,
                                get_item_quantity)

app = Chalice(app_name='aurate-webhooks')
app.debug = True


@app.schedule(Cron(0, 23, '*', '*', '?', '*'))
def index():
    internal_shipments = get_internal_shipments()
    orders = []
    state = 'assigned'

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
        # TODO: handle errors
        print(response)

    return Response(status_code=200, body=None)


@app.schedule(Cron(0, 18, '*', '*', '?', '*'))
def engravings_orders():
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
            # TODO: send an email
            print(f'Failed to create {reference} IS for engravings')

    if len(products_out_of_stock):
        reference = f'waiting-eng-{current_date}'
        shipment = create_internal_shipment(reference, products_out_of_stock)

        if not shipment:
            # TODO: send an email
            print(f'Failed to create {reference} IS for engravings')

    return Response(status_code=200, body=None)


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

        else:
            print(f'IS with reference "{number}"" doesn\'t exist')

    return Response(status_code=200, body=None)
