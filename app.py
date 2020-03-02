from datetime import date

from flask import Flask, Response

from chalicelib.fulfil import (
    create_internal_shipment, get_engraving_order_lines, get_internal_shipments,
    get_movement, get_product)
from chalicelib.rubyhas import (
    build_purchase_order, create_purchase_order, get_item_quantity)

# app = Chalice(app_name='aurate-webhooks')
app = Flask(__name__)
app.debug = True


@app.route('/', methods=['GET'])
def index():
    internal_shipments = get_internal_shipments()
    products = []
    orders = []

    for shipment in internal_shipments:
        for movement_id in shipment.get('moves'):
            movement = get_movement(movement_id)
            product = get_product(movement)
            products.append(product)

        purchase_order = build_purchase_order(
            shipment.get('reference'),
            shipment.get('create_date').get('iso_string'), products)
        orders.append(purchase_order)

    for order in orders:
        response = create_purchase_order(order)
        # TODO: handle errors
        print(response)

    return Response(status_code=200, body=None)


@app.route('/engravings', methods=['GET'])
def engravings_orders():
    engravings = get_engraving_order_lines()
    products = []

    for engraving in engravings:
        product = get_product(engraving)
        quantity = get_item_quantity(product['sku'])

        # add product only if the desired quantity is in a stock
        if quantity >= product.get('quantity'):
            products.append(product)

        else:
            print('{sku}: out of stock'.format(sku=product.get('sku')))

    if len(products):
        shipment = create_internal_shipment(products)

        if not shipment:
            # TODO: send an email
            print('Failed to create an IS for engravings')

    return Response(status_code=200, body=None)


@app.route('/rubyhas', methods=['POST'])
def purchase_order_webhook():
    request = app.current_request

    print('==========')
    print(request.json_body)
    print('==========')

    return Response(status_code=200, body=None)
