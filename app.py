from chalice import Chalice, Response

from chalicelib.fulfil import get_internal_shipments, get_movement, get_product
from chalicelib.rubyhas import build_purchase_order, create_purchase_order

app = Chalice(app_name='aurate-webhooks')
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
        status_code = create_purchase_order(order)
        print(status_code)

    return Response(status_code=200, body=None)


@app.route('/rubyhas', methods=['POST'])
def purchase_order_webhook():
    request = app.current_request

    print('==========')
    print(request.json_body)
    print('==========')

    return Response(status_code=200, body=None)
