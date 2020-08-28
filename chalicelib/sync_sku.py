import pickle
from datetime import datetime, timedelta
from chalicelib.fulfil import client, get_fulfil_product_api
from chalicelib import RUBYHAS_HQ_STORAGE

BUCKET = 'aurate-sku'


def dump_inventory_positions(inventory):
    # save inventory to s3
    from app import s3
    s3.put_object(Body=pickle.dumps(inventory),
                  Bucket=BUCKET, Key=f'ryby_inventory')


def get_inventory_positions():
    # get inventory from s3
    from app import s3
    response = s3.get_object(Bucket=BUCKET, Key=f'ryby_inventory')
    inventory = pickle.loads(response['Body'].read())
    return inventory


def sku_for_update(inventory):
    # Modify inventory one by one removing from list and check it quantity
    # check time to avoid lambda timeout
    keys = list(inventory.keys())
    start_time = datetime.now()

    for key in keys:
        value = inventory.pop(key)
        product = get_fulfil_product_api(
            'code', key, 'id,quantity_on_hand,quantity_available',
            {"locations": [RUBYHAS_HQ_STORAGE, ]}
        )
        if 'quantity_on_hand' in product:
            fulfil_inventory = product['quantity_on_hand']
            if int(value['rubyhas']) != int(fulfil_inventory):
                yield dict(SKU=key,
                           _id=product['id'],
                           _from=int(fulfil_inventory),
                           _to=int(value['rubyhas']),)

        from app import TIMEOUT
        if datetime.now() - start_time > timedelta(seconds=TIMEOUT - 30):
            print("Stop moment achieved")
            break


def new_inventory(for_update):
    # create inventory record in fullfill
    lines = [{'product': i['_id'], 'quantity': i['_to']} for i in for_update]
    params = [
        {
            'date': client.today(),
            'type': 'cycle',
            'lost_found': 7,
            'location': RUBYHAS_HQ_STORAGE,
            'lines': [['create', lines]],
        }
    ]

    stock_inventory = client.model('stock.inventory')
    res = stock_inventory.create(params)
    return res


def complete_inventory(inventory):
    # complete inventory record in fullfill
    IA = client.model('stock.inventory')
    return IA.complete(inventory)


def confirm_inventory(inventory):
    # confirm inventory record in fullfill (this apply changes)
    IA = client.model('stock.inventory')
    return IA.confirm(inventory)
