import csv
from datetime import datetime
from decimal import Decimal
from operator import and_
from functools import partial, reduce

import boto3
import re
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from chalicelib import CLAIMS_PAGE_SIZE
from chalicelib.utils import paginate_items


# EASYPOST_TABLE = 'fullfill_easypost_order'
EASYPOST_TABLE = 'easypost_ids'
SHOPIFY_SKU = 'shopify_sku'
REPAIRMENT_TABLE = 'repairment'
TMALL_LABEL_TABLE = 'tmall-labels'
CLAIM_SHIPMENT_TABLE = 'repairment_shipments'


def save_easypost_to_dynamo(info):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(EASYPOST_TABLE)

    count = 0
    ref_experssion = re.compile(r'.*(#\d+).*')
    so_experssion = re.compile(r'.*(SO\d+).*')

    for key, value in info['shipments'].items():
        if value.startswith('#'):
            ref_match = ref_experssion.findall(value)
            SK = ref_match[0]
        elif value.startswith('SO'):
            so_match = so_experssion.findall(value)
            SK = so_match[0]
        else:
            continue
        count += 1
        table.put_item(Item={'PK': SK, 'SK': key})

    table.update_item(
        Key={
            "PK": "last_id",
            'SK': 'last_id',
        },
        UpdateExpression="set id=:i",
        ExpressionAttributeValues={
            ':i': info['last_id'],
        },
    )
    return


def get_dynamo_last_id():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(EASYPOST_TABLE)

    response = table.query(
        KeyConditionExpression=Key('PK').eq('last_id'),
    )
    if response['Items']:
        return response['Items'][0]['id']
    else:
        return None


def get_easypost_ids(reference, sale_number):
    # dynamodb = boto3.resource('dynamodb')
    # batch_keys = {
    #    EASYPOST_TABLE: {'Keys': [{'PK': reference}, {'PK': sale_number}]}
    # }
    # try:
    #     response = dynamodb.batch_get_item(RequestItems=batch_keys)
    # except ClientError as e:
    #     print(e.response['Error']['Message'])
    #     return []
    # else:
    #     return response['Responses'][EASYPOST_TABLE]
    result = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(EASYPOST_TABLE)
    if sale_number:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(sale_number),
        )
        result = [i['SK'] for i in response['Items']]
        if result:
            return result
    if reference:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(reference),
        )
        result = [i['SK'] for i in response['Items']]
    return result


def save_shopify_sku(products):
    sku_ = {}
    for product in products:
        for variant in product['variants']:
            # image = ''
            for i in product['images']:
                if variant['image_id'] == i['id']:
                    image = i['src']
                    break
            else:
                if product['image']:
                    image = product['image']['src']
                else:
                    image = ''


            sku = variant['sku']
            sku_[sku] = {
                'name': product['title'],
                'title': variant['title'],
                'image': image,
                'product_type': product['product_type'],
                'product_id': product['id'],
            }
    save_sku_to_dynamo(sku_)
    return


def save_sku_to_dynamo(sku_s):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(SHOPIFY_SKU)

    write = False
    count = 0
    for key, value in sku_s.items():
        count += 1
        if not key:
            write = True
        if write and key:
            item = dict(PK=key, **value)
            table.put_item(Item=item)

    return


def get_shopify_sku_info(sku):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(SHOPIFY_SKU)

    try:
        response = table.get_item(Key={'PK': sku})
        if 'Item' in response:
            return response['Item']
        else:
            return None
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None


def get_multiple_sku_info(sku_list):
    sku_list = list(set(sku_list))
    dynamodb = boto3.resource('dynamodb')
    batch_keys = {
        SHOPIFY_SKU: {'Keys': [{'PK': sku} for sku in sku_list]}
    }
    try:
        response = dynamodb.batch_get_item(RequestItems=batch_keys)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Responses']['shopify_sku']


def scan_without_images():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(SHOPIFY_SKU)

    scan_kwargs = {
        'FilterExpression': Key('image').eq(''),
        'ProjectionExpression': "PK, image, product_id",
        # 'Limit':2,
    }
    items = []

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        i = response.get('Items', [])
        items.extend(i)
        if not i:
            break
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
        # break
    return items

# <class 'list'>: [{'PK': 'AU0426F00900', 'image': '', 'product_id': Decimal('4460529188961')}]


def update_shopify_image(pk, image):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(SHOPIFY_SKU)

    table.update_item(
        Key={
            "PK": pk,
        },
        UpdateExpression="set image=:i",
        ExpressionAttributeValues={
            ':i': image,
        },
    )


def save_repearment_order(info):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    table.put_item(Item=info)
    return info


def update_repearment_order(DT, approve, note):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    res = table.update_item(
        Key={
            "DT": DT,
        },
        UpdateExpression="set approve=:s, note=:c",
        ExpressionAttributeValues={
            ':s': approve,
            ':c': note,
        },
    )


def update_repearment_tracking_number(DT, tracking_number):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    res = table.update_item(
        Key={
            "DT": DT,
        },
        UpdateExpression="set tracking_number=:t",
        ExpressionAttributeValues={
            ':t': tracking_number,
        },
    )


def update_repearment_order_info(DT, order):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    res = table.update_item(
        Key={
            "DT": DT,
        },
        UpdateExpression="set repearment_id=:t, repearment_no=:n, repearment_total=:c ",
        ExpressionAttributeValues={
            ':t': order['id'],
            ':n': order['order_no'],
            ':c': Decimal(order['total']),
        },
    )
    print()


def search_repearments_order(order_name, approve, page, page_size=CLAIMS_PAGE_SIZE):
    items = list_repearment_orders_(order_name=order_name, approve=approve)
    items, total = paginate_items(items, sort_key='DT')
    return items, {}, total


def list_repearment_orders(ExclusiveStartKey=None, order_name=None,
                           approve=None, extra_filter=None, page=1, page_size=CLAIMS_PAGE_SIZE):
    if order_name:
        return search_repearments_order(order_name, approve, page, page_size)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    # scan_kwargs = {'Limit': 50}
    scan_kwargs = {}
    scan_kwargs['IndexName'] = 'approve-DT-index'
    scan_kwargs['ScanIndexForward'] = False
    scan_kwargs['KeyConditionExpression'] = Key('approve').eq(approve.lower())

    if ExclusiveStartKey:
        ExclusiveStartKey = int(ExclusiveStartKey)
        scan_kwargs['ExclusiveStartKey'] = ExclusiveStartKey

    # approve_expresion = Attr('approve').eq('declined')
    # scan_kwargs['FilterExpression'] = approve_expresion

    response = table.query(**scan_kwargs)
    items = response['Items']
    items, total = paginate_items(items, page, page_size, sort_key='DT')

    # # scan all table in the future replace by pagination
    while 'LastEvaluatedKey' in response:
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        response = table.scan(**scan_kwargs)
        items.extend(response['Items'])
    if items:
        items = batch_get_repearments(items, extra_filter)

    return items, response.get('LastEvaluatedKey', {}), total


def batch_get_repearments(items, extra_filter=None):
    dynamodb = boto3.resource('dynamodb')
    # items = items[:99]
    batch_keys = {
       REPAIRMENT_TABLE: {'Keys': [{'DT': item['DT']} for item in items]}
    }
    response = dynamodb.batch_get_item(RequestItems=batch_keys)
    items = response['Responses'][REPAIRMENT_TABLE]
    items.sort(key=lambda x: x['DT'], reverse=True)
    if extra_filter:
        items = [item for item in items if item.get(extra_filter)]

    return items


# Unused
def list_repearment_orders_(ExclusiveStartKey=None, order_name=None, approve=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    scan_kwargs = {}
    if ExclusiveStartKey:
        ExclusiveStartKey = int(ExclusiveStartKey)
        scan_kwargs['ExclusiveStartKey'] = ExclusiveStartKey

    approve_expresion = None
    if approve:
        if approve.lower() == 'pending':
            approve_expresion = Attr('approve').not_exists()
        else:
            approve_expresion = Attr('approve').eq(approve.lower())

    if order_name and approve_expresion:
        scan_kwargs['FilterExpression'] = Attr('order_name').contains(order_name) & approve_expresion
    elif order_name:
        scan_kwargs['FilterExpression'] = Attr('order_name').contains(order_name)
    elif approve_expresion:
        scan_kwargs['FilterExpression'] = approve_expresion
    response = table.scan(**scan_kwargs)
    items = response['Items']

    # scan all table in the future replace by pagination
    while 'LastEvaluatedKey' in response:
        # print(response['LastEvaluatedKey'])
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        response = table.scan(**scan_kwargs)
        items.extend(response['Items'])

    # items.reverse()
    return items


def list_repearment_by_date(DT_start, DT_end, ExclusiveStartKey=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    scan_kwargs = {'Limit': 50}

    if ExclusiveStartKey:
        ExclusiveStartKey = int(ExclusiveStartKey)
        scan_kwargs['ExclusiveStartKey'] = ExclusiveStartKey
    scan_kwargs['FilterExpression'] = Key('DT').between(Decimal(DT_start), (Decimal(DT_end)))
    # scan_kwargs['ConditionExpression'] = Attr('tracking_number').not_exists()
    response = table.scan(**scan_kwargs)
    return response['Items'], response.get('LastEvaluatedKey', {})


def get_repearment_order(DT):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    DT = int(DT)
    try:
        response = table.get_item(Key={'DT': DT})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


def UUlist_repearment_orders(ExclusiveStartKey=None, order_name=None,
                           approve=None, extra_filter=None, page=1, page_size=CLAIMS_PAGE_SIZE):
    if order_name:
        return search_repearments_order(order_name, approve, page, page_size)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    default_scan_kwargs = {
        'IndexName': 'approve-DT-index',
        'ScanIndexForward': False,
        'KeyConditionExpression': Key('approve').eq(approve.lower()),
    }

    scan_kwargs = default_scan_kwargs.copy()
    scan_kwargs['Limit'] = page_size

    if ExclusiveStartKey:
        ExclusiveStartKey = int(ExclusiveStartKey)
        scan_kwargs['ExclusiveStartKey'] = ExclusiveStartKey

    response = table.query(**scan_kwargs)
    items = response['Items']
    total = 0
    if items:
        # items.sort(key=lambda x: x['DT'], reverse=True)
        items = batch_get_repearments(items, extra_filter)
        total = table.query(Select='COUNT', **default_scan_kwargs)['Count']

    return items, response.get('LastEvaluatedKey', {}), total


def get_repairs_for_customer(customer_id=2949941133409, ExclusiveStartKey=None, page=1, page_size=CLAIMS_PAGE_SIZE, full=False):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    default_scan_kwargs = {
        'FilterExpression': Key('customer_id').eq(customer_id),
    }
    total = table.scan(Select='COUNT', **default_scan_kwargs)['Count']
    if not total:
        return [], {}, total

    scan_kwargs = default_scan_kwargs.copy()
    # scan_kwargs['Limit'] = min(page_size, int(total))

    if ExclusiveStartKey:
        scan_kwargs['ExclusiveStartKey'] = int(ExclusiveStartKey)

    response = table.scan(**scan_kwargs)
    items = response['Items']
    return items, response.get('LastEvaluatedKey', {}), total


def get_customer_data_from_repairs(customers):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    scan_kwargs = {}
    # scan_kwargs = {'Limit': 50}
    scan_kwargs['FilterExpression'] = Attr('approve').eq('accepted')
    response = table.scan(**scan_kwargs)
    items = response.get('Items')
    return items
    if items:
        headers = [ "DT", "Date",
            'Name', 'Email', 'Order', 'Sku', 'Photo', 'Service', 'Status',
            'Tracking_number', 'Description', 'Note']
        with open('customers.csv', 'w', newline='') as csvfile:
            cwriter = csv.writer(csvfile)
            cwriter.writerow(headers)
            for item in items:
                cwriter.writerow([
                    item['DT'],
                    datetime.fromtimestamp(item['DT']).isoformat(),
                    item['address'].get('name', ''),
                    item['email'],
                    item['order_name'],
                    item['sku'],
                    item['files'][0]['image_url'],
                    item['service'],
                    item['approve'],
                    item.get('tracking_number', ''),
                    item['description'],
                    item.get('note', '')])
    return items


def save_to_dynamo(data, table):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    table.put_item(Item=data)
    return data


def get_from_dynamo(id_key, value, table):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    try:
        response = table.get_item(Key={id_key: value})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


# TODO: add limit arg
def filter_dynamo_by_index(table, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    scan_kwargs = {}
    scan_kwargs['FilterExpression'] = reduce(and_, [Attr(k).eq(v) for k, v in kwargs.items()])
    try:
        response = table.scan(**scan_kwargs)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response.get('Items')


def update_dynamo_item(id_key, table, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    id_key = {id_key: kwargs.pop(id_key)}
    expression = 'set '
    values = {}
    for k, val in kwargs.items():
        expression += '{k}=:{k}, '.format(k=k)
        values[':{k}'.format(k=k)] = val
    expression = expression.strip(', ')

    table.update_item(
        Key=id_key,
        UpdateExpression=expression,
        ExpressionAttributeValues=values
    )


def delete_dynamo_item(id_key, table, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    id_key = {id_key: kwargs.pop(id_key)}
    params = {'Key': id_key}
    if kwargs:
        expression = 'set '
        values = {}
        for k, val in kwargs.items():
            expression += '{k}=:{k}, '.format(k=k)
            values[':{k}'.format(k=k)] = val
        params['ConditionExpression'] = expression.strip(', ')
        params['ExpressionAttributeValues'] = values
    table.delete_item(**params)


add_tmall_label = partial(save_to_dynamo, table=TMALL_LABEL_TABLE)


save_repairment_shipment = partial(save_to_dynamo, table=CLAIM_SHIPMENT_TABLE)
get_repairment_shipment_by_id = partial(get_from_dynamo, id_key='sh_id', table=CLAIM_SHIPMENT_TABLE)
filter_repairement_shipments = partial(filter_dynamo_by_index, table=CLAIM_SHIPMENT_TABLE)
update_repairment_shipment = partial(update_dynamo_item, id_key='sh_id', table=CLAIM_SHIPMENT_TABLE)
delete_repairment_shipment = partial(delete_dynamo_item, id_key='sh_id', table=CLAIM_SHIPMENT_TABLE)


update_repairment_order = partial(update_dynamo_item, id_key='DT', table=REPAIRMENT_TABLE)


def get_repairment_shipment(**kwargs):
    shipments = filter_repairement_shipments(**kwargs)
    if shipments:
        return shipments[0]


def get_tmall_label(tid):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TMALL_LABEL_TABLE)
    try:
        response = table.get_item(Key={'id': tid})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']
