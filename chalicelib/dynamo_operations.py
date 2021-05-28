from decimal import Decimal

import boto3
import re
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


# EASYPOST_TABLE = 'fullfill_easypost_order'
EASYPOST_TABLE = 'easypost_ids'
SHOPIFY_SKU = 'shopify_sku'
REPAIRMENT_TABLE = 'repairment'


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
                image = product['image']['src']


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


def list_repearment_orders(ExclusiveStartKey=None, order_name=None, approve=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPAIRMENT_TABLE)
    scan_kwargs = {'Limit': 50}
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
    response['Items'].reverse()
    return response['Items'], response.get('LastEvaluatedKey', {})


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

