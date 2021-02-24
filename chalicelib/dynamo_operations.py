import boto3
import re
from boto3.dynamodb.conditions import Key, Attr


EASYPOST_TABLE = 'fullfill_easypost_order'


def save_easypost_to_dynamo(info):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(EASYPOST_TABLE)

    count = 0
    experssion = re.compile(r'.*(#\d+).*')
    for key, value in info['shipments'].items():
        match = experssion.findall(value)
        if match:
            SK = match[0]
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
    return response['Items'][0]['id']


def get_easypost_ids(reference):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(EASYPOST_TABLE)

    response = table.query(
        KeyConditionExpression=Key('PK').eq(reference),
    )
    result = [i['SK'] for i in response['Items']]
    return result
