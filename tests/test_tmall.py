import json
import random
import requests

from app import app


def create_order_data(data=None):
    if not data:
        data = ORDER_DATA.copy()

    init_val = random.randint(1, 100000000)  # change it to avoid updating orders
    init_val = f'TEST-{init_val}'
    data['channel_identifier'] = str(init_val)
    data['reference'] = str(init_val)
    return data


def test_tmall_hook_should_create_and_cancel_order():
    order_data = create_order_data()
    response = requests.post(
        'http://127.0.0.1:8000/tmall-hook',
        headers={"content-type": "application/json"},
        data=json.dumps({'Event': 'taobao_trade_TradePAID', 'Content': order_data}))
    assert response.status_code == 201

    refund_data = REFUND_DATA.copy()
    refund_data['channel_identifier'] = order_data['channel_identifier']
    refund_data['reference'] = order_data['reference']

    refund_data['sale_lines'][0] = order_data['sale_lines'][0]
    refund_data['amount'] = order_data['amount']
    import ipdb; ipdb.set_trace()



ORDER_DATA = {
    "channel_identifier": "1741644415596323129",
    "reference": "1741644415596323129",
    "confirmed_at": "2021-07-18 21:40:15",
    "customer": {
        "name": "jacky huang",
        "contacts": [
            [
                "email",
                "312129003@qq.com"
            ]
        ]
    },
    "billing_address": {
        "name": "jacky huang",
        "address1": "hunan chagnsha",
        "address2": "address2",
        "city": "chagnsha",
        "zip": "410000",
        "subdivision_code": "CA",
        "country_code": "CHN",
        "email": "312129003@qq.com",
        "phone": "+8613397642288"
    },
    "shipping_address": {
        "name": "jacky huang",
        "address1": "hunan chagnsha",
        "address2": "address2",
        "city": "chagnsha",
        "zip": "410000",
        "subdivision_code": "CA",
        "country_code": "CHN",
        "email": "312129003@qq.com",
        "phone": "+8613397642288"
    },
    "sale_lines": [{"sku":"AU0315B00700","quantity":1,"unit_price":"1081.57","amount":"1081.57","comment":"AURATE\\u6247\\u5f62\\u73cd\\u73e0\\u6212\\u6307\\u590d\\u53e4\\u8bbe\\u8ba1\\u611f\\u7eaf\\u94f6\\u954014K\\u91d1\\u6307\\u6212\\u53e0\\u6212"}],
    "shipping_lines": [

    ],
    "amount": "1081.57",
    "currency_code": "CNY",
    "payment_term": "Due on receipt",
    "priority": 2,
    "status": "pending",
    "financial_status": "paid",
    "fulfillment_status": "unshipped"
}


REFUND_DATA = {
    "channel_identifier": "TEST-74806142",
    "reference": "TEST-74806142",
    "confirmed_at": "2021-07-18 21:40:15",
    "customer":
    {
        "name": None,
        "contacts":
        [
            [
                "email",
                None
            ]
        ]
    },
    "billing_address":
    {
        "name": None,
        "address1": None,
        "address2": None,
        "city": None,
        "zip": None,
        "subdivision_code": None,
        "country_code": None,
        "email": None,
        "phone": None
    },
    "shipping_address":
    {
        "name": None,
        "address1": None,
        "address2": None,
        "city": None,
        "zip": None,
        "subdivision_code": None,
        "country_code": None,
        "email": None,
        "phone": None
    },
    "sale_lines":
    [
        {
            "sku": "AU0315B00700",
            "quantity": 1,
            "unit_price": "1081.57",
            "amount": "1081.57",
            "comment": "AURATE\\u6247\\u5f62\\u73cd\\u73e0\\u6212\\u6307\\u590d\\u53e4\\u8bbe\\u8ba1\\u611f\\u7eaf\\u94f6\\u954014K\\u91d1\\u6307\\u6212\\u53e0\\u6212"
        }
    ],
    "shipping_lines":
    [],
    "amount": 1081.57,
    "currency_code": "CNY",
    "payment_term": "Due on receipt",
    "priority": 2,
    "status": "pending",
    "financial_status": "refunded",
    "fulfillment_status": "unshipped"
}
