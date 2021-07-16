import json
import requests

from app import app


def test_index():
    response = requests.post(
        'http://127.0.0.1:8000/tmall-hook',
        headers={"content-type": "application/json"},
        data=json.dumps(ORDER_DATA))
    return response


ORDER_DATA = {
    'channel_identifier': '17',
    'reference': '1',  # comment: uniq id of the order in your system
    "confirmed_at": "2021-06-03T08:20:23.251-05:00",
    'customer': {
        'name': 'John Doe',
        'contacts': [
            ['email', 'john@thedoe.com']
        ],
    },
    'billing_address': {
        'name': 'John Doe',
        'address1': '444 Castro St',
        'address2': 'Suite 1200',
        'city': 'Mountain View',
        'zip': '94041',
        'subdivision_code': 'CA',
        'country_code': 'US',
        'email': 'john@thedoe.com',
        'phone': '123-456-7890',
    },
    'shipping_address': {
        'name': 'Joe Doe',
        'address1': '67 Yonge St',
        'address2': 'Suite 1600',
        'city': 'Toronto',
        'zip': 'M5E 1J8',
        'subdivision_code': 'CA',
        'country_code': 'US',
        'email': 'john@thedoe.com',
        'phone': '123-456-7890',
    },
    'sale_lines': [
        {
            'sku': 'AU0014E00700',
            'quantity': 1,
            'unit_price': '200.00',
            'amount': '200.00',
            'comment': 'With extended range for a tall person'
        },
    ],
    'shipping_lines': [
    ],
    'amount': '200.00',
    'currency_code': 'USD',
    'payment_term': 'NET 30',
    'priority': 2,
    'status': 'paid',
    'financial_status': 'paid',
    'fulfillment_status': 'unshipped',
}


