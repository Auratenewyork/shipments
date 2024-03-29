import datetime
import os
import secrets
import requests

from chalicelib.create_fulfil import create_fullfill_order
from chalicelib.decorators import try_except
from chalicelib.dynamo_operations import get_repearment_order, update_repearment_order_info, get_repairment_shipment
from chalicelib.utils import capture_to_sentry, capture_error

false = False
true = True

ENV = os.environ.get('ENV')

RESHINE_URL = 'https://api-live.reshyne.com/api/v1/'


def get_store_address(headers, params):
    url = f'{RESHINE_URL}store-addresses'
    response = requests.get(url, headers=headers, params=params)
    r = response.json()
    if not r['data']:
        return create_store_address(headers, params)
    return r['data'][0]['id']


def create_store_address(headers, params):
    url = f'{RESHINE_URL}store-addresses'
    params = params.copy()
    params.update({
        "street1" : "580 5th Avenue",
        "street2" : "Suite 2300",
        "city" : 'New York',
        # "city_id" : 'New York',
        "state_id" : 37,
        "country_id" : 231,
        "number" : "+111111111111",
        "zip_code" : 10036,
        "is_default" : True,
        "is_active" : True,
        "label" : "office",
    })
    response = requests.post(url, headers=headers, json=params)
    r = response.json()
    return r['data']['id']


def get_customer(headers, params, address, email):
    url = f'{RESHINE_URL}filter-customers'
    p = params.copy()
    p['email'] = email
    # p['name'] = address['first_name']
    p['page'] = 1
    response = requests.get(url, headers=headers, params=p)
    r = response.json()
    return r['data']['results']


def create_customer(headers, params, address, email):
    url = f'{RESHINE_URL}customers'
    p = params.copy()
    p['user'] = {"email": email, "password": secrets.token_urlsafe(12),
              "first_name": address['first_name'], "last_name": address['last_name']}
    p['phone_number'] = address['phone'].replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
    response = requests.post(url, headers=headers, json=p)
    r = response.json()
    return r['data']


def get_or_create_customer(headers, params, address, email):
    customer = get_customer(headers, params, address, email)
    if customer:
        return customer[0]
    else:
        customer = create_customer(headers, params, address, email)
        return customer
    # return customer


def get_customer_address(headers, params):
    url = f'{RESHINE_URL}customer-addresses'
    response = requests.get(url, headers=headers, params=params)
    r = response.json()
    return r['data']


def get_country_id(headers, address):
    url = f'{RESHINE_URL}countries'
    response = requests.get(url, headers=headers)
    r = response.json()
    for i in r['data']:
        if i['name'] == address['country']:
            return i['id']
    return None


def get_state_id(headers, address, country_id):
    url = f'{RESHINE_URL}countries/231/states'
    response = requests.get(url, headers=headers)
    r = response.json()
    for i in r['data']:
        if i['name'] == address['province']:
            return i['id']
    return None


def get_city_info(headers, address):
    # country_id = get_country_id(headers, address)
    # state_id = get_state_id(headers, address, country_id)
    # return {"state_id": state_id, "country_id": country_id}
    url = f'{RESHINE_URL}cities'
    response = requests.get(url, headers=headers, params={'name':address['city']})
    r = response.json()
    result = {}
    for i in r['data']:
        if i['name'].startswith(address['city']) and \
                i['state']['name'] == address['province'] and \
                i['state']['country']['name'] == address['country']:
            result = {"city_id" : i['id'], "state_id":  i['state']['id'],
                    "country_id": i['state']['country']['id']}


def format_phone(number):
    if not number:
        return '111111111'
    return ''.join((digit for digit in number if digit.isdigit()))


def create_customer_address(headers, params, address, customer_id):
    url = f'{RESHINE_URL}customer-addresses'
    reshine_address = params.copy()
    reshine_address.update({
        "first_name": address['first_name'],
        "last_name": address['last_name'],
        "street1": address['address1'],
        "street2": address['address2'],
        "zip_code": address['zip'],
        "address_type": "r",
        # "store_id": 1,   comes with params
        "customer_id": customer_id,
        "country_id": 231,  # US
        "state_id": get_state_id(headers, address, 231),
        "city": address['city'],
        "phone_number": format_phone(address['phone']),
    })
    city_info = get_city_info(headers, address)
    if city_info:
        reshine_address.update(city_info)
        reshine_address.pop('city_id', None)  # for beta version
    print(reshine_address)
    response = requests.post(url, headers=headers, json=reshine_address)
    r = response.json()
    return r['data']


def find_customer_address(customer_address, address):
    for customer_a in customer_address:
        if (customer_a['street1'] == address['address1'] and
            customer_a['street2'] == address['address2'] and
            customer_a['zip_code'] == address['zip']):
            return customer_a


def get_or_create_customer_address(headers, params, address, customer_id):
    customer_address = get_customer_address(headers, {'customer_id': customer_id})
    customer_address = find_customer_address(customer_address, address)
    if customer_address:
        return customer_address
    else:
        customer_address = create_customer_address(headers, params, address, customer_id)
        return customer_address


def create_options_text_field(text_field):
    test = 'TEST' if ENV == 'local' else ''
    url = f'{RESHINE_URL}sales-order'
    if isinstance(text_field, list):
        text_field = ', '.join(text_field)

    if test:
        text_field = '{}-{}-{}'.format(test, text_field, test)
    return text_field


def create_reshyne_sales_order(headers, service, **kwargs):
    url = f'{RESHINE_URL}sales-order'
    options = service['options'][0:1]
    params = {
        "store_id": kwargs.get('store_id'),  # 49
        "customer_id": kwargs.get('customer_id'),
        "customer_address_id": kwargs.get('customer_address_id'),
        "is_store_pickup": True,
        # "shipping_id": kwargs.get('shipping_id'),
        # "rate_id": kwargs.get('rate_id'),
        "is_round_trip_shipping": True,
        "is_drop_off": True,
        "line_items": [
        #     {
        #     "status": 1,
        #     "service_cost": service['price'],  # 25
        #     "service_option_total_cost": "40.00",
        #     "qty": 1,
        #     "method": "POST",
        #     "price_group_service_id": service['id'],
        #     "name": "",
        #     "brand": "",
        #     "item_options": [
        #         {
        #             "option_id": o['id'],
        #             "field_type": o['field_type'],
        #             "field_label": o['name'],
        #             "field_text": ', '.join(o['field_text1']),
        #             "field_value": o['other_option_value'],
        #             # "formula": o['formula'],
        #             "cost": 0,
        #             "method": "POST",
        #             # "meta_data": o['meta_data']
        #
        #         }
        #         for o in options
        #     ]
        # },
            {
                "service_id": service['id'],
                "service_cost": service['price'],
                "total_service_option_cost": 0,
                "qty": 1,
                "item_value": 0,
                "insurance_amount": 0,
                "method": "POST",
                "options": [
                        {
                        "option_id": o['id'],
                        # "field_type": o['field_type'],
                        "field_label": o['name'],
                        "field_text": create_options_text_field(o['field_text1']),
                        "price": 0,
                        "method": "POST",
                        # "images": [{
                        #     "image": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgID",
                        #     "method": "POST",
                        #     "image_type": "customer_before"
                        # },],
                        # "values": [{
                        #         "price": 0.0,
                        #         "value_id": v['id'],
                        #         "method": "POST"
                        #     } for v in o['values']]
                        } for o in options
                ]
            },
        ]
    }
    response = requests.post(url, headers=headers, json=params)
    if response.status_code == 201:
        return response.json()['data']

    capture_to_sentry(
        'Reshyne sales order creation error',
        data=response.content,
        email='aurate2021@gmail.com')


def login():
    url = f'{RESHINE_URL}login'
    j = {
        "username": "aurate",
        "password": "auratenewyork"
    }
    try:
        response = requests.post(url, json=j)
    except Exception as e:
        capture_error(e, errors_source='Reshine login')
        return
    status = response.status_code
    if status != 200:
        capture_to_sentry(
            'Login to reshyne returns {}!'.format(str(status)), url=url)
        return

    res = response.json()
    token = res['data']['access']
    store = res['data']['store']
    return token, store


def get_services(headers, params, name):
    service_names = {
        'ring reshaping': 'Ring Reshaping',
        'earring posts backs': 'Earring Post Back',
        'clasp repair/replacement': 'Clasp Repair/Replacement',
        'jewelry polishing': 'Jewelry Polishing',
        'rhodium plating': 'Rhodium Plating',
        'gem stone / diamond replacement services': 'Gemstone Replacement Service',
        'stone tightening': 'Stone Tightening'
    }
    url = f'{RESHINE_URL}stores/{params["store_id"]}/portal-services'
    # url = f'{RESHINE_URL}filter-services'
    p = params.copy()
    p['is_customer_side'] = 'true'
    response = requests.get(url, headers=headers, params=p)
    r = response.json()
    services = []
    for s in r['data']['services'].values():
        services.extend(s)
    for s in services:
        if s['name'] == service_names[name]:
            return s
    # return services


def create_reshyne_repearments_order(item):
    reshine_data = login()
    if not reshine_data:
        return

    token, store = reshine_data
    headers = {"AUTHORIZATION": f"Bearer {token}"}
    store_id = store['id']
    params = {'store_id': store_id}
    customer = get_or_create_customer(headers, params, address=item['address'], email=item['email'])
    address = get_or_create_customer_address(headers, params, address=item['address'], customer_id=customer['id'])
    service = get_services(headers, params, name=item['service'])
    shipment = get_repairment_shipment(repairement_id=item['DT'])
    order_data = {
        "store_id": store_id,  # 49
        "customer_id": customer['id'],
        "customer_address_id": address['id'],
        "shipping_id": shipment['sh_id'],
        "rate_id": shipment['rate_id']
    }
    order = create_reshyne_sales_order(headers, service, **order_data)
    return order


def get_order_info(store_id, _id, headers):
    url = f'{RESHINE_URL}sales-order/{_id}'
    response = requests.get(url, headers=headers)
    r = response.json()
    return r['data']


def get_sales_order_info(_id):
    token, store = login()
    headers = {"AUTHORIZATION": f"Bearer {token}"}
    store_id = store['id']
    order_info = get_order_info(store_id=store_id, _id=_id, headers=headers)
    return order_info


@try_except(task='update_or_create_repairement_order')
def update_or_create_repairement_order(repairement_id):
    item = get_repearment_order(repairement_id)
    if 'repearment_id' not in item:
        order = create_reshyne_repearments_order(item)
        if not order:
            return False
        update_repearment_order_info(int(repairement_id), order)
        dd = create_fullfill_order(item)
    return True




"""
Services and SKU associated with them


42446, '[SH0002] AU Green Medium Shipper	AU Green Medium Shipper', 'SH0002',
42447, '[SH0001] AU Green Small Shipper	AU Green Small Shipper', 'SH0001',

50428, '[SU001] Labor & Metal - CASTING	Labor & Metal - CASTING', 'SU001',
50429, '[SU002] Labor - Polishing	Labor - Polishing', 'SU002',
50430, '[SU003] Labor & Metal - PLATING	Labor & Metal - PLATING', 'SU003',,


21465, '[SU0001] Engraving Fee	Engraving Fee', 'SU0001',
21466, '[SU0002] Overnight Ship Fee	Overnight Ship Fee', 'SU0002',

21467, '[SU0003] Repair Fee	Repair Fee', 'SU0003',

53974, '[SU0004] Ring Reshaping	Ring Reshaping', 'SU0004',
53975, '[SU0005] Earring Posts Backs	Earring Posts Backs', 'SU0005',
53976, '[SU0006] Clasp Repair/Replacement	Clasp Repair/Replacement', 'SU0006',
53977, '[SU0007] Jewelry Polishing	Jewelry Polishing', 'SU0007',
53978, '[SU0008] Rhodium Plating	Rhodium Plating', 'SU0008',
53979, '[SU0009] Gemstone/Diamond Replacement Services	Gemstone/Diamond Replacement Services', 'SU0009',
53980, '[SU0010] Stone Tightening Stone Tightening', 'SU0010',


'ring reshaping': 'Ring Reshaping',
'earring posts backs': 'Earring Post Back',
'clasp repair/replacement': 'Clasp Repair/Replacement',
'jewelry polishing': 'Jewelry Polishing',
'rhodium plating': 'Rhodium Plating',
'gem stone / diamond replacement services': 'Gemstone Replacement Service',
'stone tightening': 'Stone Tightening'
"""