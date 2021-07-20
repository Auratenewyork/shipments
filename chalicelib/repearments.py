import datetime
import os
import secrets
import requests

false = False
true = True

env_name = os.environ.get('ENV', 'sandbox')
# if env_name == 'sandbox' or True:
if False:
    RESHINE_URL = 'https://api-uat.reshyne.com/api/v1/'
else:
    RESHINE_URL = 'https://api-app.reshyne.com/api/v1/'


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


def get_city_info(headers, address):
    url = f'{RESHINE_URL}cities'
    response = requests.get(url, headers=headers, params={'name':address['city']})
    r = response.json()
    for i in r['data']:
        if i['name'].startswith(address['city']) and \
                i['state']['name'] == address['province'] and \
                i['state']['country']['name'] == address['country']:
            return {"city_id" : i['id'], "state_id":  i['state']['id'],
                    "country_id": i['state']['country']['id']}


def create_customer_address(headers, params, address, customer_id):
    url = f'{RESHINE_URL}customer-addresses'
    p = params.copy()

    p.update({
            "first_name": address['first_name'],
            "last_name": address['last_name'],
            "street1": address['address1'],
            "street2": address['address2'],
            "zip_code": address['zip'],
            "address_type": "r",
            # "store_id": 1,   comes with params
            "customer_id": customer_id,
        })
    city_info = get_city_info(headers, address)
    if city_info:
        p.update(city_info)
    response = requests.post(url, headers=headers, json=p)
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

def create_rep_order(headers, store_id, customer_id, address_id, service):
    url = f'{RESHINE_URL}sales-order'
    options = service['service_options'][0:1]
    # options = []
    params = {
        "store_id": store_id,  # 49
        "customer_id": customer_id,
        "customer_address_id": address_id,
        "is_store_pickup": true,
        # "shipping_id": "shp_d4556189cb014ff1b805b446c0423c2e",
        # "rate_id": "rate_a6470ccfe40147588c2a3613e9276cad",
        "is_round_trip_shipping": true,
        "is_drop_off": true,
        "items": [{
            "status": 1,
            "service_cost": service['price'],  # 25
            "service_option_total_cost": "40.00",
            "qty": 1,
            "method": "POST",
            "price_group_service_id": service['id'],
            "name": "",
            "brand": "",
            "item_options": [
                {
                    "option_id": o['id'],
                    "field_type": o['field_type'],
                    "field_label": o['name'],
                    "field_text": ', '.join(o['field_text1']),
                    "field_value": o['other_option_value'],
                    "formula": o['formula'],
                    "cost": 0,
                    "method": "POST",
                    # "meta_data": o['meta_data']

                }
                for o in options
            ]
        }]
    }
    response = requests.post(url, headers=headers, json=params)
    r = response.json()
    a = r['data']
    return r['data']


def login():
    url = f'{RESHINE_URL}login'
    j = {
        "username": "auratenewyork",
        "password": "auratenewyork"
    }
    response = requests.post(url, json=j)
    res = response.json()
    token = res['data']['access']
    store = res['data']['store']
    return token, store


def get_services(headers, params):
    url = f'{RESHINE_URL}filter-services'
    response = requests.get(url, headers=headers, params=params)
    r = response.json()
    return r['data']


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


def test_fill_input_item(item):
    if not item.get('address', None):
        item['address'] = {
            "first_name": "Nick",
            "last_name": "Miller",
            "company": "Aurate New York",
            "address1": "580 5th avenue",
            "address2": "",
            "city": "New York",
            "province": "New York",
            "country": "United States",
            "zip": "10012",
            "phone": "+12016550927",
            "name": "Nick Miller",
            "province_code": "NY",
            "country_code": "US",
            "country_name": "United States",
            "default": true
        }
    if not item.get('service', None):
        item['service'] = 'earring posts backs'
    if not item.get('email', None):
        item['email'] = 'maxwell@auratenewyork.com'
    return item


def create_repearments_order(item):
    item = test_fill_input_item(item)

    token, store = login()
    headers = {"AUTHORIZATION": f"Bearer {token}"}
    params = {'store_id': store['id']}
    store_id = store['id']

    customer = get_or_create_customer(headers, params, address=item['address'], email=item['email'])
    address = get_or_create_customer_address(headers, params, address=item['address'], customer_id=customer['id'])
    address_id = address['id']

    customer_id = customer['id']

    service = get_services(headers, params, name=item['service'])
    order = create_rep_order(headers, store_id, customer_id, address_id, service)
    return order


def get_order_info(store_id, _id, headers):
    url = f'{RESHINE_URL}sales-order/{_id}'
    response = requests.get(url, headers=headers)
    r = response.json()
    return r['data']


def get_sales_order_info(_id):
    token, store = login()
    headers = {"AUTHORIZATION": f"Bearer {token}"}
    params = {'store_id': store['id']}
    store_id = store['id']
    order_info = get_order_info(store_id=store_id, _id=_id, headers=headers)
    return order_info



"""
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
# def create_fullfill_order():
#     create_return_sale()
#
#
# def create_sale_line(sale):
#     model = client.model('sale.line')
#     value_list = [
#         {"sale": sale,
#          "item_blurb": {
#              "subtitle": [["SKU", "AU0328EK0000"]],
#                                       "description": "[AU0328EK0000] Birthstone Ear Chain Threader (Garnet - Jan, Yellow, 14K)",
#                                       "title": "Birthstone Ear Chain Threader (Garnet - Jan, Yellow, 14K)"},
#          "amount_blurb": {"title": "-$150.00"},
#          "quantity_blurb": {"subtitle": [["Avl", 1], ["Hand", 1]],
#                             "description": "Ship (from stock) From Aurate HQ",
#                             "title": "Ordered: -1"},
#          "price_blurb": {"subtitle": [["LIST", "$150.00"]], "title": "$150.00"},
#          "delivery_mode": "ship", "product_uom_category": 1,
#          "allow_open_amount": False, "unit": 1, "is_gift_card": False,
#          "unit_price": {"__class__": "Decimal", "decimal": "150"},
#          "warehouse": 57, "party": 40846, "type": "line", "product": 3226,
#          "description": "[AU0328EK0000] Birthstone Ear Chain Threader (Garnet - Jan, Yellow, 14K)",
#          "quantity_buildable": 0,
#          "product_type": "goods",
#          "unit_digits": 0,
#          "is_return": False,
#          "amount": {"__class__": "Decimal", "decimal": "0"},
#          "carrier_service": 424,
#          "discount": {"__class__": "Decimal", "decimal": "0"}, "carrier": 9}]
#     new_record_ids = model.create(value_list)
#     return new_record_ids
#
# def create_return_sale():
#     model = client.model('sale.sale')
#     value_list = [
#         {'channel': 3, 'party': 40846,
#          'sale_date': datetime.date.today().isoformat(),
#          'shipment_address': 59296, "payment_term": 1,
#          }]
#     new_record_ids = model.create(value_list)
#     lines = create_sale_line(new_record_ids[0])
#
#     print(new_record_ids)
#
#
