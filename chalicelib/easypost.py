import pickle
from datetime import datetime, timedelta, date

import requests
from requests.auth import HTTPBasicAuth
from retrying import retry

from chalicelib import EASYPOST_TEST_API_KEY, EASYPOST_API_KEY, EASYPOST_URL
from chalicelib.common import listDictsToHTMLTable
import easypost
from chalicelib.dynamo_operations import get_easypost_ids
from chalicelib.utils import format_fullname


def get_transit_shipment_params():
    page_size = 100
    s = date.today() - timedelta(days=28)
    e = date.today() - timedelta(days=4)
    start_datetime = f'{s}T00:00:00Z'
    end_datetime = f'{e}T00:00:00Z'
    params = {'start_datetime': start_datetime, 'end_datetime': end_datetime,
              'page_size': page_size}
    return params


@retry(stop_max_attempt_number=2, wait_fixed=50)
def pull_in_transit_shipments(params):
    result = []

    auth = HTTPBasicAuth(EASYPOST_API_KEY, '')
    response = requests.get(url=EASYPOST_URL,
                            params=params, auth=auth)

    shipments = response.json()['shipments']
    for item in shipments:
        if item['status'] == 'pre_transit':
            for tracking in item['tracker']['tracking_details']:
                if tracking['status_detail'] == 'status_update' and tracking['status'] == 'pre_transit':
                    pre_transit_update = datetime.strptime(
                        tracking['datetime'], "%Y-%m-%dT%H:%M:%SZ")
                    if pre_transit_update < (datetime.now() - timedelta(days=5)):
                        res = dict(id=str(item['id']),
                                   status=str(item['status']),
                                   order=str(item['order_id']),
                                   tracking_code=str(item['tracking_code']),
                                   tracking_status_from=str(pre_transit_update),
                                   )
                        result.append(res)
                        # Stop iterating tracking details
                        break
    if len(shipments):
        params['before_id'] = item['id']

    next_page = (len(shipments) == params['page_size'])

    return dict(shipments=result, params=params, next_page=next_page)


def run_in_transit_shipments():
    # function to check functionality in the one tread.
    params = get_transit_shipment_params()
    next_page = True
    result = []
    with open('easypost_result', 'w+') as f:
        while next_page:
            res = pull_in_transit_shipments(params)
            next_page = res['next_page']
            result += res['shipments']
        f.write(str(listDictsToHTMLTable(result)))


def get_easypost_record(reference, last_id=None):
    easypost.api_key = EASYPOST_API_KEY
    params = dict(page_size=100)
    if last_id:
        params['after_id'] = last_id
    page = 1
    not_stop = True
    next_page = True
    result = []
    match = None
    while next_page and not_stop:
        shipments_response = easypost.Shipment.all(**params)
        shipments = shipments_response.shipments

        next_page = (len(shipments) == params['page_size'])
        result += shipments
        if shipments:
            params['after_id'] = shipments[0].id

        for s in shipments:
            # print(s.options.print_custom_1)
            if reference in s.options.print_custom_1:
                match = s
                not_stop = False

        page += 1
        if page > 3:
            not_stop = False
    if len(result):
        save_new_match(result, params.get('after_id', None))
    if match:
        return match.id
    else:
        return None


def get_easypost_record_by_reference(reference, sale_number):
    # delete
    BUCKET = 'aurate-sku'
    from app import s3
    response = s3.get_object(Bucket=BUCKET, Key=f'easypost_reference_match')
    previous_data = pickle.loads(response['Body'].read())
    keys = []

    for key, value in previous_data['shipments'].items():
        if (reference in value) or (sale_number in value):
            keys.append(key)
    if keys:
        return keys
    return get_easypost_record(reference, last_id=previous_data['last_id'])


def get_easypost_record_by_reference_(reference, sale_number):
    return get_easypost_ids(reference, sale_number)


def save_new_match(match, last_id):
    info = collect_new_info(match, last_id)
    BUCKET = 'aurate-sku'
    from app import s3
    response = s3.get_object(Bucket=BUCKET, Key=f'easypost_reference_match')
    previous_data = pickle.loads(response['Body'].read())
    previous_data['shipments'].update(info['shipments'])
    previous_data['last_id'] = info['last_id']
    s3.put_object(Body=pickle.dumps(previous_data), Bucket=BUCKET,
                  Key=f'easypost_reference_match')


def scrape_easypost__match_reference(last_id):
    easypost.api_key = EASYPOST_API_KEY
    params = dict(page_size=100)
    if last_id:
        params['after_id'] = last_id
    else:
        params['after_id'] = 'shp_fd3c26b304e74899b00111bdd1606b95'
    page = 1
    next_page = True
    result = []
    while next_page:
        shipments_response = easypost.Shipment.all(**params)
        shipments = shipments_response.shipments
        result += shipments
        next_page = (len(shipments) == params['page_size'])
        if shipments:
            params['after_id'] = shipments[-1].id
        page += 1
        if page > 50:
            break
    return collect_new_info(result, params['after_id'])


def collect_new_info(result, last_id):
    match = {}
    for shipment in result:
       match[shipment.id] = f"{shipment.options.print_custom_1} " \
                            f"{shipment.options.print_custom_2}"
    return dict(last_id=last_id, shipments=match)


def get_shipment(_id):
    easypost.api_key = EASYPOST_API_KEY
    shipment = easypost.Shipment.retrieve(_id)
    return shipment


def create_address(data, email=None, api_key=EASYPOST_TEST_API_KEY):
    easypost.api_key = api_key
    address = {
        "name": data.get('name', format_fullname(data)),
        "company": data.get('company'),
        "street1": data.get('address1'),
        "street2": data.get('address2'),
        "city": data.get('city'),
        "state": data.get('province_code'),
        "zip": data.get('zip'),
        "country": data.get('country_code'),
        "phone": data.get('phone'),
        "email": email,
        "mode": "test",
    }
    return easypost.Address.create(**address)


def create_predefined_parcel(data, api_key=EASYPOST_TEST_API_KEY):
    easypost.api_key = api_key
    default = {
        "predefined_package": "FlatRateEnvelope",
        "weight": 10
    }
    default.update(data)
    return easypost.Parcel.create(**default)


def create_shipment(from_address, to_address, parcel, api_key=EASYPOST_TEST_API_KEY):
    easypost.api_key = api_key
    data = {}
    data['from_address'] = {'id': from_address}
    data['to_address'] = {'id': to_address}
    data['parcel'] = {'id': parcel}
    return easypost.Shipment.create(**data)
