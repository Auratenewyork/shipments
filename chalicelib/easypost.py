import pickle
from datetime import datetime, timedelta, date

import requests
from requests.auth import HTTPBasicAuth
from retrying import retry

from chalicelib import EASYPOST_API_KEY, EASYPOST_URL
from chalicelib.common import listDictsToHTMLTable
import easypost


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
    BUCKET = 'aurate-sku'
    from app import s3
    response = s3.get_object(Bucket=BUCKET, Key=f'easypost_reference_match')
    previous_data = pickle.loads(response['Body'].read())
    keys = []

    # import re
    # from collections import Counter
    # numbers = []
    # for item in previous_data['shipments'].values():
    #     match = re.match(r'.+(#\d+).+', item)
    #     if match:
    #         numbers.append(match[1])
    # a = Counter(numbers)
    # b = [{key: value} for key, value in a.items() if value > 1]
    # print(b)


    for key, value in previous_data['shipments'].items():
        if (reference in value) or (sale_number in value):
            keys.append(key)
    if keys:
        return keys
    return get_easypost_record(reference, last_id=previous_data['last_id'])


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
    page = 1
    next_page = True
    result = []
    while next_page:
        shipments_response = easypost.Shipment.all(**params)
        shipments = shipments_response.shipments
        result += shipments
        next_page = (len(shipments) == params['page_size'])
        if shipments:
            params['after_id'] = shipments[0].id
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

