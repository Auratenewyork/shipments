from datetime import datetime, timedelta, date

import requests
from requests.auth import HTTPBasicAuth
from retrying import retry

from chalicelib import EASYPOST_API_KEY, EASYPOST_URL


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
    messages = []

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
                        message = f"Late shipment id: {item['id']}, " \
                                  f"Tracking code:  {item['tracking_code']}, " \
                                  f"Status: {item['status']}, " \
                                  f"tracking_status: {tracking['status']} " \
                                  f"from {pre_transit_update} "

                        messages.append(message)
                        # Stop iterating tracking details
                        break
    if len(shipments):
        params['before_id'] = item['id']

    next_page = (len(shipments) == params['page_size'])

    return dict(messages=messages, params=params, next_page=next_page)


def run_in_transit_shipments():
    # function to check functionality in the one tread.
    params = get_transit_shipment_params()
    next_page = True
    with open('easypost_result', 'w') as f:
        while next_page:
            result = pull_in_transit_shipments(params)
            next_page = result['next_page']
            f.writelines(result['messages'])
