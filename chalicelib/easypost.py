import pickle
from datetime import datetime, timedelta, date
from decimal import Decimal
from operator import itemgetter

import requests
from requests.auth import HTTPBasicAuth
from retrying import retry

from chalicelib import EASYPOST_API_KEY, EASYPOST_URL
from chalicelib.common import listDictsToHTMLTable
import easypost
from chalicelib.dynamo_operations import (
    get_easypost_ids, filter_repairement_shipments,
    save_repairment_shipment, update_repairment_shipment,
    update_repairment_order, get_repairment_shipment)
from chalicelib.utils import format_fullname, capture_error
from chalicelib.stripe import StripePayment


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
    print(EASYPOST_API_KEY)
    easypost.api_key = EASYPOST_API_KEY
    shipment = easypost.Shipment.retrieve(_id)
    return shipment


class RepairementShipment:

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._easypost_shipment = None
        self.label = None
        self.payment = None
        self.repairement_shipment = None
        easypost.api_key = EASYPOST_API_KEY

    @property
    def id(self):
        if self.repairement_shipment:
            return self.repairement_shipment['sh_id']
        return self._easypost_shipment and self._easypost_shipment.get('id')

    @property
    def exists(self):
        if not self.kwargs:
            return

        if self.easypost_shipment:
            return True

        if 'shipment_id' in self.kwargs:
            return self.get_easypost_shipment(self.kwargs['shipment_id'])

        return self.get_repairement_shipment(**self.kwargs)

    @property
    def easypost_shipment(self):
        if self._easypost_shipment:
            return self._easypost_shipment
        if not self.id:
            return
        self.get_easypost_shipment(self.id)
        return self._easypost_shipment

    def create_address(self, data):
        address = {
            "name": data.get('name', format_fullname(data)),
            "company": data.get('company'),
            "street1": data.get('address1'),
            "street2": data.get('address2'),
            "city": data.get('city'),
            "state": data.get('state') or data.get('province_code'),
            "zip": data.get('zip'),
            "country": data.get('country_code'),
            "phone": data.get('phone'),
            "email": data.get('email'),
            "mode": "test",
        }
        return easypost.Address.create(**address)

    def create_parcel(self, data=None):
        default = {
            "predefined_package": "FlatRateEnvelope",
            "weight": 10
        }
        if data:
            default.update(data)
        return easypost.Parcel.create(**default)

    @retry(stop_max_attempt_number=2, wait_fixed=50)
    def create_easypost_shipment(self, from_address, to_address):
        self.from_address = self.create_address(from_address)
        self.to_address = self.create_address(to_address)
        self.parcel = self.create_parcel(from_address)
        self._easypost_shipment = easypost.Shipment.create(**{
            'from_address': self.from_address,
            'to_address': self.to_address,
            'parcel': self.parcel
        })
        return self

    def create_repearment_shipment(self, repairement):
        if not self.id:
            return ValueError('Get or create shipment firstly')
        shipment = self.easypost_shipment

        data = {
            'sh_id': shipment['id'],
            'customer_id': repairement['customer_id'],
            'order_id': repairement['order_id'],
            'repairement_id': repairement['DT'],
            'approve': repairement['approve'],
            'PID': 'unknown',
            'rate_id': self.get_retail_rates()[0]['id'],
            'client_secret': None,
            'payment_status': 'created',
        }
        save_repairment_shipment(data)

    def get_retail_rates(self):
        easypost_shipment = self.easypost_shipment
        if not easypost_shipment:
            return

        rates = []
        sh_rates = easypost_shipment['rates']
        shipment_id = easypost_shipment['id']
        for rate in sh_rates:
            retail_rate = rate['retail_rate']
            retail_rate = retail_rate and Decimal(retail_rate)
            rates.append({
                "id": rate['id'],
                "shipment_id": shipment_id,
                "carrier": rate['carrier'],
                "retail_rate": rate['retail_rate'],
                "retail_rate_value": retail_rate,
                "retail_currency": rate['retail_currency'],
                "est_delivery_days": rate['est_delivery_days'],
            })
        return sorted(rates, key=itemgetter('retail_rate_value'))

    def get_easypost_shipment(self, _id):
        if self._easypost_shipment:
            return self._easypost_shipment
        try:
            self._easypost_shipment = easypost.Shipment.retrieve(_id)
        except easypost.Error as e:
            capture_error(e, data={'shipment_id': _id}, errors_source='Easypost')
        return self._easypost_shipment

    def get_repairement_shipment(self, **kwargs):
        if self.repairement_shipment:
            return self.repairement_shipment

        kwargs = kwargs or self.kwargs.copy()
        if not kwargs:
            return

        if 'sh_id' in kwargs:
            self.repairement_shipment = get_repairment_shipment(value=kwargs['sh_id'])
        else:
            shipments = filter_repairement_shipments(**kwargs)
            if shipments:
                self.repairement_shipment = shipments[0]
        return self.repairement_shipment

    def get_label(self, rate_id=None):
        if self.label:
            return self.label

        if not self.id:
            return ValueError('Get or create shipment firstly')

        easypost_shipment = self.easypost_shipment
        if not easypost_shipment:
            return
        repairement_shipment = self.get_repairement_shipment(sh_id=self.id)
        upd_data = {}
        if easypost_shipment.get('postage_label'):
            self.label = label = easypost_shipment
        else:
            if rate_id:
                label = easypost_shipment.buy(rate={'id': rate_id})
                upd_data['rate_id'] = rate_id
            elif repairement_shipment:
                label = easypost_shipment.buy(rate={'id': self.repairement_shipment['rate_id']})
            else:
                label = easypost_shipment.buy(rate=easypost_shipment.lowest_rate(carriers=['USPS'], services=['First']))

            self.label = label

        if not repairement_shipment.get('label'):
            tracking_data = {
                'label': label['postage_label']['label_url'],
                'tracking_url': label['tracker']['public_url'],
                'tracking_number': label['tracking_code'],
            }
            self.update_repairment_shipment(
                payment_status='succeeded',
                **tracking_data,
                **upd_data
            )
            update_repairment_order(
                DT=repairement_shipment['repairement_id'],
                **tracking_data
            )
        return self.label

    def make_payment_intent(self, rate):
        repairement_shipment = self.get_repairement_shipment(sh_id=self.id)
        amount = rate['retail_rate']
        if repairement_shipment and repairement_shipment.get('PID') != 'unknown':
            payment = StripePayment(_id=repairement_shipment.get('PID'))
            if payment.intent['amount'] != int(Decimal(amount) * 100):
                payment = StripePayment(amount=amount)
        else:
            payment = StripePayment(amount=amount)
        self.payment = payment
        self.update_repairment_shipment(**{
            'sh_id': rate.get('shipment_id'),
            'PID': payment.intent['id'],
            'client_secret': payment.client_secret,
            'payment_status': 'created',
            'rate_id': rate['id']
        })
        return payment.intent['id'], payment.client_secret

    def update_repairment_shipment(self, **data):
        if 'sh_id' not in data:
            data['sh_id'] = self.id
        return update_repairment_shipment(**data)
