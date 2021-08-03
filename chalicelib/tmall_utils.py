import os
import pytz

from datetime import datetime


FULFIL_API_DOMAIN = os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox')


def get_tmall_channel_id():
    if 'sandbox' in FULFIL_API_DOMAIN:
        return 17
    return 16


class TmallOrderConverter:
    """ To 'massage' tmall order data """

    default_emails = ("fyr125896@qq.com",)

    def __init__(self, order):
        self.order = order

    def make_fake_email(self, address):
        base_val = '{} {} {} {}'.format(
            address.get('name'),
            address.get('address1'),
            address.get('phone'),
            address.get('city')
        )
        return 'email{}@qq.com'.format(hash(base_val))

    def validate_email(self):
        faked_email = None
        for address_type in ('billing_address', 'shipping_address'):
            address = self.order.get(address_type)
            if address:
                email = address.get('email')
                if email in self.default_emails:
                    faked_email = self.make_fake_email(address)
                    address['email'] = self.make_fake_email(address)
        customer = self.order['customer']
        contact = customer['contacts'][0]
        email = contact[1]
        if email in self.default_emails:
            faked_email = faked_email or self.make_fake_email(contact)
            contact[1] = faked_email

    def update_dates(self):
        # convert China time to utc
        confirmed_at = self.order['confirmed_at']
        tz = pytz.timezone('Asia/Shanghai')
        china_confirmed_at = datetime.strptime(confirmed_at, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
        utc_confirmed_at = china_confirmed_at.astimezone(pytz.timezone('utc'))
        self.order['confirmed_at'] = utc_confirmed_at.strftime('%Y-%m-%d %H:%M:%S')

    def get_order(self):
        self.update_dates()
        self.validate_email()
        return self.order
