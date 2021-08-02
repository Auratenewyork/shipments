class TmallOrderConverter:
    """ To 'massage' tmall order data """

    default_emails = ("fyr125896@qq.com",)

    def __init__(self, order):
        self.order = order

    def make_fake_email(self, address):
        base_val = '{} {} {} {}'.format(
            address['name'],
            address['address1'],
            address['phone'],
            address['city']
        )
        return 'fake-{}@qq.com'.format(hash(base_val))

    def validate_email(self):
        for address_type in ('billing_address', 'shipping_address'):
            address = self.order.get(address_type)
            if address:
                email = address.get('email')
                if email in self.default_emails or 'fake' in email:
                    address['email'] = self.make_fake_email(address)

    def get_order(self):
        self.validate_email()
        return self.order
