from decimal import Decimal
import stripe

from flask import jsonify
from chalicelib import STRIPE_API_KEY, STRIPE_WH_KEY
from chalicelib.utils import capture_to_sentry, capture_error


class StripePayment:

    def __init__(self, amount=None, _id=None):
        stripe.api_key = STRIPE_API_KEY
        if amount:
            amount = int(Decimal(amount) * 100)
        self.amount = amount
        self.error = None
        self._client_secret = None
        self._indent = None
        self.id = _id

    @property
    def intent(self):
        if self.error:
            return

        if not self._indent:
            if self.id:
                self._intent = self.get_intent(self.id)
            else:
                self._intent = self.create_intent()
                self.id = self._intent['id']

        return self._intent

    @property
    def client_secret(self):
        if not self._client_secret:
            self._client_secret = self.intent.get('client_secret')
        return self._client_secret

    def create_intent(self):
        print('CREATE'*10)
        try:
            intent = stripe.PaymentIntent.create(
                amount=self.amount,
                currency='usd',
                payment_method_types=['card']
                # 'giropay',
                # 'eps',
                # 'p24',
                # 'sofort',
                # 'sepa_debit',
                # 'card',
                # 'bancontact',
                # 'ideal'
            )
            self._intent = intent
            return intent
        except Exception as e:
            self.error = jsonify(e)

    def get_intent(self, _id):
        try:
            intent = stripe.PaymentIntent.retrieve(_id)
            self._intent = intent
            return intent
        except Exception as e:
            self.error = jsonify(e)

    # TODO: check it
    def verify_payment_intent(self, signature, data):
        return True
        try:
            event = stripe.Webhook.construct_event(
                data, signature, endpoint_secret=STRIPE_WH_KEY
            )
        except stripe.error.SignatureVerificationError as e:
            self.error = e
            capture_to_sentry('SignatureVerificationError', data, signature=signature, errors_source='Stripe')
            return False
        except Exception as e:
            self.error = e
            capture_error(e, data=data, errors_source='Stripe')
            return False

        event = event.to_dict()
        self._intent = event['data']['object']
        return event['type'] == "payment_intent.succeeded"
