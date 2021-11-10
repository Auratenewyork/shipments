from functools import wraps

from sentry_sdk import capture_exception, configure_scope
from flask import abort
from chalicelib.utils import get_authorization
from chalicelib.shopify import filter_shopify_customer


def try_except(*tag_list, **tags):
    def decorated_function(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                with configure_scope() as scope:
                    scope.transaction = func.__name__
                    for key, value in tags.items():
                        scope.set_tag(key, value)
                    capture_exception(e, scope=scope)
        return wrapper
    return decorated_function


def auth_customer_by_email(app):
    def decorated_function(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print('='*100)
            email = None
            request = app.current_request
            if request.method in ('POST', 'PUT'):  # for compatibility
                email = request.json_body.get('email')
            elif 'Authorization' in request.headers:
                email = get_authorization(request.headers.get('authorization', ''))

            if not email:
                abort(401)

            customer = filter_shopify_customer(email=email)
            if not customer:
                abort(404, 'User does not exist')
            return func(customer, *args, **kwargs)
        return wrapper
    return decorated_function
