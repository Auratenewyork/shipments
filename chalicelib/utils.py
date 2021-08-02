import csv
from datetime import datetime
import json
import os

from chalicelib.email import send_email
import sentry_sdk
from sentry_sdk import capture_message, configure_scope, capture_exception


ROLLBACK_DIR = os.environ.get('ROLLBACK_DIR', 'rollback')
FULFIL_API_DOMAIN = os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox')
ENV = os.environ.get('ENV', 'local')
EVIRONMENT = '{}-{}'.format(FULFIL_API_DOMAIN, ENV)


def make_rollbaсk_filename(filename, server_name='', suffix='json'):
    ntime = datetime.now().strftime("%m_%d_%Y_%H:%M")
    return os.path.join(ROLLBACK_DIR, f"{ntime}_{filename}_{server_name}.{suffix}")


def fill_rollback_file(data, filename, access_mode='w', server_name=''):
    filename = make_rollbaсk_filename(filename, server_name=server_name)
    with open(filename, access_mode) as out:
        data = json.dumps(data, indent=4, sort_keys=True)
        print(data, file=out)


def fill_csv_file(data, filename, access_mode='w', server_name=''):
    filename = make_rollbaсk_filename(filename, server_name=server_name, suffix='csv')
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


def capture_to_sentry(message, data=None, email=None, **tags):
    tags.setdefault('environment', EVIRONMENT)
    with configure_scope() as scope:
        for tag, value in tags.items():
            scope.set_tag(tag, value)
        if data:
            sentry_sdk.set_context('DATA', data)
        capture_message(message, scope=scope)
    if email:
        send_email(message, str(data), email=email)


def capture_error(error, data=None, email=None, **tags):
    tags.setdefault('environment', EVIRONMENT)
    with configure_scope() as scope:
        for tag, value in tags.items():
            scope.set_tag(tag, value)
        if data:
            sentry_sdk.set_context('DATA', data)
        capture_exception(error, scope=scope)
    if email:
        send_email(error, str(data), email=email)
