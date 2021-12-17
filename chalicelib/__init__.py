import os

RUBYHAS_HQ_STORAGE = 26
RUBYHAS_WAREHOUSE = 23
AURATE_WAREHOUSE = 4
AURATE_HQ_STORAGE = 3
AURATE_STORAGE_ZONE = 3
AURATE_OUTPUT_ZONE = 2
COMPANY = 1
TEST_CUSTOMER_ID = None
USD = 172
PRODUCTION = 9
FULFIL_API_URL = os.environ.get('FULFIL_API_ENDPOINT')
EASYPOST_API_KEY = os.environ.get('EASYPOST_API_KEY')
EASYPOST_URL = 'https://api.easypost.com/v2/shipments'


STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')
STRIPE_WH_KEY = os.environ.get('STRIPE_WH_KEY')

WAREHOUSE_TO_STORAGE = {
    RUBYHAS_WAREHOUSE: RUBYHAS_HQ_STORAGE,
    AURATE_WAREHOUSE: AURATE_HQ_STORAGE,
}

CLAIMS_PAGE_SIZE = 20

ENV = os.environ.get('ENV')

VENDOR_PATH = ''

DEV_EMAIL = 'aurate.api@gmail.com'

if ENV == 'local':  # Local development mode
    STRIPE_API_KEY = os.environ.get('STRIPE_TEST_API_KEY')
    STRIPE_WH_KEY = os.environ.get('STRIPE_TEST_WH_KEY')
    EASYPOST_API_KEY = os.environ.get('EASYPOST_TEST_API_KEY')
    TEST_CUSTOMER_ID = 2949941133409
    VENDOR_PATH = 'vendor'
    FULFIL_API_URL = os.environ.get('FULFIL_API_ENDPOINT')
