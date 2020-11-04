import os

from hashlib import sha1
import hmac
import base64
import requests
from fulfil_client import Client

from chalicelib.email import send_email

subdomain = os.environ.get('FULFIL_API_DOMAIN', 'aurate-sandbox')

def get_fulfil_model_url(param):
    FULFIL_API_URL = f'https://{subdomain}.fulfil.io/api/v2'
    return f'{FULFIL_API_URL}/model/{param}'

headers = {
    'X-API-KEY': os.environ.get('FULFIL_API_KEY'),
    'Content-Type': 'application/json'
}

client = Client(subdomain,
                os.environ.get('FULFIL_API_KEY', ''))


a = {"topic": "return", "trigger": "return.created", "id": "6042537", "state": "open", "created_at": "2020-10-29T16:07:31+00:00", "total": "0.13", "order_id": "26442641", "order_name": "#90507", "provider_order_id": "2773679177825", "order_number": "89507", "customer": "maxwell@auratenewyork.com", "address": {"name": "Maxwell Drut", "company": "Aurate", "address1": "257 central park west", "address2": "", "city": "New York", "state": "New York", "zip": "10024", "country": "United States", "country_code": "US", "phone": "2016550927"}, "currency": "USD", "return_product_total": "120.00", "return_discount_total": "119.88", "return_tax_total": "0.01", "return_total": "0.13", "return_credit_total": "0.00", "exchange_product_total": "0.00", "exchange_discount_total": "0.00", "exchange_tax_total": "0.00", "exchange_total": "0.00", "exchange_credit_total": "0.00", "gift_card": "0.00", "handling_fee": "0.00", "refund": "0.13", "refunds": [], "upsell": "0.00", "line_items": [{"line_item_id": "59160778", "provider_line_item_id": "5896375795809", "product_id": "4769333706849", "variant_id": "32725922578529", "sku": "AU1022E00000", "barcode": "", "title": "Gold Bamboo Huggie Earrings - 14K / Yellow / Single", "price": "120.00", "discount": "119.88", "tax": "0.01", "refund": "0.13", "returned_at": "2020-10-29 16:07:31", "exchange_variant": "", "return_reason": "Item was too big", "parent_return_reason": "Item didn't fit", "outcome": "default"}], "exchanges": [], "carrier": "", "tracking_number": "N/A", "label_status": "pending", "label_updated_at": "2020-10-29T16:07:33+00:00"}


b = {"topic": "return", "trigger": "return.updated", "id": "6042537", "state": "closed", "created_at": "2020-10-29T16:07:31+00:00", "total": "0.13", "order_id": "26442641", "order_name": "#90507", "provider_order_id": "2773679177825", "order_number": "89507", "customer": "maxwell@auratenewyork.com", "address": {"name": "Maxwell Drut", "company": "Aurate", "address1": "257 central park west", "address2": "", "city": "New York", "state": "New York", "zip": "10024", "country": "United States", "country_code": "US", "phone": "2016550927"}, "currency": "USD", "return_product_total": "120.00", "return_discount_total": "119.88", "return_tax_total": "0.01", "return_total": "0.13", "return_credit_total": "0.00", "exchange_product_total": "0.00", "exchange_discount_total": "0.00", "exchange_tax_total": "0.00", "exchange_total": "0.00", "exchange_credit_total": "0.00", "gift_card": "0.00", "handling_fee": "0.00", "refund": "0.13", "refunds": [{"gateway": "manual", "amount": "0.00"}], "upsell": "0.00", "line_items": [{"line_item_id": "59160778", "provider_line_item_id": "5896375795809", "product_id": "4769333706849", "variant_id": "32725922578529", "sku": "AU1022E00000", "barcode": "", "title": "Gold Bamboo Huggie Earrings - 14K / Yellow / Single", "price": "120.00", "discount": "119.88", "tax": "0.01", "refund": "0.13", "returned_at": "2020-10-29 16:07:31", "exchange_variant": "", "return_reason": "Item was too big", "parent_return_reason": "Item didn't fit", "outcome": "default"}], "exchanges": [], "carrier": "USPS", "tracking_number": "9461236895234251536060", "label_status": "new", "label_updated_at": "2020-10-29T16:07:35+00:00"}



def check_request_signature(request):
    try:
        webhook_secret = 'f5d0c396ba09b4c2'
        headers = request.headers
        signature = headers["X-Loop-Signature"]

        body = request.text
        body = body.replase('\n', '')

        hashed = hmac.new(webhook_secret, body)
        if hashed == signature:
            send_email(subject="loopreturns: signature, success", content="", dev_recipients=True)
        else:
            send_email(subject="loopreturns: signature, success", content=f"{body}\n{signature}", dev_recipients=True)
    except Exception as e:
        print("check_request_signature error")
        print(str(e.body))


def return_updated(body):
    return body

def return_created(body):
    errors = []
    Model = client.model('sale.sale')
    sale = Model.search_read_all(
        domain=[["AND", ["reference", "=", body['order_name']]]],
        order=None,
        fields=['id', 'lines'],
        # batch_size=5,
    )
    sale = sale.__next__()

    Model = client.model('sale.line')
    f_lines = Model.search_read_all(
        domain=[["AND", ["id", "in", sale['lines']]]],
        order=None,
        fields=['product', 'product.code', 'quantity'],
    )
    f_lines = list(f_lines)
    order_id = sale['id']
    url = f'{get_fulfil_model_url("sale.sale")}/{order_id}/return_order'

    lines = []
    for body_l in body['line_items']:
        ll = filter(lambda x: x['product.code'] == body_l['sku'], f_lines)
        if not ll:
            errors.append(f"Line not found {body_l}\n")
            continue
        line = ll.__next__()
        line_id = line['id']

        lines.append({
            "order_line_id": line_id,
            # Optional fields on line
            # ==================
            # "return_quantity": body_l[''],
            # defaults to the order line returnable quantity
            # "unit_price": "320.45",
            # defaults to the original order line unit price. Change this amount if the refund is not the full amount of the original order line.

            # If the return was created on an external returns platform,
            # the ID of the line
            "channel_identifier": body_l['line_item_id'],


            # "note": "tracking_number " + body['tracking_number'],
            "return_reason": body_l["return_reason"],  # Created if not exists
        })
    if not lines:
        errors.append("Can't create return, didn't find any line")
        return errors
    if body['exchanges']:
        Model = client.model('product.product')

    for i, item in enumerate(body['exchanges']):
        if len(lines) > i:
            product = Model.search_read_all(
                domain=[["AND", ["code", "=", item['sku']]]],
                order=None,
                fields=['id'],
            )
            product_id = product.__next__()['id']
            lines[i]['exchange_quantity'] = 1
            lines[i]['exchange_product'] = product_id
            lines[i]['exchange_unit_price'] = item['total']
            # # Exchange fields
            # # ==================
            # # +ve quantity of replacement item to ship to customer
            # "exchange_quantity": 1,
            # # ID of the product being sent.
            # # If replacement item is not specified, the same outbound item will be shipped.
            # "exchange_product": 1234,
            # # If the unit price is not specified, the unit price of the exchanged item is used.
            # "exchange_unit_price": "320.45",  # Unit price for outbound item
        else:
            errors.append(f"failed to add exchange for {item}\n "
                          f"there is more exchanges than returns")
            break
    payload = [{
            "channel_identifier":  body['id'],  # Unique identifier for the return in the channel. This will be used as idempotency key to avoid duplication.
            "reference": body["order_name"],  # Return order reference, RMA
            "lines": lines,
        }]

    response = requests.put(url, json=payload, headers=headers)
    return response, errors


def process_return(body):
    triggers = {'return.created': return_created, 'return.updated': return_updated}
    return triggers[body['trigger']](body)


def process_label(body):
    return body


def process_restock(body):
    return body


def process_request(request):
    triggers = {'return':process_return, 'label':process_label,
                'restock':process_restock}

    check_request_signature(request)

    body = request.json_body
    result = triggers[a['topic']](a)
    return result
