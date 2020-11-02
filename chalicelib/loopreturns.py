a = {"topic": "return", "trigger": "return.created", "id": "6042537", "state": "open", "created_at": "2020-10-29T16:07:31+00:00", "total": "0.13", "order_id": "26442641", "order_name": "#90507", "provider_order_id": "2773679177825", "order_number": "89507", "customer": "maxwell@auratenewyork.com", "address": {"name": "Maxwell Drut", "company": "Aurate", "address1": "257 central park west", "address2": "", "city": "New York", "state": "New York", "zip": "10024", "country": "United States", "country_code": "US", "phone": "2016550927"}, "currency": "USD", "return_product_total": "120.00", "return_discount_total": "119.88", "return_tax_total": "0.01", "return_total": "0.13", "return_credit_total": "0.00", "exchange_product_total": "0.00", "exchange_discount_total": "0.00", "exchange_tax_total": "0.00", "exchange_total": "0.00", "exchange_credit_total": "0.00", "gift_card": "0.00", "handling_fee": "0.00", "refund": "0.13", "refunds": [], "upsell": "0.00", "line_items": [{"line_item_id": "59160778", "provider_line_item_id": "5896375795809", "product_id": "4769333706849", "variant_id": "32725922578529", "sku": "AU1022E00000", "barcode": "", "title": "Gold Bamboo Huggie Earrings - 14K / Yellow / Single", "price": "120.00", "discount": "119.88", "tax": "0.01", "refund": "0.13", "returned_at": "2020-10-29 16:07:31", "exchange_variant": "", "return_reason": "Item was too big", "parent_return_reason": "Item didn't fit", "outcome": "default"}], "exchanges": [], "carrier": "", "tracking_number": "N/A", "label_status": "pending", "label_updated_at": "2020-10-29T16:07:33+00:00"}

def check_request_signature(request):
    headers = request.headers
    body = request.json_body

def process_return(body):
    pass

def process_label(body):
    pass

def process_restock(body):
    pass


def process_request(request):
    triggers = {'return':process_return, 'label':process_label,
                'restock':process_restock}


    check_request_signature(request)

    body = request.json_body
    triggers[a['topic']](body)
