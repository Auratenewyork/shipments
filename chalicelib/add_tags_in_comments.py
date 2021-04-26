from datetime import datetime, timedelta
from chalicelib.fulfil import client


def get_customer_shipments():
    d = datetime.utcnow() - timedelta(days=300)
    filter_ = ['AND', ["create_date", ">",
                      {"__class__": "datetime", "year": d.year,
                       "month": d.month, "day": d.day,
                       "hour": d.hour,
                       "minute": d.minute, "second": d.second,
                       "microsecond": 0}],
              ]
    filter_ = ['AND', ['id', '=', 50981]]
    fields = ['sales', 'shipping_instructions', 'rec_name', 'warehouse', 'contents_explanation']
    Shipment = client.model('stock.shipment.out')
    shipments = Shipment.search_read_all(
        domain=filter_,
        order=None,
        fields=fields
    )
    return list(shipments)


def get_sales_by_ids(ids):
    filter_ = ['AND', ["id", "in", ids],]
    fields = ['total_amount']
    Sale = client.model('sale.sale')
    sales = Sale.search_read_all(
        domain=filter_,
        order=None,
        fields=fields
    )
    return list(sales)


def update_shipment(shipment, changes):
    model = client.model('stock.shipment.out')
    model.write([shipment['id']], changes)


def add_AOV_comment(shipments):
    for shipment in shipments:
        if shipment['contents_explanation']:
            contents_explanation = f"{shipment['contents_explanation']}" \
                                    f"\r\nAOV"
        else:
            contents_explanation = 'AOV'
        update_shipment(shipment, {'contents_explanation': contents_explanation})


def add_AOV_tag_to_shipments():
    shipments = get_customer_shipments()
    sale_ids = []
    for item in shipments:
        sale_ids.extend(item['sales'])
    sales = get_sales_by_ids(sale_ids)
    for sh in shipments:
        sh['total'] = 0
        for sa in sales:
            if sa['id'] in sh['sales']:
                sh['total'] += sa['total_amount']
                continue
    AOV_candidates = [shipment for shipment in shipments if shipment['total'] >= 2000]
    add_AOV_comment(AOV_candidates)
