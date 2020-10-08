from datetime import timedelta

from .fulfil import client
import easypost
from chalicelib import EASYPOST_API_KEY, EASYPOST_URL

import datetime


def date_after_some_workdays(d, wd_number=3, excluded=(6, 7)):
    wd = 0
    while wd != wd_number:
        if d.isoweekday() not in excluded:
            wd += 1
        d += datetime.timedelta(days=1)
    return d


def fulfill_tracking(sale_reference):
    Model = client.model('sale.sale')
    sale = Model.search_read_all(
        domain=['AND', [["reference", "=", sale_reference]]],
        order=None,
        fields=[]
    )
    sale = list(sale)
    if not sale:
        raise Exception("Not found")

    sale = Model.get(sale[0]['id'])
    Model = client.model('stock.shipment.out')
    shipment = Model.get(sale['shipments'][0])
    mto = (shipment['planned_date'] - shipment['sale_date'] > timedelta(days=17))
    tracking = []
    info = dict(
            message='Order received',
            date=shipment['sale_date'].isoformat(),
            status='order received',
            status_detail='Order received',
        )
    tracking.append(info)
    if mto:
        after_3_days = date_after_some_workdays(shipment['sale_date'])
        if datetime.date.today() >= after_3_days:
            info = dict(
                message='your order is being made',
                date=after_3_days.isoformat(),
                status='your order is being made',
                status_detail='your order is being made',
            )
            tracking.append(info)
        after_14_days = shipment['sale_date'] + timedelta(days=14)
        if datetime.date.today() >= after_14_days:
            info = dict(
                message='your order is polishing',
                date=after_14_days.isoformat(),
                status='your order is polishing',
                status_detail='your order is polishing',
            )
            tracking.append(info)
    return tracking
