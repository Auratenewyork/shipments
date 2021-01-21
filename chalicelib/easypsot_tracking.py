from datetime import timedelta, date, datetime

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
        return []

    sale = Model.get(sale[0]['id'])
    if not sale['shipments']:
        return [None, None, None]
    Model = client.model('stock.shipment.out')
    shipment = Model.get(sale['shipments'][0])
    mto = (shipment['planned_date'] - shipment['sale_date'] > timedelta(days=17))
    tracking = []
    if mto:
        info = dict(
                message='Go, gold. Your ring is currently being casted and made. Hang tight for updates.',
                date=shipment['sale_date'].strftime('%m/%d/%Y'),
                status='Go, gold. Your ring is currently being casted and made. Hang tight for updates.',
                status_detail='Go, gold. Your ring is currently being casted and made. Hang tight for updates.',
            )
        tracking.append(info)
        after_3_days = date_after_some_workdays(shipment['sale_date'])
        if datetime.date.today() >= after_3_days:
            info = dict(
                message='Your ring was finished being made and our team of helicopter moms are making sure it looks just as perfect as promised',
                date=after_3_days.strftime('%m/%d/%Y'),
                status='Your ring was finished being made and our team of helicopter moms are making sure it looks just as perfect as promised',
                status_detail='Your ring was finished being made and our team of helicopter moms are making sure it looks just as perfect as promised',
            )
            tracking.append(info)
        after_14_days = shipment['sale_date'] + timedelta(days=14)
        if datetime.date.today() >= after_14_days:
            info = dict(
                message='Your order is on the go. It’s being packed and ready to be shipped out!',
                date=after_14_days.strftime('%m/%d/%Y'),
                status='Your order is on the go. It’s being packed and ready to be shipped out!',
                status_detail='Your order is on the go. It’s being packed and ready to be shipped out!',
            )
            tracking.append(info)
    else:
        info = dict(
            message='Your order is on the go. It’s being packed and ready to be shipped out!',
            date=shipment['sale_date'].strftime('%m/%d/%Y'),
            status='Your order is on the go. It’s being packed and ready to be shipped out!',
            status_detail='Your order is on the go. It’s being packed and ready to be shipped out!',
        )
        tracking.append(info)
    estimated_date = dict(
        day=shipment['planned_date'].strftime('%d'),
        weekday=shipment['planned_date'].strftime('%A'),
        month=shipment['planned_date'].strftime('%B'),
    )
    return tracking, estimated_date, sale['number']


def dates_with_passed_some_work_days(wd_number=3, excluded=(6, 7)):
    d = date.today()
    wd = 0
    date_list = []
    while wd <= wd_number + 1:
        d -= datetime.timedelta(days=1)
        if d.isoweekday() not in excluded:
            wd += 1
        if wd == wd_number:
            date_list.append(d)
    return date_list


def fulfill_mto_candidates(report_date):
    Shipment = client.model('stock.shipment.out')
    fields = ['sales', 'order_numbers', 'sale_date', 'planned_date']

    # domain = [["AND",["create_date","=",{"__class__":"datetime","year":report_date.year,"month":11,"day":26,"hour":22,"minute":0,"second":0,"microsecond":0}],["state", "!=", "done"], ["state", "!=", "cancel"]]]
    shipments = Shipment.search_read_all(
        domain=[['AND', [["state", "!=", "done"], ["state", "!=", "cancel"],
                         ["create_date", ">", {"__class__": "datetime",
                                               "year": report_date.year,
                                               "month": report_date.month,
                                               "day": report_date.day,
                                               "hour":0,
                                               "minute":0,
                                               "second":0,
                                               "microsecond":0}],
                         ["create_date", "<", {"__class__": "datetime",
                                               "year": report_date.year,
                                               "month": report_date.month,
                                               "day": report_date.day,
                                               "hour": 23,
                                               "minute": 59,
                                               "second": 59,
                                               "microsecond": 0}],
                        ]]],
        order=None,
        fields=fields
    )
    candidates = filter(lambda item: item['planned_date'] - item['sale_date'] > timedelta(days=17), shipments)
    candidates = list(candidates)
    return candidates


def get_n_days_old_orders(days):
    d = date.today()
    if d.isoweekday() in (6, 7):
        return "We are not sending emails on weekends!???"
    d_list = dates_with_passed_some_work_days(days)
    candidates = []
    for d in d_list:
        c = fulfill_mto_candidates(d)
        candidates.extend(c)
    sale_ids = []
    for c in candidates:
        sale_ids.extend(c['sales'])

    Sale = client.model('sale.sale')
    fields = ['party.email', 'reference']

    sales = Sale.search_read_all(
        domain=['AND', ['id', 'in', sale_ids]],
        order=None,
        fields=fields)
    sales = list(sales)
    for sale in sales:
        for c in candidates:
            if sale['id'] in c['sales']:
                sale['planned_date'] = c['planned_date']
                break


    return sales
    # emails = [item['party.email'] for item in sales]
    # return emails

