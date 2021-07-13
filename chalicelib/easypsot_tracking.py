from datetime import timedelta, date, datetime

from chalicelib.dynamo_operations import get_shopify_sku_info
from .fulfil import client
import easypost
from chalicelib import EASYPOST_API_KEY, EASYPOST_URL

import datetime
from .common import date_after_some_workdays, dates_with_passed_some_work_days

def get_shipments(sale_reference):
    Model = client.model('sale.sale')
    sales = Model.search_read_all(
        domain=['AND', [["reference", "=", sale_reference]]],
        order=[["sale_date","DESC"]],
        fields=[]
    )
    sales = list(sales)
    if not sales:
        return [], None, None
    sale = Model.get(sales[0]['id'])

    if not sale['shipments']:
        if len(sales) > 1:
            sale = Model.get(sales[1]['id'])
            if not sale['shipments']:
                return [], None, None
        else:
            return [], None, None
    Model = client.model('stock.shipment.out')
    shipments = []
    for shipment_id in sale['shipments']:
        shipment = Model.get(shipment_id)
        shipments.append(shipment)

    lines = []
    Model = client.model('sale.line')
    for line_id in sale['lines']:
        line = Model.get(line_id)
        lines.append(line)

    return shipments, lines, sale['number']


def fulfill_tracking(shipment):

    mto = (shipment['planned_date'] - shipment['sale_date'] > timedelta(days=17))

    tracking = []
    if mto:
        info = dict(
                message="We have a liftoff. Your order has been received. Hang tight for updates.",
                date=shipment['sale_date'].strftime('%m/%d/%Y'),
            )
        tracking.append(info)
        after_3_days = date_after_some_workdays(shipment['sale_date'], wd_number=10)
        if datetime.date.today() >= after_3_days:
            info = dict(
                message='Go, gold. Your gold is currently being casted and made. Hang tight for updates.',
                date=after_3_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)

        after_12_days = date_after_some_workdays(shipment['sale_date'], wd_number=15)
        if datetime.date.today() >= after_12_days:
            info = dict(
                message='Your gold was finished being made and our team of helicopter moms are making sure it looks just as perfect as promised.',
                date=after_12_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)

        after_17_days = date_after_some_workdays(shipment['sale_date'], wd_number=20)
        if datetime.date.today() >= after_17_days and shipment['all_moves'][0]['vermeil']:
            info = dict(
                message='Your Vermeil piece is ready to be dipped in 14k gold and finished with a top coat to ensure durability and shine.',
                date=after_17_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)
        if shipment['shipping_instructions'] != None and 'Planned date delayed' in shipment['shipping_instructions']:
            info = dict(
                message='Your shipping timeline has been updated.',
                date=(after_17_days + timedelta(days=2)).strftime('%m/%d/%Y'),
            )
            tracking.append(info)
    else:
        info = dict(
            message='Your order is on the go. It’s being packed and ready to be shipped out!',
            date=shipment['sale_date'].strftime('%m/%d/%Y'),
        )
        tracking.append(info)
    estimated_date = dict(
        day=shipment['planned_date'].strftime('%d'),
        weekday=shipment['planned_date'].strftime('%A'),
        month=shipment['planned_date'].strftime('%B'),
    )
    return tracking, estimated_date, shipment['number']


def _fulfill_tracking_(shipment): # delete this!!!!!!!

    mto = (shipment['planned_date'] - shipment['sale_date'] > timedelta(days=17))

    tracking = []
    if mto:
        info = dict(
                message="We have a liftoff. Your order has been received. Hang tight for updates.",
                date=shipment['sale_date'].strftime('%m/%d/%Y'),
            )
        tracking.append(info)
        after_3_days = date_after_some_workdays(shipment['sale_date'], wd_number=10)
        if True:
        # if datetime.date.today() >= after_3_days:
            info = dict(
                message='Go, gold. Your gold is currently being casted and made. Hang tight for updates.',
                date=after_3_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)

        after_12_days = date_after_some_workdays(shipment['sale_date'], wd_number=15)
        if True:
        # if datetime.date.today() >= after_12_days:
            info = dict(
                message='Your gold was finished being made and our team of helicopter moms are making sure it looks just as perfect as promised.',
                date=after_12_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)

        after_17_days = date_after_some_workdays(shipment['sale_date'], wd_number=20)
        if True and shipment['all_moves'][0]['vermeil']:
        # if datetime.date.today() >= after_17_days and shipment['all_moves'][0]['vermeil']:
            info = dict(
                message='Your Vermeil piece is ready to be dipped in 14k gold and finished with a top coat to ensure durability and shine.',
                date=after_17_days.strftime('%m/%d/%Y'),
            )
            tracking.append(info)
        if True:
            info = dict(
                message='Your shipping timeline has been updated.',
                date=(after_17_days + timedelta(days=2)).strftime('%m/%d/%Y'),
            )
            tracking.append(info)

    else:
        info = dict(
            message='Your order is on the go. It’s being packed and ready to be shipped out!',
            date=shipment['sale_date'].strftime('%m/%d/%Y'),
        )
        tracking.append(info)
    estimated_date = dict(
        day=shipment['planned_date'].strftime('%d'),
        weekday=shipment['planned_date'].strftime('%A'),
        month=shipment['planned_date'].strftime('%B'),
    )
    return tracking, estimated_date, shipment['number']

def get_sale_lines_info(candidates):
    sale_ids = []
    for c in candidates:
        sale_ids.extend(c['sales'])
    Sale = client.model('sale.sale')
    fields = ['lines']

    sales = Sale.search_read_all(
        domain=[['AND', ["id", "in", sale_ids],]],
        order=None,
        fields=fields
    )
    line_ids = []
    for s in sales:
        line_ids.extend(s['lines'])
    fields = ['product', 'metadata', 'sale']
    Line = client.model('sale.line')
    lines = Line.search_read_all(
        domain=[['AND', ["id", "in", line_ids],]],
        order=None,
        fields=fields
    )
    return list(lines)


def fulfill_mto_candidates(report_date):
    Shipment = client.model('stock.shipment.out')
    fields = ['sales', 'order_numbers', 'sale_date', 'planned_date', 'inventory_moves', 'tracking_number']

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


def late_order_candidates(planned_date):
    Shipment = client.model('stock.shipment.out')
    fields = ['sales', 'order_numbers', 'sale_date', 'planned_date',
              'inventory_moves', 'tracking_number', 'shipping_instructions']
    shipments = Shipment.search_read_all(
        domain=[[
            "AND",
            [
                "planned_date", "=", {
                "__class__": "date",
                "year": planned_date.year,
                "day": planned_date.day,
                "month": planned_date.month,
            }
            ],
            ["state", "in", ["waiting", "packed", "assigned"]]
        ]],
        order=None,
        fields=fields
    )
    # filter exchange orders
    candidates = filter(lambda item: 'exe' not in item.get('order_numbers', ''),
                        shipments)
    candidates = list(candidates)
    for item in candidates:
        item['mto'] = item['planned_date'] - item['sale_date'] > timedelta(days=17)
    return candidates


SKIP_ORDERS = ['SO512143', 'SO512090', 'SO512070', 'SO511906', 'SO511774', 'SO508569', 'SO508561', 'SO508555', 'SO508512', 'SO508510', 'SO508510', 'SO508481', 'SO508476', 'SO508459', 'SO508436', 'SO508428', 'SO508414', 'SO508406', 'SO508368', 'SO508114', 'SO508083', 'SO507575', 'SO507046', 'SO507004', 'SO506964', 'SO505136', 'SO505050', 'SO505958', 'SO505732', 'SO505683', 'SO505592', 'SO505514', 'SO508112', 'SO507300', 'SO507254', 'SO507248', 'SO507246', 'SO507240', 'SO507239', 'SO507237', 'SO507214', 'SO507201', 'SO507192', 'SO507186', 'SO507181', 'SO507178', 'SO505761', 'SO512025', 'SO512023', 'SO512013', 'SO511980', 'SO511967', 'SO511953', 'SO511946', 'SO511946', 'SO508728', 'SO508703', 'SO508694', 'SO508678', 'SO508674', 'SO508663', 'SO508196', 'SO508174', 'SO507835', 'SO507823', 'SO507808', 'SO507795', 'SO507785', 'SO507759', 'SO507738', 'SO507738', 'SO507713', 'SO507700', 'SO507692', 'SO507689', 'SO507662', 'SO507630', 'SO507606', 'SO507575', 'SO507572', 'SO507563', 'SO507531', 'SO507517', 'SO507498', 'SO507494', 'SO507494', 'SO507494', 'SO507494', 'SO507494', 'SO507460', 'SO507426', 'SO507409', 'SO507374', 'SO507305', 'SO507296', 'SO507234', 'SO512064', 'SO509024', 'SO508847', 'SO508839', 'SO508839', 'SO508823', 'SO508807', 'SO508804', 'SO508753', 'SO508611', 'SO508332', 'SO506439', 'SO506211', 'SO506130', 'SO506062', 'SO505979', 'SO508948', 'SO508894', 'SO508395', 'SO508326', 'SO506639', 'SO506628', 'SO509029', 'SO509023', 'SO509023', 'SO509007', 'SO509007', 'SO509000', 'SO509000', 'SO508987', 'SO508987', 'SO508783', 'SO508547', 'SO508503', 'SO508471', 'SO508438', 'SO508434', 'SO508145', 'SO508130', 'SO508105', 'SO508061', 'SO508056', 'SO508037', 'SO508035', 'SO508020', 'SO508020', 'SO508020', 'SO508010', 'SO507974', 'SO507973', 'SO507954', 'SO507946', 'SO507943', 'SO507938', 'SO507916', 'SO507891', 'SO507880', 'SO507867', 'SO507853', 'SO507732', 'SO506563', 'SO506555', 'SO506555', 'SO506540', 'SO506534', 'SO506525', 'SO509285', 'SO509285', 'SO509285', 'SO509242', 'SO509230', 'SO509219', 'SO509191', 'SO509170', 'SO509170', 'SO509130', 'SO509130', 'SO509117', 'SO509112', 'SO509112', 'SO509109', 'SO509105', 'SO509105', 'SO509102', 'SO509095', 'SO509059', 'SO509059', 'SO509051', 'SO509047', 'SO509047', 'SO509039', 'SO509039', 'SO508933', 'SO508933', 'SO508869', 'SO508867', 'SO508183', 'SO508208', 'SO508158', 'SO508101', 'SO508091', 'SO508089', 'SO508003', 'SO508003', 'SO506648', 'SO506648', 'SO506648', 'SO506648', 'SO506648', 'SO50824', 'SO50824', 'SO50824', 'SO508244', 'SO507588', 'SO506844', 'SO506716', 'SO506712', 'SO509329', 'SO508860', 'SO508836', 'SO512081', 'SO509451', 'SO509433', 'SO509392', 'SO509374', 'SO509355', 'SO507039', 'SO507036', 'SO507005', 'SO509537', 'SO509515', 'SO509504', 'SO509489', 'SO509295', 'SO509275', 'SO509264', 'SO509258', 'SO509257', 'SO509222', 'SO509161', 'SO509020', 'SO509020', 'SO509008', 'SO508104', 'SO508104', 'SO507438', 'SO507136', 'SO511339', 'SO508723', 'SO508723', 'SO510142', 'SO509639', 'SO509636', 'SO509636', 'SO509635', 'SO509631', 'SO509626', 'SO509622', 'SO509618', 'SO509596', 'SO509584', 'SO509579', 'SO509237', 'SO509207', 'SO509190', 'SO509171', 'SO509158', 'SO509111', 'SO509099', 'SO509069', 'SO509047', 'SO507265', 'SO507243', 'SO507220',]


def get_n_days_old_orders(days, vermeil=False, late=False):
    d = date.today()
    if d.isoweekday() in (6, 7):
        return "We are not sending emails on weekends!???"
    d_list = dates_with_passed_some_work_days(days)
    candidates = []
    for d in d_list:
        if late:
            c = late_order_candidates(d)
        else:
            c = fulfill_mto_candidates(d)
        candidates.extend(c)
    # we are not sending emails if tracking number is printed
    candidates = filter(lambda item: not item['tracking_number'], candidates)
    candidates = filter(lambda item: 'return' not in item.get('order_numbers', ''),
                        candidates)
    candidates = filter(lambda item:
                        not any(number in item['order_numbers'] for number in SKIP_ORDERS),
                        candidates)
    candidates = list(candidates)

    lines = get_sale_lines_info(candidates)
    candidates = add_product_info(candidates, lines)
    if vermeil:
        candidates = filter_vermeil_candidates(candidates)

    sale_ids = []
    for c in candidates:
        sale_ids.extend(c['sales'])

    Sale = client.model('sale.sale')
    fields = ['party.email', 'party.name', 'reference']

    sales = Sale.search_read_all(
        domain=['AND', ['id', 'in', sale_ids]],
        order=None,
        fields=fields)
    sales = list(sales)
    for sale in sales:
        sale['c'] = []
        for c in candidates:
            if sale['id'] in c['sales']:
                sale['planned_date'] = c['planned_date']
                sale['c'].append(c)
    return sales


def _get_n_days_old_orders_(sale_reference, vermeil=False):
    """This is for tests, so move or remove this after testing finished"""
    # d = date.today()
    # if d.isoweekday() in (6, 7):
    #     return "We are not sending emails on weekends!???"
    # d_list = dates_with_passed_some_work_days(days)
    # candidates = []
    # for d in d_list:
    #     c = fulfill_mto_candidates(d)
    #     candidates.extend(c)

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
    shipment_ids = sale['shipments']

    fields = ['sales', 'order_numbers', 'sale_date', 'planned_date', 'inventory_moves']
    Shipment = client.model('stock.shipment.out')
    shipments = Shipment.search_read_all(
        domain=['AND', [["id", 'in', shipment_ids]]],
        order=None,
        fields=fields
    )
    candidates = filter(lambda item: item['planned_date'] - item['sale_date'] > timedelta(days=17), shipments)
    candidates = list(candidates)
    candidates = add_product_info(candidates)
    if vermeil:
        candidates = filter_vermeil_candidates(candidates)

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
        sale['c'] = []
        for c in candidates:
            if sale['id'] in c['sales']:
                sale['planned_date'] = c['planned_date']
                sale['c'].append(c)
    return sales
    # emails = [item['party.email'] for item in sales]
    # return emails


def add_shopify_product_info(m):
    sku = m['product.code']
    shopify_info = get_shopify_sku_info(sku)
    if shopify_info:
        m['name'] = shopify_info['name']
        m['image'] = shopify_info['image']
        m['title'] = shopify_info['title']
    else:
        m['name'] = m['product.name']
        if m['product.media_json']:
            m['image'] = m['product.media_json'][0]['url']
        else:
            m['image'] = None
        m['title'] = None
    return m


def add_product_info(candidates, lines=[]):
    VERMAIL_CODES = 'ABCD'
    moves_ids = []
    for c in candidates:
        moves_ids.extend(c['inventory_moves'])

    Move = client.model('stock.move')
    fields = ['product.code', 'quantity', 'product.media_json', 'product.attributes_json', 'product', 'product.name']
    moves = Move.search_read_all(
        domain=['AND', ['id', 'in', moves_ids]],
        order=None,
        fields=fields)

    moves = list(moves)
    for c in candidates:
        all_moves = []
        for one_move in c['inventory_moves']:
            for m in moves:
                if one_move == m['id']:
                    if len(m['product.code']) > 10:
                        m['vermeil'] = (m['product.code'][6] in VERMAIL_CODES)
                    else:
                        m['vermeil'] = False
                    m = add_shopify_product_info(m)
                    for line in lines:
                        if line['sale'] in c['sales'] and line['product'] == m['product']:
                            if line['metadata']:
                                metadata = []
                                for key, value in line['metadata'].items():
                                    if key[0] != '_' and value:
                                        metadata.append(f'{key}: {value}')
                                m['metadata'] = metadata
                            break
                    all_moves.append(m)
                    break
        c['all_moves'] = all_moves

    return candidates


def filter_vermeil_candidates(candidates):

    new_candidates = []
    for c in candidates:
        all_moves = []
        for m in c['all_moves']:
            if m['vermeil']:
                all_moves.append(m)
        if all_moves:
            c['all_moves'] = all_moves
            new_candidates.append(c)
    return new_candidates