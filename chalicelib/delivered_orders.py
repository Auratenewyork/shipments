from datetime import date, timedelta
import requests
from .fulfil import client


def delivered_orders():
    end_date = date.today() - timedelta(days=11)
    start_date = date.today() - timedelta(days=18)
    Model = client.model('stock.shipment.out')
    fields = ['sales', 'planned_date']
    shipments = Model.search_read_all(
        domain=[["AND",
                 ["planned_date", ">=",
                  {"__class__": "date", "year": start_date.year,
                   "month": start_date.month, "day": start_date.day}],
                 ["planned_date", "<=",
                  {"__class__": "date", "year": end_date.year,
                   "month": end_date.month, "day": end_date.day}]
                 ]],
        order=None,
        fields=fields,
    )
    sale_ids = []
    for s in shipments:
        sale_ids.extend(s['sales'])
    sale_ids = list(set(sale_ids))
    Model = client.model('sale.sale')
    fields = ["reference", "party.name", "party.email"]
    sales = Model.search_read_all(
        domain=[["AND",
                 ["id", "in", sale_ids]
                 ]],
        order=None,
        fields=fields,
    )
    message = f'sales with planned date between {start_date} and {end_date}'
    return list(sales), message


def previous_week_range(date):
    start_date = date + timedelta(-date.weekday(), weeks=-1)
    end_date = date + timedelta(-date.weekday() - 1)
    return start_date, end_date


# def return_orders():
#     start_date, end_date = previous_week_range(date.today())
#     # start_date = date.today()
#     end_date = start_date - timedelta(weeks=12)
#     # start_date = date.today() - timedelta(weeks=12)
#     # end_date = date.today() - timedelta(weeks=24)
#
#     url = 'https://api.loopreturns.com/api/v1/warehouse/return/list'
#     headers = {'X-Authorization': '559172097439fae55481bda45dd5a0d25ec8a1d2'}
#     params = {"from": f"{start_date.year}-{start_date.month}-{start_date.day} 00:00:00",
#               "to": f"{end_date.year}-{end_date.month}-{end_date.day} 23:59:59"}
#     response = requests.get(url=url, params=params, headers=headers)
#     try:
#         r = response.json()
#     except:
#         pass
#     return r, f'return orders for dates {start_date} - {end_date}'


def return_orders():
    start_date, end_date = previous_week_range(date.today())
    url = 'https://api.loopreturns.com/api/v1/warehouse/return/list'
    headers = {'X-Authorization': '559172097439fae55481bda45dd5a0d25ec8a1d2'}
    params = {"from": f"{start_date.year}-{start_date.month}-{start_date.day} 00:00:00",
              "to": f"{end_date.year}-{end_date.month}-{end_date.day} 23:59:59"}
    response = requests.get(url=url, params=params, headers=headers)
    r = response.json()
    return r, f'return orders for dates {start_date} - {end_date}'

