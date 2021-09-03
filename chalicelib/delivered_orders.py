from datetime import date, timedelta
import requests
from jinja2 import Template

from chalicelib.email import send_email
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
#     start_date = date.today()
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

#
def return_orders():
    start_date, end_date = previous_week_range(date.today())
    url = 'https://api.loopreturns.com/api/v1/warehouse/return/list'
    headers = {'X-Authorization': '559172097439fae55481bda45dd5a0d25ec8a1d2'}
    params = {"from": f"{start_date.year}-{start_date.month}-{start_date.day} 00:00:00",
              "to": f"{end_date.year}-{end_date.month}-{end_date.day} 23:59:59"}
    response = requests.get(url=url, params=params, headers=headers)
    r = response.json()
    return r, f'return orders for dates {start_date} - {end_date}'


REPEARMENT_CASE = {
    'confirmed': {
        'subject':'About that warranty claim',
        'data':{
            'PREH':'Well received!',
            'HEADER':'We’re on it',
            'TEXT':'''
Just wanted to let you know your claim has been received and
is being reviewed by our team. We aim to keep your expectations the
highest, so rest assured knowing we take every claim very seriously.
Stay on the lookout for more updates from us (including a timeline
estimate) and of course reach out to care@auratenewyork.com with
any questions. Let’s fix this!<br/><br/>
Side note: We usually expect 3-4 weeks for something like this, but
will update you should it be outside of this timeframe.            
            ''',
        }
    },
    'accepted': {
        'subject':'Your repair has been accepted. ',
        'data':{
            'PREH':'What to do next. ',
            'HEADER':'Accepted Repair',
            'TEXT':'''
We’ve received your repair request, and agree that your piece could use 
some extra love. We want to handle this for you ASAP, as we take our jewelry 
quality super seriously.
            ''',
            'LINK': 'https://warrantyclaims.auratenewyork.com/info',
            'LINK_TEXT': 'NEXT STEPS',
        }
    },
    'declined': {
        'subject':'Your repair request. ',
        'data':{
            'PREH':'An update from us',
            'HEADER':'Repair Denied',
            'TEXT':'''
Hey there, we’re sorry your repair unfortunately did not meet the requirements 
needed. We know this isn’t the news you probably wanted, so here’s a little 
something from us for your next order. Use code ONUS for 15% off your next 
purchase. And to learn more about your repair status, <a href="https://auratenewyork.com/blogs/support/tagged/care-repair">click here</a>.<br/>
            ''',
        }
    },
    'delivered': {
        'subject': 'About that warranty claim',
        'data': {
            'PREH': 'Well delivered!',
            'HEADER': 'Delivered Email',
            'TEXT': '''
        ''',
        }
    },
    'reminder': {
        'subject': 'About that warranty claim',
        'data': {
            'PREH': 'Reminder!',
            'HEADER': 'Request Approved!',
            'LINK_TEXT': 'NEXT STEPS',
            'TEXT': '''
            We’ve approved your repair request and ready to receive it. 
            Use the link below and fill the tracking number to track your parcel.
        ''',
        }
    },
}

def send_repearment_email(email, case, NOTE=None, DT=None):
    from app import BASE_DIR
    info = REPEARMENT_CASE[case]
    if DT:
        LINK = f"https://warrantyclaims.auratenewyork.com/info?DT={DT}"
        info['data']['LINK'] = LINK
    template = Template(
        open(f'{BASE_DIR}/chalicelib/template/notification.html').read())

    content = template.render(**info['data'], **{"NOTE": NOTE},)

    send_email(info['subject'],
               content,
               email=email,
               dev_recipients=False,
               from_email='warrantyclaims@auratenewyork.com')