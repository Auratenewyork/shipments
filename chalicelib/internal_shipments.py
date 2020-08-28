import collections
import datetime

from chalicelib.email import send_email
from chalicelib.rubyhas import get_item_quantity
from .fulfil import client, create_internal_shipment


class ProcessInternalShipment:
    type_filters = dict(
        engraving=[["state", "in", ["processing"]]],
        global_order=[["reference", "like", "GE%"],
                      ["state", "in", ["processing"]]],
        sets_and_bundles=[["state", "in", ["processing"]]]
    )

    def __init__(self, after_create_time, sale_order_type):
        self.after_create_time = after_create_time
        self.sale_order_type = sale_order_type
        self.filter = self.get_filter()
        self.ruby_quantities = collections.defaultdict(int)
        self.errors = []
        self.success = False

    def process_internal_shipments(self):
        sales = self.collect_information()
        available_products = self.collect_available_products(sales)
        self.check_products(available_products)
        products = [{'id': key, 'quantity': value}
                    for key, value in available_products.items()]
        if products:
            self.create_shipment(products)
            self.send_email()

    def collect_information(self):
        shipments = self.get_customer_shipments()
        self.sale_ids = []
        for item in shipments:
            self.sale_ids.extend(item['sales'])
        sales = self.get_sales_orders()
        line_ids = []
        for sale in sales:
            line_ids.extend(sale['lines'])
        lines = self.get_order_lines(line_ids)
        for sale in sales:
            sale['lines_info'] = []
            for sale_line_id in sale['lines']:
                for line in lines:
                    if sale_line_id == line['id']:
                        sale['lines_info'].append(line)
        if 'engraving' == self.sale_order_type:
            sales = self.left_engraving_sales(sales)
        elif 'sets_and_bundles' == self.sale_order_type:
            sales = self.left_sets_and_bundles(sales)

        for sale in sales:
            for line in sale['lines_info']:
                if line['product.code'] != 'SHIPPING':
                    line['ruby_quantity'] = get_item_quantity(line['product.code'])
                    if line['ruby_quantity']:
                        self.ruby_quantities[line['product.code']] = line['ruby_quantity']
                else:
                    line['ruby_quantity'] = 0
        return sales

    def collect_available_products(self, sales):
        products = collections.defaultdict(int)
        for sale in sales:
            for line in sale['lines_info']:
                if not line['ruby_quantity']:
                    continue
                elif line['quantity'] < line['ruby_quantity']:
                    products[line['product']] += line['quantity']
                else:
                    products[line['product']] += line['ruby_quantity']
        return products

    def check_products(self, products):
        for key, value in products.items():
            if self.ruby_quantities[key] < value:
                send_email(
                    "!!!IMPORTANT: Internal shipments (product check result)",
                    f"problem with {key} created internal shipment "
                    f"with values ({value}) more then available "
                    f"on rubyhas {self.ruby_quantities[key]}",
                    dev_recipients=True)
                products[key] = self.ruby_quantities[key]

    def get_filter(self):
        d = self.after_create_time
        filter_phrase = ['AND', ["create_date", ">",
                                 {"__class__": "datetime", "year": d.year,
                                  "month": d.month, "day": d.day,
                                  "hour": d.hour,
                                  "minute": d.minute, "second": d.second,
                                  "microsecond": 0}]]
        if self.type_filters[self.sale_order_type]:
            filter_phrase.extend(self.type_filters[self.sale_order_type])
        return filter_phrase

    def get_customer_shipments(self):
        d = self.after_create_time
        filter_ = ['AND', ["create_date", ">",
                          {"__class__": "datetime", "year": d.year,
                           "month": d.month, "day": d.day,
                           "hour": d.hour,
                           "minute": d.minute, "second": d.second,
                           "microsecond": 0}],
                  ["state", "in", ["waiting", "packed", "assigned"]]]
        fields = ['sales']
        Shipment = client.model('stock.shipment.out')
        shipments = Shipment.search_read_all(
            domain=filter_,
            order=[["create_date", "DESC"]],
            fields=fields
        )
        return list(shipments)

    def get_sales_orders(self):
        Sale = client.model('sale.sale')
        fields = ['number', 'lines']
        self.filter.append(['id', 'in', self.sale_ids])
        sales = Sale.search_read_all(
            domain=self.filter,
            order=[["create_date", "DESC"]],
            fields=fields
        )
        return list(sales)

    def get_order_lines(self, ids):
        Line = client.model('sale.line')
        fields = ['note', 'product', 'quantity', 'product.code', 'note',
                  'product.quantity_available', 'product.boms']
        lines = Line.search_read_all(
            domain=['AND', ['id', 'in', ids]],
            order=None,
            fields=fields
        )
        lines = list(lines)
        self.mark_engraving_lines(lines)
        self.mark_bundle_lines(lines)

        return list(lines)

    def mark_engraving_lines(self, lines):
        for line in lines:
            note = line.get('note', '')
            line['has_engraving'] = "engraving" in note.lower() if note else False

    def mark_bundle_lines(self, lines):
        for line in lines:
            line['has_bundle'] = len(line['product.boms']) > 1

    @staticmethod
    def left_engraving_sales(sales):
        s = []
        for sale in sales:
            if any(line['has_engraving'] for line in sale['lines_info']):
                s.append(sale)
        return s

    @staticmethod
    def left_sets_and_bundles(sales):
        s = []
        for sale in sales:
            if any(line['has_bundle'] for line in sale['lines_info']):
                s.append(sale)
        return s

    def create_shipment(self, products):
        result = create_internal_shipment(
            self.get_reference(), products, state='assigned')
        if not result:
            self.errors.append('failed to create internal shipments')
        else:
            self.success = True

    def get_reference(self):
        d = datetime.date.today()
        prefix = f'{self.sale_order_type}-{d}'
        return prefix

    def send_email(self):
        message = ''
        if self.success:
            message += f"{self.get_reference()} created (reference of internal shipment) <br>"
        if self.errors:
            message += f"Errors <br>"
            message += "<br />".join(self.errors)
        if message:
            send_email(f"Fulfil Report: Internal shipments {self.sale_order_type}",
                       message,  email=['maxwell@auratenewyork.com'],
                       dev_recipients=True)