import datetime
import json
import os

import pdfkit
from _decimal import Decimal
from jinja2 import Environment, FileSystemLoader, Template, BaseLoader


BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class HTML:

    def __init__(self, Header, tableStyles={}, trStyles={}, thStyles={}):
        self.tableStyles = HTML._styleConverter(tableStyles)
        trStyles = HTML._styleConverter(trStyles)
        thStyles = HTML._styleConverter(thStyles)
        self.rows = []
        self.Header = f'<tr {trStyles} >'
        for th in Header:
            self.Header += f'\n<th {thStyles} >{th}</th>'
        self.Header += '\n</tr>'

    @staticmethod
    def _styleConverter(styleDict: dict):
        if styleDict == {}:
            return ''
        styles = ''
        for [style, value] in styleDict.items():
            styles += f'{style}: {value}';
        return f'style="{styles}"'

    def addRow(self, row, trStyles={}, tdStyles={}):
        trStyles = HTML._styleConverter(trStyles)
        tdStyles = HTML._styleConverter(tdStyles)
        temp_row = f'\n<tr {trStyles} >'
        for td in row:
            temp_row += f'\n<td {tdStyles} >{td}</td>'
        temp_row += '\n</tr>'
        self.rows.append(temp_row)

    def __str__(self):
        return f''' 
<table {self.tableStyles} >
    {self.Header}
    {''.join(self.rows)}
 </table> '''


def listDictsToHTMLTable(data: list, keys=None):
    if not data:
        return "Empty result"
    if not keys:
        keys = list(data[0].keys())
    header = [""] + keys
    html = HTML(Header=header,
                tableStyles={'margin': '3px'},
                trStyles={'background-color': '#7cc3a97d'})
    for i, row in enumerate(data):
        if i % 2 == 0:
            BGC = 'aliceblue'
        else:
            BGC = '#c2d4e4'
        html.addRow([i] + [row[key] for key in keys],  #list(row.values()) +
                    trStyles={'background-color': BGC},
                    tdStyles={'padding': '0.5rem'})
    return html


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime.timedelta, datetime.date, datetime.datetime)):
            return str(obj)
        return super(CustomJsonEncoder, self).default(obj)


def dates_with_passed_some_work_days(wd_number=3, excluded=(6, 7)):
    d = datetime.date.today()
    wd = 0
    date_list = []
    while wd <= wd_number + 1:
        d -= datetime.timedelta(days=1)
        if d.isoweekday() not in excluded:
            wd += 1
        if wd == wd_number:
            date_list.append(d)
    return date_list


def date_after_some_workdays(d, wd_number=3, excluded=(6, 7)):
    wd = 0
    while wd != wd_number:
        if d.isoweekday() not in excluded:
            wd += 1
        d += datetime.timedelta(days=1)
    return d


# creates PDF from rendered html
def create_pdf_file(html_str, binary_path, options):
    default_options = {'debug-javascript': '', 'javascript-delay': 200}
    default_options.update(options)
    config = pdfkit.configuration(wkhtmltopdf=binary_path)
    pdf_string = pdfkit.from_string(
        html_str,
        output_path=False,
        configuration=config,
        options=default_options)
    return pdf_string


def render_template(data, template, **kwargs):
    env = Environment(loader=BaseLoader())
    filters = kwargs.get('filters', {})
    for key, value in filters.items():
        env.filters[key] = value

    tmp = env.from_string(template)
    return tmp.render(**data)


# render html from internal template, see chalicelib.fulfil to create from fulfil template
def render_internal_template(data, template, **kwargs):
    env = Environment(loader=FileSystemLoader(f'{BASE_DIR}/template'))
    filters = kwargs.get('filters', {})
    for key, value in filters.items():
        env.filters[key] = value

    tmp = env.get_template(template)
    return tmp.render(**data)
