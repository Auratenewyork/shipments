import os

RUBYHAS_HQ_STORAGE = 26
RUBYHAS_WAREHOUSE = 23
AURATE_WAREHOUSE = 4
AURATE_HQ_STORAGE = 3
AURATE_STORAGE_ZONE = 3
AURATE_OUTPUT_ZONE = 2
COMPANY = 1
PRODUCTION = 9
FULFIL_API_URL = os.environ.get('FULFIL_API_ENDPOINT')
EASYPOST_API_KEY = os.environ.get('EASYPOST_API_KEY')
EASYPOST_URL = 'https://api.easypost.com/v2/shipments'


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


def listDictsToHTMLTable(data: list):
    header = [""] + list(data[0].keys())
    html = HTML(Header=header,
                tableStyles={'margin': '3px'},
                trStyles={'background-color': '#7cc3a97d'})
    for i, row in enumerate(data):
        if i % 2 == 0:
            BGC = 'aliceblue'
        else:
            BGC = '#c2d4e4'
        html.addRow([i] + list(row.values()),
                    trStyles={'background-color': BGC},
                    tdStyles={'padding': '0.5rem'})
    return html

