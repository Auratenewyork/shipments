import csv
from datetime import datetime

import json
import os


ROLLBACK_DIR = os.environ.get('ROLLBACK_DIR', 'rollback')


def make_rollbaсk_filename(filename, server_name='', suffix='json'):
    ntime = datetime.now().strftime("%m_%d_%Y_%H:%M")
    return os.path.join(ROLLBACK_DIR, f"{ntime}_{filename}_{server_name}.{suffix}")


def fill_rollback_file(data, filename, access_mode='w', server_name=''):
    filename = make_rollbaсk_filename(filename, server_name=server_name)
    with open(filename, access_mode) as out:
        data = json.dumps(data, indent=4, sort_keys=True)
        print(data, file=out)


def fill_csv_file(data, filename, access_mode='w', server_name=''):
    filename = make_rollbaсk_filename(filename, server_name=server_name, suffix='csv')
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)