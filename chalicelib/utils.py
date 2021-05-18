from datetime import datetime

import json
import os


ROLLBACK_DIR = os.environ.get('ROLLBACK_DIR', 'rollback')


def make_rollbak_filename(filename, server_name):
    ntime = datetime.now().strftime("%m_%d_%Y_at_%I%p")
    return os.path.join(ROLLBACK_DIR, f"{ntime}_{filename}_{server_name}.json")


def fill_rollback_file(data, filename='', access_mode='w', server_name=''):
    filename = make_rollbak_filename(filename, server_name=server_name)
    with open(filename, access_mode) as out:
        data = json.dumps(data, indent=4, sort_keys=True)
        print(data, file=out)
