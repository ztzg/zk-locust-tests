#!/usr/bin/env python3

import sys
import json


def main(executable, ls_csv_path, zkm_csv_path, stem, id=None):
    task_set, op = stem.split('/')[-2:]

    data = [{
        'id': id,
        'locust-stats': ls_csv_path,
        'zk-metrics': zkm_csv_path
    }]

    info = {'task_set': task_set, 'op': op, 'data': data}

    s = json.dumps(info, ensure_ascii=True, indent=None)

    sys.stdout.write(s + '\n')


if __name__ == '__main__':
    main(*sys.argv)
