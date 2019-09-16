#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, task_sets_var, ops_var, csv_path):
    df = pd.read_csv(csv_path, index_col='timestamp', parse_dates=True)

    task_sets = [x for x in df.name.unique() if x != 'Total']
    print('%s = %s' % (task_sets_var, ' '.join(task_sets)))

    df_full = df.dropna()

    ops = [x for x in (df_full.name.map(str) + '/' + df_full.method).unique()]
    print('%s = %s' % (ops_var, ' '.join(ops)))


if __name__ == '__main__':
    main(*sys.argv)
