#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, task_sets_var, ops_var, csv_path):
    df = pd.read_csv(csv_path, index_col='timestamp', parse_dates=True)

    task_sets = [x for x in df.name.unique() if x != 'Total']
    print('%s = %s' % (task_sets_var, ' '.join(task_sets)))

    df_no_total = df[df.name != 'Total']

    # Empty cells are filled with N/As by Pandas.  We want to carry
    # them over, but need a name for file system storage; let's use
    # "UNNAMED_OP" for now.  TODO(ddiederen): Get rid of this.
    ops = (df_no_total.name.map(str) + '/' +
           df_no_total.method.fillna('UNNAMED_OP')).unique()
    print('%s = %s' % (ops_var, ' '.join(ops)))


if __name__ == '__main__':
    main(*sys.argv)
