#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, csv_path, task_set_and_op, subset_csv_path):
    task_set, op = task_set_and_op.split('/')[-2:]
    df = pd.read_csv(csv_path, index_col='timestamp', parse_dates=True)

    df1 = df[(df.Name == task_set) & (df.Method == op)]
    req_diff = df1['# requests'].diff()
    df2 = df1[req_diff.isnull() | req_diff > 0]
    df2.to_csv(subset_csv_path)


if __name__ == '__main__':
    main(*sys.argv)
