#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, csv_path, task_set_and_op, subset_csv_path):
    task_set, op = task_set_and_op.split('/')[-2:]
    df = pd.read_csv(csv_path, index_col='timestamp', parse_dates=True)

    task_set_idx = df.name == task_set
    # "UNNAMED_OP" currently selects N/A-containing records.  See note
    # in gen_subsets_mk.py
    op_idx = df.method == op if op != 'UNNAMED_OP' else df.method.isna()

    df1 = df[task_set_idx & op_idx]
    req_diff = df1['num_requests'].diff()
    df2 = df1[req_diff.isnull() | req_diff > 0]
    df2.to_csv(subset_csv_path)


if __name__ == '__main__':
    main(*sys.argv)
