#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, ls_subset_csv_path, zkm_csv_path, task_set_and_op,
         zkm_subset_csv_path):
    task_set, op = task_set_and_op.split('/')[-2:]
    ls_df = pd.read_csv(
        ls_subset_csv_path, index_col='timestamp', parse_dates=True)
    min_ls = min(ls_df.index)
    max_ls = max(ls_df.index)

    zkm_df = pd.read_csv(zkm_csv_path, index_col='timestamp', parse_dates=True)

    subset_df = zkm_df[(zkm_df.index >= min_ls) & (zkm_df.index <= max_ls)]

    subset_df.to_csv(zkm_subset_csv_path)


if __name__ == '__main__':
    main(*sys.argv)
