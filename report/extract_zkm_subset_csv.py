#!/usr/bin/env python3

import sys
import pandas as pd


def main(executable, ls_subset_csv_path, zkm_csv_path, stem,
         zkm_subset_csv_path):
    task_set, op = stem.split('/')[-2:]
    ls_df = pd.read_csv(
        ls_subset_csv_path, index_col='timestamp', parse_dates=True)

    zkm_df = pd.read_csv(zkm_csv_path, index_col='timestamp', parse_dates=True)

    if len(ls_df) > 0:
        min_ls = min(ls_df.index)
        max_ls = max(ls_df.index)

        subset_df = zkm_df[(zkm_df.index >= min_ls) & (zkm_df.index <= max_ls)]
    else:
        subset_df = zkm_df.head(0)  # Only metadata

    subset_df.to_csv(zkm_subset_csv_path)


if __name__ == '__main__':
    main(*sys.argv)
