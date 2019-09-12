#!/usr/bin/env python3

import sys
import pandas as pd
import pandas.plotting
import matplotlib.pyplot as plt

pandas.plotting.register_matplotlib_converters()


def write_md(df, task_set, op, md_path, base_path):
    data = df.tail(1)

    with open(md_path, 'w') as f:
        f.write("## Task set '%s', op '%s'\n\n" % (task_set, op))
        for metric in [
                '# requests', '# failures', 'Median response time',
                'Average response time', 'Min response time',
                'Max response time', 'Requests/s'
        ]:
            v = data[metric][0]
            if isinstance(v, float):
                v = round(v, 3)
            f.write('  * %s: %s\n' % (metric.replace('#', '\\#'), v))
        f.write('\n![](%s)\n' % base_path)
        f.write('\n### Percentiles\n\n')
        for pc in [
                '50%', '66%', '75%', '80%', '90%', '95%', '98%', '99%', '100%'
        ]:
            f.write('  * %s <= %s ms\n' % (pc, data[pc][0]))
        f.write('\n')


def plot_latencies(df, base_path):
    ax = plt.gca()

    for pc in ['66%', '75%', '80%', '90%', '98%', '99%', '100%']:
        ax.fill_between(df.index, 0, df[pc], facecolor='blue', alpha=0.1)

    df.plot.line(y='50%', color='blue', ax=ax)
    df.plot.line(y='95%', color='blue', linestyle='--', ax=ax)
    df.plot.line(y='100%', color='blue', linestyle=':', ax=ax)

    for ext in ['.svg', '.pdf']:
        ax.get_figure().savefig(base_path + ext)


def main(executable, csv_path, task_set_and_op, base_path):
    task_set, op = task_set_and_op.split('/')[-2:]
    df = pd.read_csv(csv_path, index_col='timestamp', parse_dates=True)

    plot_latencies(df, base_path)
    write_md(df, task_set, op, base_path + '.md', base_path)


if __name__ == '__main__':
    main(*sys.argv)
