#!/usr/bin/env python3

import sys
import numpy as np
import pandas as pd
import pandas.plotting
import matplotlib.pyplot as plt

pandas.plotting.register_matplotlib_converters()

_savefig_exts = ['.svg', '.pdf']

_ls_key_labels = {
    'num_requests': '# requests',
    'num_failures': '# failures',
    'median_response_time': 'Median response time',
    'avg_response_time': 'Average response time',
    'min_response_time': 'Min response time',
    'max_response_time': 'Max response time',
    'total_rps': 'Requests/s',
}

_zkm_plots = [{
    'label': 'Outstanding Requests',
    'name': 'outstanding_requests',
    'metrics': ['outstanding_requests']
},
              {
                  'label':
                  'Clients',
                  'name':
                  'clients',
                  'metrics':
                  ['num_alive_connections', 'open_file_descriptor_count']
              },
              {
                  'label': 'Nodes',
                  'name': 'nodes',
                  'metrics': ['znode_count', 'ephemerals_count']
              },
              {
                  'label': 'Watches',
                  'name': 'watch_count',
                  'metrics': ['watch_count']
              }]


def write_md(df, task_set, op, md_path, latencies_base_path,
             user_count_base_path, num_requests_base_path, zkm_plot_infos):
    with open(md_path, 'w') as f:
        f.write("## Task set '%s', op '%s'\n\n" % (task_set, op))

        data = df.tail(1)

        if len(data) == 0:
            f.write("(Empty dataset)\n\n")
            return

        for key, label in _ls_key_labels.items():
            v = data[key][0]
            if isinstance(v, float):
                v = round(v, 3)
            f.write('  * %s: %s\n' % (label.replace('#', '\\#'), v))

        if latencies_base_path:
            f.write('\n### Latencies\n\n')
            f.write('\n![](%s)\n' % latencies_base_path)

        f.write('\n#### Percentiles\n\n')
        for pc in [
                '50%', '66%', '75%', '80%', '90%', '95%', '98%', '99%', '100%'
        ]:
            f.write('  * %s <= %s ms\n' % (pc, data[pc][0]))

        f.write('\n### Other Metrics\n\n')

        if user_count_base_path:
            f.write('\n#### Client Count\n\n')
            f.write('\n![](%s)\n' % user_count_base_path)

        if num_requests_base_path:
            f.write('\n#### Requests\n\n')
            f.write('\n![](%s)\n' % num_requests_base_path)

        if len(zkm_plot_infos) > 0:
            f.write('\n### ZooKeeper Metrics\n\n')
            for label, base_path in zkm_plot_infos:
                f.write('\n#### %s\n\n' % label)
                f.write('\n![](%s)\n' % base_path)

        f.write('\n')


def plot_latencies(df, latencies_base_path):
    fig = plt.figure()
    ax = fig.gca()

    for pc in ['66%', '75%', '80%', '90%', '98%', '99%', '100%']:
        ax.fill_between(df.index, 0, df[pc], facecolor='blue', alpha=0.1)

    df.plot.line(y='50%', color='blue', ax=ax)
    df.plot.line(y='95%', color='blue', linestyle='--', ax=ax)
    df.plot.line(y='100%', color='blue', linestyle=':', ax=ax)

    for ext in _savefig_exts:
        fig.savefig(latencies_base_path + ext)

    plt.close(fig)


def plot_user_count(df, user_count_base_path):
    fig = plt.figure()
    ax = fig.gca()

    df.plot.line(y='user_count', ax=ax)

    for ext in _savefig_exts:
        fig.savefig(user_count_base_path + ext)

    plt.close(fig)


def plot_num_requests(df, num_requests_base_path):
    fig, axes = plt.subplots(nrows=2)

    dt = [
        delta.total_seconds()
        for delta in df.index.to_series(keep_tz=True).diff()
    ]
    dnr_dt = df.num_requests.diff() / dt
    dnr_dt[dnr_dt < 0] = np.nan
    dnf_dt = df.num_failures.diff() / dt
    dnf_dt[dnf_dt < 0] = np.nan

    ax = axes[0]

    ax.plot(df.index, dnr_dt, label='Req./s')
    ax.legend()
    ax.xaxis.label.set_visible(False)
    ax.tick_params(axis='x', which='both', labelbottom=False)

    ax = axes[1]
    ax.plot(df.index, dnf_dt, label='Fail/s')
    ax.legend()

    for label in ax.get_xticklabels():
        label.set_ha("right")
        label.set_rotation(30)
    fig.subplots_adjust(bottom=0.2)

    for ext in _savefig_exts:
        fig.savefig(num_requests_base_path + ext)

    plt.close(fig)


def plot_zkm(df, plot_def, base_path):
    plot_path = base_path + '_' + plot_def['name']
    host_ports = df.host_port.unique()
    n = len(host_ports)

    fig, axes = plt.subplots(nrows=n)

    for i in range(n):
        ax = axes[i]
        df[df.host_port == host_ports[i]].plot(y=plot_def['metrics'], ax=ax)
        if (i < n - 1):
            ax.xaxis.label.set_visible(False)
            ax.tick_params(axis='x', which='both', labelbottom=False)

    for ext in _savefig_exts:
        fig.savefig(plot_path + ext)

    plt.close(fig)

    return (plot_def['label'], plot_path)


def main(executable, ls_csv_path, zkm_csv_path, task_set_and_op, base_path):
    task_set, op = task_set_and_op.split('/')[-2:]
    ls_df = pd.read_csv(ls_csv_path, index_col='timestamp', parse_dates=True)
    zkm_df = pd.read_csv(zkm_csv_path, index_col='timestamp', parse_dates=True)

    latencies_base_path = None
    if len(ls_df) > 0:
        latencies_base_path = base_path + '_latencies'
        plot_latencies(ls_df, latencies_base_path)

    user_count_base_path = None
    if len(ls_df) > 0:
        user_count_base_path = base_path + '_user_count'
        plot_user_count(ls_df, user_count_base_path)

    if ls_df.shape[0] < 2:
        num_requests_base_path = None
    else:
        num_requests_base_path = base_path + '_num_requests'
        plot_num_requests(ls_df, num_requests_base_path)

    zkm_plot_infos = []
    if len(zkm_df) > 0:
        for zkm_plot in _zkm_plots:
            zkm_plot_info = plot_zkm(zkm_df, zkm_plot, base_path)
            zkm_plot_infos.append(zkm_plot_info)

    write_md(ls_df, task_set, op, base_path + '.md', latencies_base_path,
             user_count_base_path, num_requests_base_path, zkm_plot_infos)


if __name__ == '__main__':
    main(*sys.argv)
