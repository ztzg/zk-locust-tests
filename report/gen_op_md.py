#!/usr/bin/env python3

import sys
import logging

import numpy as np
import pandas as pd
import pandas.plotting
import matplotlib.pyplot as plt

pandas.plotting.register_matplotlib_converters()

logging.basicConfig()
_logger = logging.getLogger(__name__)
# _logger.setLevel(logging.DEBUG)

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

_zkm_plots = [
    {
        'label': 'Outstanding Requests',
        'name': 'outstanding_requests',
        'metrics': ['outstanding_requests']
    },
    {
        'label': 'Clients',
        'name': 'clients',
        'metrics': ['num_alive_connections']
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
    }
]  # yapf:disable


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


def plot_num_requests_per_1s(df, num_requests_base_path):
    dnr_dt = df.num_requests.diff()
    dnr_dt[dnr_dt < 0] = np.nan
    dnf_dt = df.num_failures.diff()
    dnf_dt[dnf_dt < 0] = np.nan

    fig, axes = plt.subplots(nrows=2)
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
    return True


def plot_num_requests_multi(df, num_requests_base_path):
    if len(df) < 2:
        return False

    client_ids = df['client_id'].unique()

    min_t, max_t = df.index.min(), df.index.max()
    nidx = pd.date_range(min_t, max_t, freq='1s')
    columns = ['num_requests', 'num_failures']

    x_df = pd.DataFrame(index=nidx, columns=columns)
    x_df = x_df.fillna(0)

    for client_id in client_ids:
        client_df = df.loc[df['client_id'] == client_id, columns]
        client_df = client_df.reindex(
            client_df.index.union(nidx)).interpolate().reindex(nidx)
        x_df += client_df

    x_df = x_df.cumsum()

    return plot_num_requests_per_1s(x_df, num_requests_base_path)


def plot_num_requests(df, num_requests_base_path):
    clients_df = df.loc[:, ['client_id', 'num_requests', 'num_failures']]
    clients_df = clients_df.dropna()

    _logger.debug('plot_num_requests %d records, %d client records', len(df),
                  len(clients_df))

    if len(clients_df):
        return plot_num_requests_multi(clients_df, num_requests_base_path)

    df = df.loc[:, ['num_requests', 'num_failures']]
    df = df.dropna()

    if len(df) < 2:
        return False

    oidx = df.index
    nidx = pd.date_range(oidx.min(), oidx.max(), freq='1s')

    df = df.reindex(oidx.union(nidx)).interpolate().reindex(nidx)
    # df = df.rolling(3).mean()

    return plot_num_requests_per_1s(df, num_requests_base_path)


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


def main(executable, ls_csv_path, zkm_csv_path, stem, base_path):
    task_set, op = stem.split('/')[-2:]
    ls_df = pd.read_csv(ls_csv_path, index_col='timestamp', parse_dates=True)
    zkm_df = pd.read_csv(zkm_csv_path, index_col='timestamp', parse_dates=True)

    ls_merged_df = ls_df[ls_df['client_id'].isna()]

    latencies_base_path = None
    if len(ls_merged_df) > 0:
        latencies_base_path = base_path + '_latencies'
        plot_latencies(ls_merged_df, latencies_base_path)

    user_count_base_path = None
    if len(ls_merged_df) > 0:
        user_count_base_path = base_path + '_user_count'
        plot_user_count(ls_merged_df, user_count_base_path)

    num_requests_base_path = base_path + '_num_requests'
    if not plot_num_requests(ls_df, num_requests_base_path):
        num_requests_base_path = None

    zkm_plot_infos = []
    if len(zkm_df) > 0:
        for zkm_plot in _zkm_plots:
            zkm_plot_info = plot_zkm(zkm_df, zkm_plot, base_path)
            zkm_plot_infos.append(zkm_plot_info)

    write_md(ls_df, task_set, op, base_path + '.md', latencies_base_path,
             user_count_base_path, num_requests_base_path, zkm_plot_infos)


if __name__ == '__main__':
    main(*sys.argv)
