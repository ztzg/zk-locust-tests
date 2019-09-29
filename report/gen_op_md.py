#!/usr/bin/env python3

import sys
import os.path
import logging
import json

import numpy as np
import pandas as pd
import pandas.plotting
import matplotlib.pyplot as plt

pandas.plotting.register_matplotlib_converters()

_colors = [c["color"] for c in list(plt.rcParams["axes.prop_cycle"])]

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


class Group(object):
    def __init__(self, sample_id, ls_df, zkm_df):
        self.sample_id = sample_id
        self.ls_df = ls_df
        self.zkm_df = zkm_df
        self._client_ids = None
        self._ls_merged_df = None
        self._ls_unmerged_df = None

    def client_ids(self):
        if self._client_ids is None:
            df = self.unmerged_client_stats()
            self._client_ids = df['client_id'].unique()
        return self._client_ids

    def merged_client_stats(self):
        if self._ls_merged_df is None:
            pick = self.ls_df['client_id'].isna()
            self._ls_merged_df = self.ls_df[pick]
        return self._ls_merged_df

    def unmerged_client_stats(self):
        if self._ls_unmerged_df is None:
            self._ls_unmerged_df = self.ls_df.loc[:, [
                'client_id', 'num_requests', 'num_failures'
            ]].dropna()
        return self._ls_unmerged_df


class FigureInfo(object):
    def __init__(self, title, naked_path, exts):
        self.title = title
        self.naked_path = naked_path
        self.exts = exts


def write_md(df, task_set, op, md_path, latencies_base_path,
             user_count_base_path, num_requests_base_path, errors_fig_infos,
             zkm_plot_infos):
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

        if errors_fig_infos:
            f.write('#### Errors\n\n')
            for fig in errors_fig_infos:
                f.write('##### %s\n\n' % fig.title)
                f.write('\n![](%s)\n\n' % fig.naked_path)

        if num_requests_base_path:
            f.write('\n#### Requests\n\n')
            f.write('\n![](%s)\n' % num_requests_base_path)

        if user_count_base_path:
            f.write('\n#### Client Count\n\n')
            f.write('\n![](%s)\n' % user_count_base_path)

        if len(zkm_plot_infos) > 0:
            f.write('\n### ZooKeeper Metrics\n\n')
            for label, base_path in zkm_plot_infos:
                f.write('\n#### %s\n\n' % label)
                f.write('\n![](%s)\n' % base_path)

        f.write('\n')


def plot_latencies(groups, latencies_base_path):
    fig = plt.figure()
    ax = fig.gca()

    is_relative = len(groups) > 1

    for i in range(len(groups)):
        is_main = i == 0
        df = groups[i].merged_client_stats()
        color = _colors[i % len(_colors)]

        if is_relative:
            nidx = (df.index - df.index.min()).total_seconds()
            df = df.set_index(nidx)

        if is_main:
            for pc in ['66%', '75%', '80%', '90%', '98%', '99%', '100%']:
                ax.fill_between(
                    df.index, 0, df[pc], facecolor=color, alpha=0.1)

        df.plot.line(y='50%', color=color, ax=ax)
        df.plot.line(y='95%', color=color, linestyle='--', ax=ax)
        df.plot.line(y='100%', color=color, linestyle=':', ax=ax)

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


def plot_num_requests_per_1s(dfs, num_requests_base_path):
    fig, axes = plt.subplots(nrows=3)
    req_ax = axes[0]
    succ_ax = axes[1]
    fail_ax = axes[2]

    is_relative = len(dfs) > 1

    if is_relative:
        for i in range(len(dfs)):
            df = dfs[i]
            nidx = (df.index - df.index.min()).total_seconds()
            dfs[i] = df.set_index(nidx)

    for i in range(len(dfs)):
        df = dfs[i]
        color = _colors[i % len(_colors)]

        dnr_dt = df.num_requests.diff()
        dnr_dt[dnr_dt < 0] = np.nan

        dnf_dt = df.num_failures.diff()
        dnf_dt[dnf_dt < 0] = np.nan

        req_ax.plot(df.index, dnr_dt, label='Req./s', color=color)
        succ_ax.plot(df.index, dnr_dt - dnf_dt, label='Successes', color=color)
        fail_ax.plot(df.index, dnf_dt, label='Failures', color=color)

    for ax in [req_ax, succ_ax]:
        ax.legend()
        ax.xaxis.label.set_visible(False)
        ax.tick_params(axis='x', which='both', labelbottom=False)

    fail_ax.legend()

    for label in fail_ax.get_xticklabels():
        label.set_ha("right")
        label.set_rotation(30)
    fig.subplots_adjust(bottom=0.2)

    for ext in _savefig_exts:
        fig.savefig(num_requests_base_path + ext)

    plt.close(fig)
    return True


def plot_num_requests_multi(groups, num_requests_base_path):
    columns = ['num_requests', 'num_failures']

    sel_groups = []

    for group in groups:
        df = group.unmerged_client_stats()

        if len(df) < 2:
            continue

        sel_groups.append(group)

    if len(sel_groups) == 0:
        return False

    is_relative = len(sel_groups) > 1

    dfs = []

    for i in range(len(sel_groups)):
        group = sel_groups[i]
        df = group.unmerged_client_stats()
        client_ids = group.client_ids()

        min_t = df.index.min()
        max_t = df.index.max()
        nidx = pd.date_range(min_t, max_t, freq='1s')

        x_df = pd.DataFrame(index=nidx, columns=columns)
        x_df = x_df.fillna(0)

        for client_id in client_ids:
            client_df = df.loc[df['client_id'] == client_id, columns]

            client_df = client_df.reindex(
                client_df.index.union(nidx)).interpolate().reindex(nidx)
            x_df += client_df

        x_df = x_df.cumsum()

        if is_relative:
            ridx = x_df.index - x_df.index.min()
            x_df = x_df.set_index(ridx)

        dfs.append(x_df)

    return plot_num_requests_per_1s(dfs, num_requests_base_path)


def plot_num_requests(groups, num_requests_base_path):
    can_multi = True
    for group in groups:
        clients_df = group.unmerged_client_stats()
        if not len(clients_df):
            can_multi = False
            break

    if can_multi:
        return plot_num_requests_multi(groups, num_requests_base_path)

    dfs = []
    min_t = None
    max_t = None

    for group in groups:
        df = group.merged_client_stats()
        df = df.loc[:, ['num_requests', 'num_failures']]
        df = df.dropna()

        if len(df) < 2:
            continue

        dfs.append(df)
        df_min_t = df.index.min()
        df_max_t = df.index.max()
        if min_t is None or df_min_t < min_t:
            min_t = df_min_t
        if max_t is None or df_max_t > max_t:
            max_t = df_max_t

    if len(dfs) == 0:
        return False

    nidx = pd.date_range(min_t, max_t, freq='1s')

    for i in range(len(dfs)):
        df = dfs[i]
        df = df.reindex(df.index.union(nidx)).interpolate().reindex(nidx)
        # df = df.rolling(3).mean()
        dfs[i] = df

    return plot_num_requests_per_1s(dfs, num_requests_base_path)


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


def process_errors(groups, base_path):
    if len(groups) != 1:
        return None

    df = groups[0].ls_df
    df = df[df.client_id.notnull()]

    if not len(df):
        return None

    encoded_errors = df.errors.fillna('')
    deserialized = []
    keys = set()
    for error_json in encoded_errors:
        if not error_json:
            deserialized.append({})
            continue

        error_data = json.loads(error_json)
        deserialized.append(error_data)
        keys |= error_data.keys()

    if not len(keys):
        return None

    new_columns = {}
    for key in keys:
        column_name = key
        new_columns[column_name] = pd.Series(
            name=column_name, index=df.index, data=0)

    for i in range(len(encoded_errors)):
        error_data = deserialized[i]
        for key in keys:
            count = error_data.get(key)
            if not count:
                continue

            column_name = key
            s = new_columns[column_name]
            s[df.index[i]] = count

    fig_infos = []

    for column in new_columns.values():
        data = {'client_id': df.client_id, column.name: column}

        x_df = pd.DataFrame(data)

        fig, ax = plt.subplots()

        x_df.groupby('client_id').plot(y=column.name, ax=ax, legend=False)

        naked_path = base_path
        if fig_infos:
            naked_path = naked_path + '_' + str(len(fig_infos))

        for ext in _savefig_exts:
            fig.savefig(naked_path + ext)

        fig_infos.append(FigureInfo(column.name, naked_path, _savefig_exts))

        plt.close(fig)

    return fig_infos


def process_task_set_op_single(task_set, op, group, base_path, md_path):
    ls_df = group.ls_df
    zkm_df = group.zkm_df
    ls_merged_df = group.merged_client_stats()

    latencies_base_path = None
    if len(ls_merged_df) > 0:
        latencies_base_path = base_path + '_latencies'
        plot_latencies([group], latencies_base_path)

    user_count_base_path = None
    if len(ls_merged_df) > 0:
        user_count_base_path = base_path + '_user_count'
        plot_user_count(ls_merged_df, user_count_base_path)

    num_requests_base_path = base_path + '_num_requests'
    if not plot_num_requests([group], num_requests_base_path):
        num_requests_base_path = None

    errors_fig_infos = process_errors([group], base_path + '_errors')

    zkm_plot_infos = []
    if len(zkm_df) > 0:
        for zkm_plot in _zkm_plots:
            zkm_plot_info = plot_zkm(zkm_df, zkm_plot, base_path)
            zkm_plot_infos.append(zkm_plot_info)

    write_md(ls_df, task_set, op, md_path, latencies_base_path,
             user_count_base_path, num_requests_base_path, errors_fig_infos,
             zkm_plot_infos)


def process_task_set_op_multi(task_set, op, groups, base_path, md_path):
    latencies_base_path = None
    latencies_groups = []
    for group in groups:
        if len(group.merged_client_stats()) > 0:
            latencies_base_path = base_path + '_latencies'
            latencies_groups.append(group)

    if latencies_base_path:
        plot_latencies(latencies_groups, latencies_base_path)

    num_requests_base_path = base_path + '_num_requests'
    if not plot_num_requests(groups, num_requests_base_path):
        num_requests_base_path = None


def load_group(base_input_path, data_item):
    sample_id = data_item.get('id') or None
    ls_csv_path = os.path.join(base_input_path, data_item['locust-stats'])
    ls_df = pd.read_csv(ls_csv_path, index_col=0, parse_dates=True)
    zkm_csv_path = os.path.join(base_input_path, data_item['zk-metrics'])
    zkm_df = pd.read_csv(zkm_csv_path, index_col=0, parse_dates=True)

    return Group(sample_id, ls_df, zkm_df)


def process_task_set_op(base_input_path, task_set, op, data, base_path,
                        md_path):
    groups = [load_group(base_input_path, data_item) for data_item in data]

    if len(groups) == 1:
        process_task_set_op_single(task_set, op, groups[0], base_path, md_path)
    else:
        process_task_set_op_multi(task_set, op, groups, base_path, md_path)


def process_fragments(base_input_path, fragments, base_path, md_path):
    frag_dict = {}
    for fragment in fragments:
        key = (fragment['task_set'], fragment['op'])
        frag_dict[key] = frag_dict.get(key, []) + fragment['data']

    for (task_set, op), data in frag_dict.items():
        process_task_set_op(base_input_path, task_set, op, data, base_path,
                            md_path)


def main(executable, metadata, base_path, md_path):
    with open(metadata) as f:
        lines = f.readlines()

    fragments = [json.loads(line) for line in lines]

    process_fragments('.', fragments, base_path, md_path)


if __name__ == '__main__':
    main(*sys.argv)
