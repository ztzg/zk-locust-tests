#!/usr/bin/env python3

import sys
import os.path
import logging
import json
import warnings

import numpy as np
import pandas as pd
import pandas.plotting
import matplotlib.pyplot as plt

pandas.plotting.register_matplotlib_converters()

_colors = [c["color"] for c in list(plt.rcParams["axes.prop_cycle"])]

_figsize = plt.rcParams["figure.figsize"]

logging.basicConfig()
_logger = logging.getLogger(__name__)
# _logger.setLevel(logging.DEBUG)

warnings.filterwarnings('ignore', 'The handle <matplotlib')

_savefig_exts = ['.svg', '.pdf']
_per_worker = '/Wkr'

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
        'metrics': ['outstanding_requests'],
        'ignore_not_serving': True
    },
    {
        'label': 'Clients',
        'name': 'clients',
        'metrics': ['num_alive_connections'],
        'ignore_not_serving': True
    },
    {
        'label': 'Nodes',
        'name': 'nodes',
        'metrics': ['znode_count', 'ephemerals_count'],
        'ignore_not_serving': True
    },
    {
        'label': 'Watches',
        'name': 'watch_count',
        'metrics': ['watch_count'],
        'ignore_not_serving': True
    }
]  # yapf:disable


class Group(object):
    def __init__(self, sample_id, ls_df, zkm_df):
        self.is_unique = False
        self.sample_id = sample_id
        self.ls_df = ls_df
        self.zkm_df = zkm_df
        self._client_ids = None
        self._ls_merged_df = None
        self._ls_unmerged_df = None

    def prefix_label(self, label):
        if self.is_unique:
            return label

        return self.sample_id + ', ' + label

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

    def unmerged_client_stats(self, extra_columns=None):
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


def relativize(df, *, index_base=None):
    if index_base is None:
        index_base = df.index.min()

    return df.set_index((df.index - index_base).total_seconds())


def vsubplots(nrows):
    figsize = (_figsize[0], _figsize[0] / 3 * nrows)
    return plt.subplots(nrows=nrows, figsize=figsize)


def plot_latencies(groups, latencies_base_path):
    fig = plt.figure()
    ax = fig.gca()

    is_relative = len(groups) > 1

    for i in range(len(groups)):
        is_main = i == 0
        group = groups[i]
        df = group.merged_client_stats()
        color = _colors[i % len(_colors)]

        if is_relative:
            df = relativize(df)

        if is_main:
            for pc in ['66%', '75%', '80%', '90%', '98%', '99%', '100%']:
                ax.fill_between(
                    df.index,
                    0,
                    df[pc],
                    facecolor=color,
                    alpha=0.1,
                    label=group.prefix_label(pc))

        df.plot.line(
            y='50%', color=color, ax=ax, label=group.prefix_label('50%'))
        df.plot.line(
            y='95%',
            color=color,
            linestyle='--',
            ax=ax,
            label=group.prefix_label('95%'))
        df.plot.line(
            y='100%',
            color=color,
            linestyle=':',
            ax=ax,
            label=group.prefix_label('100%'))

    for ext in _savefig_exts:
        fig.savefig(latencies_base_path + ext)

    plt.close(fig)


def plot_client_count(groups, naked_client_count_path):
    is_relative = len(groups) > 1

    fig = plt.figure()
    ax = fig.gca()

    col_names = ['user_count']

    labels = []

    for i in range(len(groups)):
        group = groups[i]
        color = _colors[i % len(_colors)]

        df = group.merged_client_stats()
        df = df.loc[:, col_names]
        df = df.dropna()

        if is_relative:
            index_base = df.index.min()
            df = relativize(df)

        df.plot.line(ax=ax, color=color)
        labels.append(group.prefix_label('ZK Clients'))

        w_ids = group.client_ids()
        alpha = 2 * 1.0 / len(w_ids)

        for j in range(len(w_ids)):
            w_id = w_ids[j]
            ws_df = group.ls_df
            pick_rows = ws_df['client_id'] == w_id
            w_df = ws_df.loc[pick_rows, col_names]

            if is_relative:
                w_df = relativize(w_df, index_base=index_base)

            w_df.plot.line(ax=ax, color=color, linestyle=':', alpha=alpha)
            labels.append(
                group.prefix_label('ZK C.' + _per_worker) if j == 0 else '_')

    ax.legend(labels)

    for ext in _savefig_exts:
        fig.savefig(naked_client_count_path + ext)

    plt.close(fig)

    return naked_client_count_path


def plot_num_requests_per_1s(groups, dfs, client_dfs, num_requests_base_path):
    fig, axes = vsubplots(3)
    req_ax = axes[0]
    succ_ax = axes[1]
    fail_ax = axes[2]

    is_relative = len(dfs) > 1

    t_labels = ('Req./s', 'Successes', 'Failures')
    w_labels = [l + _per_worker for l in t_labels]

    for i in range(len(dfs)):
        group = groups[i]
        df = dfs[i]
        color = _colors[i % len(_colors)]

        all_dfs = [df]

        if client_dfs and client_dfs[i]:
            all_dfs += client_dfs[i]
            alpha = 2 * 1.0 / len(client_dfs[i])

        for df_j in range(len(all_dfs)):
            df = all_dfs[df_j]

            if is_relative:
                df = relativize(df)

            dnr_dt = df.num_requests.diff()
            dnr_dt[dnr_dt < 0] = np.nan

            dnf_dt = df.num_failures.diff()
            dnf_dt[dnf_dt < 0] = np.nan

            if df_j == 0:
                labels = t_labels
            elif df_j == 1:
                labels = w_labels
            else:
                labels = None

            kwargs = {'color': color}
            if df_j > 0:
                kwargs['alpha'] = alpha
                kwargs['linestyle'] = ':'

            req_ax.plot(
                df.index,
                dnr_dt,
                label=group.prefix_label(labels[0]) if labels else '_',
                **kwargs)
            succ_ax.plot(
                df.index,
                dnr_dt - dnf_dt,
                label=group.prefix_label(labels[1]) if labels else '_',
                **kwargs)
            fail_ax.plot(
                df.index,
                dnf_dt,
                label=group.prefix_label(labels[2]) if labels else '_',
                **kwargs)

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

    dfs = []
    client_dfs = []

    for i in range(len(sel_groups)):
        group = sel_groups[i]
        df = group.unmerged_client_stats()
        client_ids = group.client_ids()

        min_t = df.index.min()
        max_t = df.index.max()
        nidx = pd.date_range(min_t, max_t, freq='1s')

        x_df = pd.DataFrame(index=nidx, columns=columns)
        x_df = x_df.fillna(0)

        x_client_dfs = []

        for client_id in client_ids:
            client_df = df.loc[df['client_id'] == client_id, columns]
            client_df = client_df.cumsum()
            client_df = client_df.reindex(
                client_df.index.union(nidx)).interpolate().reindex(nidx)

            x_df += client_df

            x_client_dfs.append(client_df)

        dfs.append(x_df)
        client_dfs.append(x_client_dfs)

    return plot_num_requests_per_1s(sel_groups, dfs, client_dfs,
                                    num_requests_base_path)


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

    sel_groups = []

    for group in groups:
        df = group.merged_client_stats()
        df = df.loc[:, ['num_requests', 'num_failures']]
        df = df.dropna()

        if len(df) < 2:
            continue

        dfs.append(df)
        sel_groups.append(group)

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

    return plot_num_requests_per_1s(sel_groups, dfs, None,
                                    num_requests_base_path)


def plot_zkm_multi(groups, plot_def, base_path):
    plot_path = base_path + '_' + plot_def['name']

    host_ports = set()
    dfs = []
    sel_groups = []
    for group in groups:
        df = group.zkm_df
        pick = df.error != (
            'This ZooKeeper instance is not currently serving requests')
        df = df[pick]

        if not len(df):
            continue

        host_ports |= set(df.host_port.unique())

        dfs.append(df)
        sel_groups.append(group)

    if len(dfs) > 1:
        for group_j in range(len(dfs)):
            dfs[group_j] = relativize(dfs[group_j])

    host_ports = list(host_ports)
    n = len(host_ports)

    fig, axes = vsubplots(n)

    for host_i in range(n):
        ax = axes[host_i]
        host_port = host_ports[host_i]
        labels = []

        for group_j in range(len(dfs)):
            group = sel_groups[group_j]
            color = _colors[group_j % len(_colors)]

            df = dfs[group_j]
            df = df[df.host_port == host_port]

            if not len(df):
                continue

            kwargs = {}
            for metric in plot_def['metrics']:
                df.plot(y=metric, ax=ax, color=color, **kwargs)
                labels.append(group.prefix_label(host_port + ", " + metric))
                kwargs['linestyle'] = ':'  # KLUDGE.

        ax.legend(labels)

        if (host_i < n - 1):
            ax.xaxis.label.set_visible(False)
            ax.tick_params(axis='x', which='both', labelbottom=False)

    for ext in _savefig_exts:
        fig.savefig(plot_path + ext)

    plt.close(fig)

    return (plot_def['label'], plot_path)


def process_errors_single(df):
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

    return (df, new_columns)


def process_errors(groups, base_path):
    is_relative = len(groups) > 1
    keys = set()
    dfs = []
    series_dicts = []
    sel_groups = []

    for group in groups:
        df = group.ls_df

        if is_relative:
            df = relativize(df)

        pair = process_errors_single(df)
        if not pair:
            continue

        df, series_dict = pair

        keys |= series_dict.keys()

        dfs.append(df)
        series_dicts.append(series_dict)
        sel_groups.append(group)

    fig_j = 0
    figs = {}
    for key in keys:
        tuple = plt.subplots()
        tuple += (fig_j, [])
        fig_j += 1
        figs[key] = tuple

    for i in range(len(dfs)):
        df = dfs[i]
        series_dict = series_dicts[i]
        group = sel_groups[i]
        color = _colors[i % len(_colors)]

        for key in keys:
            data = {'client_id': df.client_id, key: series_dict[key]}

            x_df = pd.DataFrame(data)
            w_ids = x_df.client_id.unique()

            fig, ax, fig_j, labels = figs[key]
            alpha = 2 * 1.0 / len(w_ids)

            for w_id in w_ids:
                plot_df = x_df[x_df.client_id == w_id]

                plot_df.plot.line(
                    y=key, ax=ax, legend=False, color=color, alpha=alpha)

                labels.append(
                    group.prefix_label(key + _per_worker) if w_id ==
                    w_ids[0] else '_')

    fig_infos = []

    for key in keys:
        fig, ax, fig_j, labels = figs[key]

        ax.legend(labels)

        naked_path = base_path
        if fig_j > 0:
            naked_path += '_' + str(fig_j)

        for ext in _savefig_exts:
            fig.savefig(naked_path + ext)

        fig_infos.append(FigureInfo(key, naked_path, _savefig_exts))

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

    naked_client_count_path = plot_client_count([group],
                                                base_path + '_client_count')

    num_requests_base_path = base_path + '_num_requests'
    if not plot_num_requests([group], num_requests_base_path):
        num_requests_base_path = None

    errors_fig_infos = process_errors([group], base_path + '_errors')

    zkm_plot_infos = []
    if len(zkm_df) > 0:
        for plot_def in _zkm_plots:
            zkm_plot_info = plot_zkm_multi([group], plot_def, base_path)
            zkm_plot_infos.append(zkm_plot_info)

    write_md(ls_df, task_set, op, md_path, latencies_base_path,
             naked_client_count_path, num_requests_base_path, errors_fig_infos,
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

    naked_client_count_path = plot_client_count(groups,
                                                base_path + '_client_count')

    num_requests_base_path = base_path + '_num_requests'
    if not plot_num_requests(groups, num_requests_base_path):
        num_requests_base_path = None

    errors_fig_infos = process_errors(groups, base_path + '_errors')

    for plot_def in _zkm_plots:
        zkm_plot_info = plot_zkm_multi(groups, plot_def, base_path)


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
    is_unique = len(groups) == 1

    if is_unique:
        groups[0].is_unique = True

    if is_unique:
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
