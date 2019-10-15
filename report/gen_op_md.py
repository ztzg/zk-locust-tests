#!/usr/bin/env python3

from abc import ABCMeta, abstractmethod

import sys
import os.path
import logging
import json
import warnings

import distutils.util

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
        'ylabel': 'Count',
        'name': 'outstanding_requests',
        'metrics': ['outstanding_requests'],
        'ignore_not_serving': True
    },
    {
        'label': 'Clients',
        'ylabel': 'Count',
        'name': 'clients',
        'metrics': ['num_alive_connections'],
        'ignore_not_serving': True
    },
    {
        'label': 'Nodes',
        'ylabel': 'Count',
        'name': 'nodes',
        'metrics': ['znode_count', 'ephemerals_count'],
        'ignore_not_serving': True
    },
    {
        'label': 'Watches',
        'ylabel': 'Count',
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


class FigInfo(object):
    def __init__(self, fig, title):
        self.fig = fig
        self.title = title


class SavedFigInfo(object):
    def __init__(self, fig_info, naked_path, exts):
        self.fig_info = fig_info
        self.naked_path = naked_path
        self.exts = exts


def write_md(df, task_set, op, md_path, latencies_base_path,
             client_count_fig_infos, request_frequency_fig_infos,
             errors_fig_infos, zkm_fig_infos):
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
            f.write('\n### Operation Latencies\n\n')
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

        if request_frequency_fig_infos:
            f.write('\n#### ZK Client Requests\n\n')
            for saved_fig_info in request_frequency_fig_infos:
                f.write('\n![](%s)\n' % saved_fig_info.naked_path)

        if client_count_fig_infos:
            f.write('\n#### ZK Client Count\n\n')
            for saved_fig_info in client_count_fig_infos:
                f.write('\n![](%s)\n' % saved_fig_info.naked_path)

        if zkm_fig_infos:
            f.write('\n### ZooKeeper Metrics\n\n')
            for saved_fig_info in zkm_fig_infos:
                f.write('\n#### %s\n\n' % saved_fig_info.fig_info.title)
                f.write('\n![](%s)\n' % saved_fig_info.naked_path)

        f.write('\n')


def option_getter(options, section):
    def raw_getter(xkey):
        v = options.get('.'.join(xkey))
        if v is not None:
            return v
        xkey[0] = '*'
        return options.get('.'.join(xkey))

    def getter(*args, type=None, fallback=None):
        xkey = [section] + list(args)
        v = raw_getter(xkey)
        if v is None:
            return fallback
        if type is None or type is str:
            return v
        if type is int:
            return int(v)
        if type is bool:
            return distutils.util.strtobool(v)
        raise ValueError(f"Don't know how to convert '{v}' into type {type}.")

    return getter


def relativize(df, *, index_base=None):
    if index_base is None:
        index_base = df.index.min()

    return df.set_index((df.index - index_base).total_seconds())


def vsubplots(nrows):
    figsize = (_figsize[0], _figsize[0] / 3 * nrows)
    return plt.subplots(nrows=nrows, figsize=figsize)


def worker_alpha(n):
    if n <= 2:
        return 1.0 / 3  # Keep some transparency
    return 2.0 / n


def set_ax_labels(ax, *, x_is_relative=False, y_label=None):
    ax.set_xlabel('Time (s)' if x_is_relative else 'Clock')
    if y_label:
        ax.set_ylabel(y_label)


class AbstractPlotter(metaclass=ABCMeta):
    @abstractmethod
    def plot(self, groups):
        pass

    def plot_and_save(self, groups, base_path, *, exts=None):
        fig_infos = self.plot(groups)
        return self.save(fig_infos, base_path, exts=exts)

    def save(self, fig_infos, base_path, exts=None):
        n = len(fig_infos)
        saved_fig_infos = []

        if exts is None:
            exts = _savefig_exts

        for i in range(n):
            fig_info = fig_infos[i]

            naked_path = base_path if n == 1 else f'{base_path}_{i + 1}'
            for ext in exts:
                fig_info.fig.savefig(naked_path + ext)

            saved_fig_infos.append(SavedFigInfo(fig_info, naked_path, exts))

        return saved_fig_infos


class LatenciesPlotter(AbstractPlotter):
    def __init__(self, options={}):
        get_option = option_getter(options, 'latencies')

        self._shade = get_option('shade', type=bool, fallback=True)

    def plot(self, groups):
        fig = plt.figure()
        ax = fig.gca()
        title = 'Operation Latencies'

        fig.suptitle(title)

        is_relative = len(groups) > 1

        shaded_pcs = ['66%', '75%', '80%', '90%', '98%']
        highlighted_pcs = [('50%', '-'), ('95%', '--'), ('99%', ':')]

        for i in range(len(groups)):
            is_main = i == 0
            group = groups[i]
            df = group.merged_client_stats()
            color = _colors[i % len(_colors)]

            if is_relative:
                df = relativize(df)

            if is_main and self._shade:
                # Only shade first group.
                for pc in shaded_pcs:
                    ax.fill_between(
                        df.index,
                        0,
                        df[pc],
                        facecolor=color,
                        alpha=0.1,
                        label=group.prefix_label(pc))

            for (pc, linestyle) in highlighted_pcs:
                df.plot.line(
                    y=pc,
                    color=color,
                    linestyle=linestyle,
                    ax=ax,
                    label=group.prefix_label(pc))

        set_ax_labels(ax, x_is_relative=is_relative, y_label='Latency (ms)')

        return [FigInfo(fig, title)]


def plot_latencies(groups, latencies_base_path, options):
    plotter = LatenciesPlotter(options)

    return plotter.plot_and_save(groups, latencies_base_path)


class ClientCountPlotter(AbstractPlotter):
    def __init__(self, options={}):
        get_option = option_getter(options, 'client_count')

        self._per_worker = get_option('per_worker', type=bool, fallback=True)

    def plot(self, groups):
        is_relative = len(groups) > 1

        fig = plt.figure()
        title = 'ZK Client Count'

        fig.suptitle(title)

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

            if not self._per_worker:
                continue

            w_ids = group.client_ids()
            if len(w_ids) < 2:
                continue

            alpha = worker_alpha(len(w_ids))

            for j in range(len(w_ids)):
                w_id = w_ids[j]
                ws_df = group.ls_df
                pick_rows = ws_df['client_id'] == w_id
                w_df = ws_df.loc[pick_rows, col_names]

                if is_relative:
                    w_df = relativize(w_df, index_base=index_base)

                w_df.plot.line(ax=ax, color=color, linestyle=':', alpha=alpha)
                labels.append(
                    group.prefix_label('ZK C.' +
                                       _per_worker) if j == 0 else '_')

        if any(not l.startswith('_') for l in labels):
            ax.legend(labels)
        else:
            ax.get_legend().set_visible(False)

        set_ax_labels(ax, x_is_relative=is_relative, y_label='Count')

        return [FigInfo(fig, title)]


def plot_client_count(groups, naked_client_count_path, options):
    plotter = ClientCountPlotter(options)

    return plotter.plot_and_save(groups, naked_client_count_path)


class RequestFrequencyPlotter(AbstractPlotter):
    def __init__(self, options={}):
        get_option = option_getter(options, 'request_frequency')

        self._per_worker = get_option('per_worker', type=bool, fallback=True)

    def _plot_num_requests_per_1s(self, groups, dfs, dfs_per_worker):
        fig, axes = vsubplots(3)
        title = 'ZK Client Requests'

        fig.suptitle(title)

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

            if self._per_worker and dfs_per_worker and dfs_per_worker[
                    i] and len(dfs_per_worker[i]) > 1:
                all_dfs += dfs_per_worker[i]
                alpha = worker_alpha(len(dfs_per_worker[i]))

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
            ax.set_ylabel('Count')
            ax.xaxis.label.set_visible(False)
            ax.tick_params(axis='x', which='both', labelbottom=False)

        fail_ax.legend()

        for label in fail_ax.get_xticklabels():
            label.set_ha("right")
            label.set_rotation(30)
        fig.subplots_adjust(bottom=0.2)

        set_ax_labels(fail_ax, x_is_relative=is_relative, y_label='Count')

        return [FigInfo(fig, title)]

    def _plot_num_requests_multi(self, groups):
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
        dfs_per_worker = []

        for i in range(len(sel_groups)):
            group = sel_groups[i]
            df = group.unmerged_client_stats()
            w_ids = group.client_ids()

            min_t = df.index.min()
            max_t = df.index.max()

            # Using a fine grid to lower aliasing.  TODO(ddiederen): It
            # would be better to use something more robust, here; perhaps
            # fit a spline and sample that?
            fine_idx = pd.date_range(min_t, max_t, freq='25ms')
            nidx = pd.date_range(min_t, max_t, freq='1s')

            x_df = pd.DataFrame(index=nidx, columns=columns)
            x_df = x_df.fillna(0)

            x_w_dfs = []

            for client_id in w_ids:
                w_df = df.loc[df['client_id'] == client_id, columns]
                w_df = w_df.cumsum()
                w_df = w_df.reindex(
                    w_df.index.union(fine_idx)).interpolate().reindex(nidx)

                x_df += w_df

                if self._per_worker:
                    x_w_dfs.append(w_df)

            dfs.append(x_df)
            if self._per_worker:
                dfs_per_worker.append(x_w_dfs)

        return self._plot_num_requests_per_1s(sel_groups, dfs, dfs_per_worker)

    def plot(self, groups):
        can_multi = True
        for group in groups:
            clients_df = group.unmerged_client_stats()
            if not len(clients_df):
                can_multi = False
                break

        if can_multi:
            return self._plot_num_requests_multi(groups)

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

        return self._plot_num_requests_per_1s(sel_groups, dfs, None)


def plot_request_frequency(groups, base_path, options):
    plotter = RequestFrequencyPlotter(options)

    return plotter.plot_and_save(groups, base_path)


class ZooKeeperMetricsPlotter(AbstractPlotter):
    def __init__(self, plot_def, options={}):
        self._def = plot_def

    def plot(self, groups):
        plot_def = self._def

        title = 'ZooKeeper ' + plot_def['label']

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

        is_relative = len(dfs) > 1

        if is_relative:
            for group_j in range(len(dfs)):
                dfs[group_j] = relativize(dfs[group_j])

        host_ports = list(host_ports)
        n = len(host_ports)

        fig, axes = vsubplots(n)

        fig.suptitle(title)

        metrics = plot_def['metrics']
        ylabel = plot_def['ylabel']

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
                for metric in metrics:
                    df.plot(y=metric, ax=ax, color=color, **kwargs)

                    label = host_port
                    if len(metrics) > 1:
                        label += ', ' + metric
                    labels.append(group.prefix_label(label))

                    kwargs['linestyle'] = ':'  # KLUDGE.

            if any(not l.startswith('_') for l in labels):
                ax.legend(labels)
            else:
                ax.get_legend().set_visible(False)

            if (host_i < n - 1):
                ax.tick_params(axis='x', which='both', labelbottom=False)
                ax.set_xlabel(host_port)
                ax.set_ylabel(ylabel)
            else:
                set_ax_labels(ax, x_is_relative=is_relative, y_label=ylabel)
                ax.set_xlabel(host_port + ', ' + ax.get_xlabel())

        return [FigInfo(fig, title)]


def plot_zkm_multi(groups, plot_def, base_path, options):
    plotter = ZooKeeperMetricsPlotter(plot_def, options)
    plot_path = base_path + '_' + plot_def['name']

    return plotter.plot_and_save(groups, plot_path)


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
        fig, ax = plt.subplots()

        fig.suptitle(f'{key} Errors')

        figs[key] = (fig, ax, fig_j, [])
        fig_j += 1

    for i in range(len(dfs)):
        df = dfs[i]
        series_dict = series_dicts[i]
        group = sel_groups[i]
        color = _colors[i % len(_colors)]

        for key in keys:
            series = series_dict.get(key)
            if series is None:
                continue

            fig, ax, fig_j, labels = figs[key]

            data = {'client_id': df.client_id, key: series}

            x_df = pd.DataFrame(data)
            w_ids = x_df.client_id.unique()

            x_df[key].rolling(
                len(w_ids), center=True).sum().plot.line(
                    ax=ax, legend=False, color=color)

            if len(w_ids) < 2:
                continue

            labels.append(group.prefix_label(key + ' (Total)'))

            alpha = worker_alpha(len(w_ids))

            for w_id in w_ids:
                plot_df = x_df[x_df.client_id == w_id]

                plot_df.plot.line(
                    y=key,
                    ax=ax,
                    legend=False,
                    color=color,
                    linestyle=':',
                    alpha=alpha)

                labels.append(
                    group.prefix_label(key + _per_worker) if w_id ==
                    w_ids[0] else '_')

    fig_infos = []

    for key in keys:
        fig, ax, fig_j, labels = figs[key]

        if labels:
            ax.legend(labels)

        set_ax_labels(ax, x_is_relative=is_relative, y_label='Count')

        naked_path = base_path
        if fig_j > 0:
            naked_path += '_' + str(fig_j)

        for ext in _savefig_exts:
            fig.savefig(naked_path + ext)

        fig_infos.append(
            SavedFigInfo(FigInfo(fig, key), naked_path, _savefig_exts))

        plt.close(fig)

    return fig_infos


def process_task_set_op_single(task_set, op, group, base_path, md_path,
                               options):
    ls_df = group.ls_df
    zkm_df = group.zkm_df
    ls_merged_df = group.merged_client_stats()

    latencies_base_path = None
    if len(ls_merged_df) > 0:
        latencies_base_path = base_path + '_latencies'
        plot_latencies([group], latencies_base_path, options)

    client_count_fig_infos = plot_client_count(
        [group], base_path + '_client_count', options)

    request_frequency_fig_infos = plot_request_frequency(
        [group], base_path + '_num_requests', options)

    errors_fig_infos = process_errors([group], base_path + '_errors')

    zkm_fig_infos = []
    if len(zkm_df) > 0:
        for plot_def in _zkm_plots:
            fis = plot_zkm_multi([group], plot_def, base_path, options)
            zkm_fig_infos += fis

    write_md(ls_df, task_set, op, md_path, latencies_base_path,
             client_count_fig_infos, request_frequency_fig_infos,
             errors_fig_infos, zkm_fig_infos)


def process_task_set_op_multi(task_set, op, groups, base_path, md_path,
                              options):
    latencies_base_path = None
    latencies_groups = []
    for group in groups:
        if len(group.merged_client_stats()) > 0:
            latencies_base_path = base_path + '_latencies'
            latencies_groups.append(group)

    if latencies_base_path:
        plot_latencies(latencies_groups, latencies_base_path, options)

    client_count_fig_infos = plot_client_count(
        groups, base_path + '_client_count', options)

    request_frequency_fig_infos = plot_request_frequency(
        groups, base_path + '_num_requests', options)

    errors_fig_infos = process_errors(groups, base_path + '_errors')

    for plot_def in _zkm_plots:
        plot_zkm_multi(groups, plot_def, base_path, options)


def load_group(base_input_path, data_item):
    sample_id = data_item.get('id') or None
    ls_csv_path = os.path.join(base_input_path, data_item['locust-stats'])
    ls_df = pd.read_csv(ls_csv_path, index_col=0, parse_dates=True)
    zkm_csv_path = os.path.join(base_input_path, data_item['zk-metrics'])
    zkm_df = pd.read_csv(zkm_csv_path, index_col=0, parse_dates=True)

    return Group(sample_id, ls_df, zkm_df)


def process_task_set_op(base_input_path, task_set, op, data, base_path,
                        md_path, options):
    groups = [load_group(base_input_path, data_item) for data_item in data]
    is_unique = len(groups) == 1

    if is_unique:
        groups[0].is_unique = True

    if is_unique:
        process_task_set_op_single(task_set, op, groups[0], base_path, md_path,
                                   options)
    else:
        process_task_set_op_multi(task_set, op, groups, base_path, md_path,
                                  options)


def process_fragments(base_input_path, fragments, base_path, md_path, options):
    frag_dict = {}
    for fragment in fragments:
        key = (fragment['task_set'], fragment['op'])
        frag_dict[key] = frag_dict.get(key, []) + fragment['data']

    for (task_set, op), data in frag_dict.items():
        process_task_set_op(base_input_path, task_set, op, data, base_path,
                            md_path, options)


def main(executable, metadata, base_path, md_path):
    with open(metadata) as f:
        lines = f.readlines()

    fragments = [json.loads(line) for line in lines]
    options = {}

    process_fragments('.', fragments, base_path, md_path, options)


if __name__ == '__main__':
    main(*sys.argv)
