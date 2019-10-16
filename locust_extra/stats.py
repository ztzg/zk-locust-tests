import os
import logging
import json

from itertools import chain

import gevent

import locust.runners
import locust.events
from locust.stats import sort_stats, StatsEntry

from .output import format_timestamp, ensure_output

_logger = logging.getLogger(__name__)

_stats_csv_path = os.getenv('LOCUST_EXTRA_STATS_CSV')
_distrib_path = os.getenv('LOCUST_EXTRA_STATS_DISTRIB')
_delay_ms = int(os.getenv('LOCUST_EXTRA_STATS_COLLECT', '0'))

_percentiles = [0.5, 0.66, 0.75, 0.80, 0.90, 0.95, 0.98, 0.99, 1.00]
_no_percentiles = [None for f in _percentiles]

_columns = [
    'timestamp',
    'client_id',
    'method',
    'name',
    'num_requests',
    'num_failures',
    'median_response_time',
    'avg_response_time',
    'min_response_time',
    'max_response_time',
    'avg_content_length',
    'total_rps',
    'user_count',
    'errors',
] + ["%.2d%%" % int(f * 100) for f in _percentiles]


def write_csv_header_locked(output):
    output.w.writerow(_columns)
    output.keys = _columns
    output.num_requests = 0


def write_csv_row(timestamp, client_id, s, e, user_count, output):
    if client_id:
        total_rps = None
        pcs = _no_percentiles
    else:
        total_rps = s.total_rps
        pcs = [s.get_response_time_percentile(f) for f in _percentiles]

    errors_json = None
    if e:
        errors_json = json.dumps(e, ensure_ascii=True, indent=None)

    row = [
        timestamp,
        client_id,
        s.method,
        s.name,
        s.num_requests,
        s.num_failures,
        s.median_response_time,
        s.avg_response_time,
        s.min_response_time or 0,
        s.max_response_time,
        s.avg_content_length,
        total_rps,
        user_count,
        errors_json,
    ] + pcs

    with output.lock:
        output.w.writerow(row)
        output.f.flush()


def write_jsonl_entry(timestamp, s, e, user_count, output):
    info = {
        'timestamp': timestamp,
        'method': s.method,
        'name': s.name,
        'num_requests': s.num_requests,
        'num_failures': s.num_failures,
        'median_response_time': s.median_response_time,
        'avg_response_time': s.avg_response_time,
        'min_response_time': s.min_response_time or 0,
        'max_response_time': s.max_response_time,
        'avg_content_length': s.avg_content_length,
        'total_rps': s.total_rps,
        'user_count': user_count,
        'response_times': s.response_times
    }

    if e:
        info['errors'] = e

    s = json.dumps(info, ensure_ascii=True, indent=None)

    with output.lock:
        output.f.write(s + '\n')
        output.f.flush()


def _classify_errors(errors_data):
    errors = {}
    for e_data in errors_data:
        op_key = (e_data['name'], e_data['method'])

        op_data = errors.get(op_key)
        if not op_data:
            errors[op_key] = op_data = {}

        e_key = e_data['error']
        op_data[e_key] = op_data.get(e_key, 0) + e_data['occurences']
    return errors


class ClientStats(object):
    def __init__(self, total, stats, errors, user_count):
        self.total = total
        self.stats = stats
        self.errors = errors
        self.user_count = user_count

    def stats_for(self, key):
        return self.stats.get(key)

    def errors_for(self, key):
        return self.errors.get(key)

    @classmethod
    def from_client_data(cls, data):
        total = StatsEntry.unserialize(data["stats_total"])
        stats = {}
        for stats_data in data["stats"]:
            entry = StatsEntry.unserialize(stats_data)
            key = (entry.name, entry.method)
            stats[key] = entry
        errors = _classify_errors(data["errors"].values())
        user_count = data["user_count"]
        return cls(total, stats, errors, user_count)


def _invoke_fn(fn, worker_id, stats, errors, user_count):
    fn(worker_id=worker_id, stats=stats, errors=errors, user_count=user_count)


def collect_extra_stats(stats_csv_path, distrib_path, fn, client_id,
                        client_data, last_num_requests):
    timestamp = format_timestamp()

    locust_runner = locust.runners.locust_runner
    if not locust_runner:
        return

    stats_total = locust_runner.stats.total
    num_requests = stats_total.num_requests
    if num_requests == last_num_requests:
        # Not using > in case stats were reset.
        return

    user_count = locust_runner.user_count
    errors_data = locust_runner.stats.serialize_errors()
    errors = _classify_errors(errors_data.values())

    stats_output = None
    distrib_output = None

    if stats_csv_path:
        stats_output = ensure_output(stats_csv_path, for_csv=True)
        if not stats_output.f:
            return

    if distrib_path:
        distrib_output = ensure_output(distrib_path, for_csv=False)
        if not distrib_output.f:
            return

    if stats_output and not hasattr(stats_output, 'keys'):
        with stats_output.lock:
            if not hasattr(stats_output, 'keys'):
                write_csv_header_locked(stats_output)

    client_stats = None
    if stats_output and client_id and client_data:
        client_stats = ClientStats.from_client_data(client_data)

    request_stats = sort_stats(locust_runner.request_stats)
    for s in chain(request_stats, [stats_total]):
        key = None if s is stats_total else (s.name, s.method)
        e = errors.get(key)

        if stats_output or fn:
            if client_stats:
                client_s = client_stats.stats_for(key)
                client_e = client_stats.errors_for(key)
                if client_s:
                    if fn:
                        _invoke_fn(fn, client_id, client_s, client_e,
                                   client_stats.user_count)
                    if stats_output:
                        write_csv_row(timestamp, client_id, client_s, client_e,
                                      client_stats.user_count, stats_output)

        if fn:
            _invoke_fn(fn, None, s, e, user_count)
        if stats_output:
            write_csv_row(timestamp, None, s, e, user_count, stats_output)

        if distrib_output:
            write_jsonl_entry(timestamp, s, e, user_count, distrib_output)

    return num_requests


def collect_extra_stats_loop(stats_csv_path, distrib_path, fn, delay_s):
    while not locust.runners.locust_runner:
        gevent.idle()
    if isinstance(locust.runners.locust_runner,
                  locust.runners.SlaveLocustRunner):
        return

    is_master = isinstance(locust.runners.locust_runner,
                           locust.runners.MasterLocustRunner)

    if is_master:
        info = 'mode: worker-triggered'
    else:
        info = 'mode: polling with delay %ds' % delay_s

    if stats_csv_path:
        _logger.info(
            "Writing extra stats to CSV: '%s'; %s" % (stats_csv_path, info))
    if distrib_path:
        _logger.info(
            "Writing full distributions to: '%s'; %s" % (distrib_path, info))

    num_requests = 0

    if is_master:

        def on_slave_report(client_id, data):
            nonlocal num_requests
            num_requests = collect_extra_stats(stats_csv_path, distrib_path,
                                               fn, client_id, data,
                                               num_requests)

        locust.events.slave_report += on_slave_report
        # We are now hooked; let's abandon the greenlet.
        return

    # Not master; polling.
    while True:
        num_requests = collect_extra_stats(stats_csv_path, distrib_path, fn,
                                           None, None, num_requests)
        gevent.sleep(delay_s)


def spawn_collector(stats_csv_path, distrib_path, fn, delay_ms):
    gevent.spawn(collect_extra_stats_loop, stats_csv_path, distrib_path, fn,
                 delay_ms / 1000.0)


def register_extra_stats(stats_csv_path=_stats_csv_path,
                         distrib_path=_distrib_path,
                         fn=None,
                         delay_ms=_delay_ms):
    has_delay = delay_ms > 0
    has_output = stats_csv_path is not None or distrib_path is not None
    has_fn = fn is not None

    if has_delay and (has_output or has_fn):
        spawn_collector(stats_csv_path, distrib_path, fn, delay_ms)
