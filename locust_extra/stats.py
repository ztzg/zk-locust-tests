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
] + ["%.2d%%" % int(f * 100) for f in _percentiles]


def write_csv_header_locked(output):
    output.w.writerow(_columns)
    output.keys = _columns
    output.num_requests = 0


def write_csv_row(timestamp, client_id, s, user_count, output):
    if client_id:
        total_rps = None
        pcs = _no_percentiles
    else:
        total_rps = s.total_rps
        pcs = [s.get_response_time_percentile(f) for f in _percentiles]

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
    ] + pcs

    with output.lock:
        output.w.writerow(row)
        output.f.flush()


def write_jsonl_entry(timestamp, s, user_count, errors, output):
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

    if errors:
        info['errors'] = errors

    s = json.dumps(info, ensure_ascii=True, indent=None)

    with output.lock:
        output.f.write(s + '\n')
        output.f.flush()


def collect_extra_stats(stats_csv_path, distrib_path, client_id, client_data,
                        last_num_requests):
    timestamp = format_timestamp()

    locust_runner = locust.runners.locust_runner
    if not locust_runner:
        return

    user_count = locust_runner.user_count
    total = locust_runner.stats.total
    errors = locust_runner.stats.serialize_errors()
    num_requests = total.num_requests
    if num_requests == last_num_requests:
        # Not using > in case stats were reset.
        return

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

    client_entries = None
    if stats_output and client_id and client_data:
        client_entries = {}
        for stats_data in client_data["stats"]:
            entry = StatsEntry.unserialize(stats_data)
            request_key = (entry.name, entry.method)
            client_entries[request_key] = entry

    request_stats = sort_stats(locust_runner.request_stats)
    for s in chain(request_stats, [total]):
        if stats_output:
            if client_entries:
                request_key = (s.name, s.method)
                entry = client_entries.get(request_key)
                if entry:
                    write_csv_row(timestamp, client_id, entry, None,
                                  stats_output)

            write_csv_row(timestamp, None, s, user_count, stats_output)

        if distrib_output:
            write_jsonl_entry(timestamp, s, user_count,
                              errors if s is total else None, distrib_output)

    return num_requests


def collect_extra_stats_loop(stats_csv_path, distrib_path, delay_s):
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
                                               client_id, data, num_requests)

        locust.events.slave_report += on_slave_report
        # We are now hooked; let's abandon the greenlet.
        return

    # Not master; polling.
    while True:
        num_requests = collect_extra_stats(stats_csv_path, distrib_path, None,
                                           None, num_requests)
        gevent.sleep(delay_s)


def spawn_collector(stats_csv_path, distrib_path, delay_ms):
    gevent.spawn(collect_extra_stats_loop, stats_csv_path, distrib_path,
                 delay_ms / 1000.0)


def register_extra_stats(stats_csv_path=_stats_csv_path,
                         distrib_path=_distrib_path,
                         delay_ms=_delay_ms):
    if delay_ms > 0 and (stats_csv_path is not None
                         or distrib_path is not None):
        spawn_collector(stats_csv_path, distrib_path, delay_ms)
