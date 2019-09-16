import os
import logging
import json

from itertools import chain

import gevent

import locust.runners
from locust.stats import sort_stats

from .output import format_timestamp, ensure_output

_logger = logging.getLogger(__name__)

_stats_csv_path = os.getenv('LOCUST_EXTRA_STATS_CSV')
_distrib_path = os.getenv('LOCUST_EXTRA_STATS_DISTRIB')
_delay_ms = int(os.getenv('LOCUST_EXTRA_STATS_COLLECT', '0'))

_percentiles = [0.5, 0.66, 0.75, 0.80, 0.90, 0.95, 0.98, 0.99, 1.00]

_columns = [
    'timestamp',
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
] + ["%.2d%%" % int(f * 100) for f in _percentiles]


def write_csv_header_locked(output):
    output.w.writerow(_columns)
    output.keys = _columns
    output.num_requests = 0


def write_csv_row(timestamp, s, output):
    row = [
        timestamp,
        s.method,
        s.name,
        s.num_requests,
        s.num_failures,
        s.median_response_time,
        s.avg_response_time,
        s.min_response_time or 0,
        s.max_response_time,
        s.avg_content_length,
        s.total_rps,
    ] + [s.get_response_time_percentile(f) for f in _percentiles]

    with output.lock:
        output.w.writerow(row)
        output.f.flush()


def write_jsonl_entry(timestamp, s, errors, output):
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
        'response_times': s.response_times
    }

    if errors:
        info['errors'] = errors

    s = json.dumps(info, ensure_ascii=True, indent=None)

    with output.lock:
        output.f.write(s + '\n')
        output.f.flush()


def collect_extra_stats(stats_csv_path, distrib_path, last_num_requests):
    timestamp = format_timestamp()

    locust_runner = locust.runners.locust_runner
    if not locust_runner:
        return

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

    request_stats = sort_stats(locust_runner.request_stats)
    for s in chain(request_stats, [total]):
        if stats_output:
            write_csv_row(timestamp, s, stats_output)

        if distrib_output:
            write_jsonl_entry(timestamp, s, errors if s is total else None,
                              distrib_output)

    return num_requests


def collect_extra_stats_loop(stats_csv_path, distrib_path, delay_s):
    while not locust.runners.locust_runner:
        gevent.idle()
    if isinstance(locust.runners.locust_runner,
                  locust.runners.SlaveLocustRunner):
        return

    if stats_csv_path:
        _logger.info("Writing extra stats to CSV: '%s'; delay %ds" %
                     (stats_csv_path, delay_s))
    if distrib_path:
        _logger.info("Writing full distributions to: '%s'; delay %ds" %
                     (distrib_path, delay_s))

    num_requests = 0
    while True:
        num_requests = collect_extra_stats(stats_csv_path, distrib_path,
                                           num_requests)
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
