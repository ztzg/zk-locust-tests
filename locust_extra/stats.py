import os
import logging

from itertools import chain

import gevent

import locust.runners
from locust.stats import sort_stats

from .output import format_timestamp, ensure_output

_logger = logging.getLogger(__name__)

_csv_path = os.getenv('LOCUST_EXTRA_STATS_CSV')
_delay_ms = int(os.getenv('LOCUST_EXTRA_STATS_COLLECT', '0'))

_percentiles = [0.5, 0.66, 0.75, 0.80, 0.90, 0.95, 0.98, 0.99, 1.00]

_columns = [
    'timestamp',
    'Method',
    'Name',
    '# requests',
    '# failures',
    'Median response time',
    'Average response time',
    'Min response time',
    'Max response time',
    'Average Content Size',
    'Requests/s',
] + ["%.2d%%" % int(f * 100) for f in _percentiles]


def write_meta_locked(output):
    output.w.writerow(_columns)
    output.keys = _columns
    output.num_requests = 0


def collect_extra_stats(csv_path):
    timestamp = format_timestamp()

    locust_runner = locust.runners.locust_runner
    if not locust_runner:
        return

    output = ensure_output(csv_path, for_csv=True)
    if not output.f:
        return

    if not hasattr(output, 'keys'):
        with output.lock:
            if not hasattr(output, 'keys'):
                write_meta_locked(output)

    total = locust_runner.stats.total
    num_requests = total.num_requests
    if num_requests == output.num_requests:
        # Not using > in case stats were reset.
        return

    request_stats = sort_stats(locust_runner.request_stats)
    for s in chain(request_stats, [total]):
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

    with output.lock:
        output.f.flush()
        output.num_requests = num_requests


def collect_extra_stats_loop(csv_path, delay_s):
    while True:
        collect_extra_stats(csv_path)
        gevent.sleep(delay_s)


def register_extra_stats(csv_path=_csv_path, delay_ms=_delay_ms):
    if csv_path and delay_ms > 0:
        _logger.info(
            "Writing extra stats to '%s'; delay %dms" % (csv_path, delay_ms))
        gevent.spawn(collect_extra_stats_loop, csv_path, delay_ms / 1000.0)
