import logging
import threading
import os
import json
import csv

from datetime import datetime

_logger = logging.getLogger(__name__)

_metrics_csv_path = os.getenv('ZK_LOCUST_ZK_METRICS_CSV')
_metrics_csv_info = None
_metrics_csv_lock = threading.Lock()


class MetricsCsvInfo(object):
    f = None
    w = None


def ensure_metrics_csv():
    global _metrics_csv_info
    if not _metrics_csv_info:
        with _metrics_csv_lock:
            if not _metrics_csv_info:
                info = MetricsCsvInfo()
                try:
                    info.f = open(_metrics_csv_path, 'w', newline='')
                    info.w = csv.writer(info.f)
                    _metrics_csv_info = info
                except OSError:
                    _logger.exception(
                        "Creating CSV writer for '%s'" % _metrics_csv_path)
                    _metrics_csv_info = info
    return _metrics_csv_info


def write_metrics_csv_meta(info, tree):
    keys = []
    for key in tree:
        keys.append(key)
    info.w.writerow(['timestamp', 'host_port'] + keys)
    info.keys = keys


def write_metrics_csv(host_port, data):
    row = [datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), host_port]

    info = ensure_metrics_csv()
    if not info.f:
        return

    tree = json.loads(data)

    if not hasattr(info, 'keys'):
        with _metrics_csv_lock:
            if not hasattr(info, 'keys'):
                write_metrics_csv_meta(info, tree)

    for key in info.keys:
        value = tree.get(key)
        if value is None:
            row.append('')
        else:
            row.append(str(value))

    with _metrics_csv_lock:
        info.w.writerow(row)
        info.f.flush()


def maybe_write_metrics_csv(host_port, data):
    if _metrics_csv_path:
        write_metrics_csv(host_port, data)
