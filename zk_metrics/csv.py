import os
import json

from locust_extra.output import format_timestamp, ensure_output

_metrics_csv_path = os.getenv('ZK_LOCUST_ZK_METRICS_CSV')


def write_metrics_csv_meta_locked(output, tree):
    keys = []
    for key in tree:
        keys.append(key)
    output.w.writerow(['timestamp', 'host_port'] + keys)
    output.keys = keys


def write_metrics_csv(host_port, data, csv_path):
    row = [format_timestamp(), host_port]

    output = ensure_output(csv_path, for_csv=True)
    if not output.f:
        return

    tree = json.loads(data) if data else None

    if not hasattr(output, 'keys'):
        if not tree or tree.get('error'):
            return
        with output.lock:
            if not hasattr(output, 'keys'):
                write_metrics_csv_meta_locked(output, tree)

    for key in output.keys:
        value = tree.get(key) if tree else None
        if value is None:
            row.append('')
        else:
            row.append(str(value))

    with output.lock:
        output.w.writerow(row)
        output.f.flush()


def maybe_write_metrics_csv(host_port, data):
    if _metrics_csv_path:
        write_metrics_csv(host_port, data, _metrics_csv_path)
