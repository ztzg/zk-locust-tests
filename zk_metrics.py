import logging
import threading
import os
import json
import csv
import requests
import re

from datetime import datetime

from flask import Blueprint, render_template, abort, Response
from jinja2 import TemplateNotFound

from locust import __version__ as version
from locust.web import app

from common import get_zk_locust_hosts, parse_zk_hosts

_logger = logging.getLogger(__name__)

_zk_hosts = parse_zk_hosts(get_zk_locust_hosts())
_zk_re_port = re.compile(r":\d{1,4}$")
_zk_metrics_scheme = 'http'
_zk_metrics_port = 8080

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


page = Blueprint(
    'zk-metrics',
    __name__,
    template_folder='templates',
    static_folder='static')


@page.route('/')
def show():
    try:
        return render_template(
            'zk_metrics.html',
            version=version,
            zk_locust_hosts=','.join(_zk_hosts))
    except TemplateNotFound:
        abort(404)


@page.route('/proxy/<command>/<int:index>')
def proxy(command, index):
    if command != 'monitor' or index < 0 or index >= len(_zk_hosts):
        abort(400)

    host_port = _zk_re_port.sub("", _zk_hosts[index]) + ':' + \
        str(_zk_metrics_port)
    url = _zk_metrics_scheme + '://' + host_port + '/commands/' + command

    r = requests.get(url, allow_redirects=False, stream=False)

    if _metrics_csv_path:
        write_metrics_csv(host_port, r.content)

    return Response(r.content, r.status_code, [])


def register_page(url_prefix='/zk-metrics'):
    app.register_blueprint(page, url_prefix=url_prefix)
