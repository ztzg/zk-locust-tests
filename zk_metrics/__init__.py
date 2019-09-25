import logging
import os
import requests
import re
import json

import gevent

from flask import Blueprint, render_template, abort, Response
from jinja2 import TemplateNotFound

from locust import __version__ as version
from locust.web import app
import locust.runners

from zk_locust import split_zk_hosts, split_zk_host_port

from .csv import maybe_write_metrics_csv
from .defs import metric_defs

_logger = logging.getLogger(__name__)

_zk_host_ports = split_zk_hosts()
_zk_metrics_scheme = 'http'
_zk_metrics_port = 8080

_zk_metrics_collect = os.getenv('ZK_LOCUST_ZK_METRICS_COLLECT', 'web')

_page = Blueprint(
    'zk-metrics',
    __name__,
    template_folder='templates',
    static_folder='static')


def compose_metrics_url(zk_host_port, command):
    host = split_zk_host_port(zk_host_port)[0]
    host_port = host + ':' + str(_zk_metrics_port)
    url = _zk_metrics_scheme + '://' + host_port + '/commands/' + command
    return url


def metrics_collect_loop(zk_host_port, url, delay_s):
    while not locust.runners.locust_runner:
        gevent.sleep(0.1)
    if isinstance(locust.runners.locust_runner,
                  locust.runners.SlaveLocustRunner):
        return

    while True:
        try:
            r = requests.get(url, allow_redirects=False, stream=False)
            r.raise_for_status()
            maybe_write_metrics_csv(zk_host_port, r.content)
        except Exception:
            _logger.exception('Metrics collect loop')
        gevent.sleep(delay_s)


@_page.route('/')
def ui():
    try:
        return render_template(
            'zk_metrics.html',
            version=version,
            zk_locust_hosts=','.join(_zk_host_ports))
    except TemplateNotFound:
        abort(404)


@_page.route('/defs.js')
def defs():
    s = json.dumps(metric_defs, ensure_ascii=True)

    return 'loadServerMetricDefinitions(' + s + ')'


@_page.route('/proxy/<command>/<int:index>')
def proxy(command, index):
    if command != 'monitor' or index < 0 or index >= len(_zk_host_ports):
        abort(400)

    zk_host_port = _zk_host_ports[index]
    url = compose_metrics_url(zk_host_port, command)

    r = requests.get(url, allow_redirects=False, stream=False)

    if (r.status_code == 200):
        maybe_write_metrics_csv(zk_host_port, r.content)

    return Response(r.content, r.status_code, [])


def register_zk_metrics_page(url_prefix='/zk-metrics'):
    app.register_blueprint(_page, url_prefix=url_prefix)
    # print(app.url_map)


def register_zk_metrics(url_prefix='/zk-metrics',
                        web=_zk_metrics_collect == 'web',
                        delay_ms=None):
    if web:
        register_zk_metrics_page(url_prefix=url_prefix)
    else:
        command = 'monitor'
        delay_s = (delay_ms or int(_zk_metrics_collect)) / 1000.0
        for zk_host_port in _zk_host_ports:
            url = compose_metrics_url(zk_host_port, command)
            gevent.spawn(metrics_collect_loop, zk_host_port, url, delay_s)
