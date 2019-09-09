import requests
import re
import json

from flask import Blueprint, render_template, abort, Response
from jinja2 import TemplateNotFound

from locust import __version__ as version
from locust.web import app

from zk_locust import get_zk_hosts, split_zk_hosts

from .csv import maybe_write_metrics_csv
from .defs import metric_defs

_zk_hosts = split_zk_hosts(get_zk_hosts())
_zk_re_port = re.compile(r":\d{1,4}$")
_zk_metrics_scheme = 'http'
_zk_metrics_port = 8080

_page = Blueprint(
    'zk-metrics',
    __name__,
    template_folder='templates',
    static_folder='static')


@_page.route('/')
def ui():
    try:
        return render_template(
            'zk_metrics.html',
            version=version,
            zk_locust_hosts=','.join(_zk_hosts))
    except TemplateNotFound:
        abort(404)


@_page.route('/defs.js')
def defs():
    s = json.dumps(metric_defs, ensure_ascii=True)

    return 'loadServerMetricDefinitions(' + s + ')'


@_page.route('/proxy/<command>/<int:index>')
def proxy(command, index):
    if command != 'monitor' or index < 0 or index >= len(_zk_hosts):
        abort(400)

    host_port = _zk_re_port.sub("", _zk_hosts[index]) + ':' + \
        str(_zk_metrics_port)
    url = _zk_metrics_scheme + '://' + host_port + '/commands/' + command

    r = requests.get(url, allow_redirects=False, stream=False)

    maybe_write_metrics_csv(host_port, r.content)

    return Response(r.content, r.status_code, [])


def register_zk_metrics_page(url_prefix='/zk-metrics'):
    app.register_blueprint(_page, url_prefix=url_prefix)
    # print(app.url_map)
