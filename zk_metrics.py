import re
import requests

from flask import Blueprint, render_template, abort, Response
from jinja2 import TemplateNotFound

from locust import __version__ as version
from locust.web import app

from common import get_zk_locust_hosts, parse_zk_hosts

_zk_hosts = parse_zk_hosts(get_zk_locust_hosts())
_zk_re_port = re.compile(r":\d{1,4}$")
_zk_metrics_scheme = 'http'
_zk_metrics_port = 8080

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

    return Response(r.content, r.status_code, [])


def register_page(url_prefix='/zk-metrics'):
    app.register_blueprint(page, url_prefix=url_prefix)
