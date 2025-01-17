# An "heavy-hitting" Locust script which seeks a "stable" point of
# equilibrium while loading an ensemble whose availability varies.
#
# An invocation such as this one:
#
#     ./parameterized-locust.sh \
#         --hosts "$ENSEMBLE_HOSTS" \
#         --multi 64 \
#         --multi-workdir "$REPORT_DIR/workers" \
#         --kazoo-handler gevent \
#         --kazoo-timeout-s 60 \
#         --ignore-connection-down \
#         --min-wait 25 \
#         --max-wait 50 \
#         --stats-collect 100 \
#         --zk-metrics-collect 100 \
#         --report-dir "$REPORT_DIR" \
#         --bench-wait-disable-ms 5000 \
#         --bench-wait-enable-ms 500 \
#         --bench-wait-adjust-ms 4500 \
#         --force \
#         -- \
#             --no-web \
#             -c 64 -r 128 -t 240s \
#             -f locust_max_load_seeker.py
#
# can generate "interesting" response curves such as the ones in
# `doc/locust_max_load_binary_search.html` (produced using an
# experimental version of this script).  The "ZK Client Count" plot
# shows an intense initial ramp-up of ZK Locust clients, followed by a
# decrease as the error rate spikes.  The script then tries and
# continually adjusts the ramp-up/ramp-down rate.
#
# Note that this is still a relatively early version of the benchmark;
# the analysis ought to be quite a bit smarter, and more metrics
# should be considered (reasonable latencies, outstanding requests,
# etc.).

import math
import collections
import os
import time
import logging

import gevent.thread
import gevent.queue
import gevent.event

from locust import events

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from locust_extra.control import register_controller
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKGetTaskSet, ZKSetTaskSet, ZKConnectTaskSet
from zk_dispatch import register_dispatcher

_modify_ensemble = int(os.getenv('ZK_LOCUST_BENCH_MODIFY_ENSEMBLE', '1')) != 0

_disable_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_DISABLE_MS', '3000'))
_enable_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_ENABLE_MS', '250'))
_adjust_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_ADJUST_MS', '3000'))

_hatch_rate = float(os.getenv('ZK_LOCUST_BENCH_HATCH_RATE', '0'))
_hatch_duration_s = float(os.getenv('ZK_LOCUST_BENCH_HATCH_DURATION_S', '0'))

_op_set = int(os.getenv('ZK_LOCUST_BENCH_OP_SET', '1')) != 0
_op_get = int(os.getenv('ZK_LOCUST_BENCH_OP_GET', '1')) != 0
_op_connect = int(os.getenv('ZK_LOCUST_BENCH_OP_CONNECT', '1')) != 0

_logging_level = logging.DEBUG

logging.basicConfig()
logging.getLogger('zk_dispatch').setLevel(_logging_level)
logging.getLogger('zk_metrics').setLevel(_logging_level)
logging.getLogger('locust_extra.control').setLevel(_logging_level)

# Ignore connection issues when trying to gather metrics
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

_logger = logging.getLogger(__name__)
_logger.setLevel(_logging_level)

_ensemble_queue = gevent.queue.Queue(maxsize=1)
_clients_queue = gevent.queue.Queue(maxsize=1)
_hatch_complete_event = gevent.event.Event()

_errors_pair = None
_errors_lock = gevent.thread.LockType()


class IrregularSeries(object):
    def __init__(self):
        self._raw = []
        self._base = None
        self._interp = []

    def record(self, at, sample):
        z = len(self._raw)
        if z == 0:
            # Initial sample
            self._raw.append((at, sample))
            self._base = int(math.ceil(at))
        else:
            prev_at, prev_value = self._raw[z - 1]

            # Cumsum
            value = prev_value + sample
            self._raw.append((at, value))

            # Interp
            r_at = at - self._base
            r_prev_at = prev_at - self._base
            t0 = int(math.ceil(r_prev_at))
            t1 = int(math.floor(r_at))
            if t1 == int(math.ceil(r_at)):
                t1 -= 1
            dt = r_at - r_prev_at
            for t in range(t0, t1 + 1):
                f = (t - r_prev_at) / dt
                iv = prev_value * (1 - f) + value * f
                self._interp.append(iv)

    def get_interp(self):
        return (self._base, self._interp)


_stats_info = collections.defaultdict(IrregularSeries)
_stats_lock = gevent.thread.LockType()


def _zk_ensemble_manager(controller, members, **kwargs):
    controller.wait_initial_hatch_complete()

    while True:
        # Wait for "continue" signal
        _clients_queue.get()
        controller.disable_leader(members)
        controller.sleep_ms(_enable_ms)
        controller.enable_all(members)
        # Send "continue" token
        _ensemble_queue.put(True)


if _modify_ensemble:
    register_dispatcher(fn=_zk_ensemble_manager)


def _count_errors(errors_map):
    if not errors_map:
        return 0

    # We only care about session expiration for now.
    return errors_map.get("SessionExpiredError()", 0)


def _compute_error_rate(last_tuple):
    with _errors_lock:
        errors_pair = _errors_pair

    if not errors_pair:
        return (0, 0, None)

    at, errors_map = errors_pair
    count = _count_errors(errors_map)
    next_tuple = (at, count)

    if not last_tuple:
        return (0, 0, next_tuple)

    last_at, last_count = last_tuple

    dt = at - last_at
    derr = count - last_count
    if dt <= 0 or derr < 0:
        _logger.warn(
            'Unexpected error values dt=%d, derr=%d, '
            'last_tuple=%s, errors_pair=%s', dt, derr, repr(last_tuple),
            repr(errors_pair))
        return (0, 0, None)

    return (derr, dt, next_tuple)


def _locust_clients_manager(controller):
    controller.wait_initial_hatch_complete()

    def on_hatch_complete(user_count):
        _logger.debug('Hatch complete; user_count=%r', user_count)
        _hatch_complete_event.set()

    events.hatch_complete += on_hatch_complete

    last_error_mark = None

    num_workers = controller.get_num_workers()
    exp_clients = controller.get_user_count()

    base_f = 2
    f = base_f

    max_new_clients = num_workers * 64

    while True:
        controller.sleep_ms(_disable_ms)

        if _modify_ensemble:
            # Send "continue" token
            _clients_queue.put(True)
            # Wait for "continue" signal
            _ensemble_queue.get()

        controller.sleep_ms(_adjust_ms)

        act_clients = controller.get_user_count()
        _logger.debug('Current client count: %d', act_clients)

        derr, dt, last_error_mark = _compute_error_rate(last_error_mark)

        has_errors = derr > 0
        if has_errors:
            _logger.info('Noticed %r new errors in %gs', derr, dt)

        has_dead_clients = act_clients < exp_clients
        if has_dead_clients:
            _logger.info('Noticed %r dead clients; exp_clients=%r',
                         exp_clients - act_clients, exp_clients)

        if has_errors or has_dead_clients:
            # Reduce rate, so that we won't come back so fast
            base_f = max(base_f * 3 / 4, 1 + 1 / max_new_clients)

        if has_errors:
            # And back off
            f = max(1 / base_f, 3 / 4)
        else:
            f = base_f

        exp_clients = act_clients

        if not has_dead_clients:
            num_clients = int(act_clients * f)
            num_clients = min(num_clients, act_clients + max_new_clients)
            # At least one new client per worker.
            num_clients = max(num_clients, act_clients + num_workers)

            if _hatch_rate > 0:
                hatch_rate = _hatch_rate
            elif _hatch_duration_s > 0:
                hatch_rate = (num_clients - act_clients) / _hatch_duration_s
            else:
                hatch_rate = max(num_clients, 128)

            _logger.info(
                'Adjusting client count (%+d); derr=%r, dt=%gs, f=%r, '
                'act_clients=%r, num_clients=%r, hatch_rate=%r',
                num_clients - act_clients, derr, dt, f, act_clients,
                num_clients, hatch_rate)

            _hatch_complete_event.clear()

            controller.start_hatching(
                num_clients=num_clients, hatch_rate=hatch_rate)

            # Wait for new "generation."  KLUDGE: Mostly.  Locust
            # 0.11.0 is broken and often sends multiple
            # "hatch_complete" notifications.  The next step is a
            # "big" sleep, so let's hope that will compensate.
            _hatch_complete_event.wait()

            exp_clients = num_clients


register_controller(fn=_locust_clients_manager)


def _locust_stats_handler(*, worker_id, stats, errors, **kwargs):
    at = time.time()

    if worker_id:
        # Handle stats on a per-worker basis.
        with _stats_lock:
            key = (worker_id, stats.name, stats.method)
            series = _stats_info[key]
            series.record(at, stats.num_requests)
    elif errors:
        # Handle errors globally.
        with _errors_lock:
            global _errors_pair
            _errors_pair = (at, errors)


register_extra_stats(fn=_locust_stats_handler)
register_zk_metrics()


class LocustBase(ZKLocust):
    def __init__(self, *args, **kwargs):
        super(LocustBase, self).__init__(*args, **kwargs)


if _op_get:
    class Get(LocustBase):
        task_set = ZKGetTaskSet


if _op_set:
    class Set(LocustBase):
        task_set = ZKSetTaskSet


if _op_connect:
    class Connect(LocustBase):
        task_set = ZKConnectTaskSet

        def __init__(self):
            # Unlike other locust instances, this one must not "autostart"
            # the ZK client.
            super(Connect, self).__init__(pseudo_root=None, autostart=False)
