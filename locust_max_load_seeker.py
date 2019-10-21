# An "heavy" and very experimental "locustfile" which seeks a "stable"
# point of equilibrium while loading an ensemble.
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
#         --force \
#         -- \
#             --no-web \
#             -c 64 -r 128 -t 240s \
#             -f locust_max_load_seeker.py
#
# can generate "interesting" response curves such as the ones in
# `doc/locust_max_load_binary_search.html`.  The "ZK Client Count"
# plot shows an intense initial ramp-up of ZK Locust clients, followed
# by a decrease as the error rate spikes.  The script then tries and
# continually adjusts the ramp-up/ramp-down rate.
#
# Note that this is an initial, mostly-untested proof-of-concept at
# this point.  The analysis ought to be quite a bit smarter, and more
# metrics should be considered (reasonable latencies, outstanding
# requests, etc.).

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

from zk_locust.task_sets import ZKSetTaskSet
from zk_dispatch import register_dispatcher

_disable_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_DISABLE_MS', '3000'))
_enable_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_ENABLE_MS', '250'))
_adjust_ms = int(os.getenv('ZK_LOCUST_BENCH_WAIT_ADJUST_MS', '3000'))

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
            num_clients = max(num_clients, num_workers)

            if num_clients != act_clients:
                _logger.info(
                    'Adjusting client count (%+d); derr=%r, dt=%gs, f=%r, '
                    'act_clients=%r, num_clients=%r',
                    num_clients - act_clients, derr, dt, f, act_clients,
                    num_clients)

                _hatch_complete_event.clear()

                controller.start_hatching(
                    num_clients=num_clients, hatch_rate=num_clients)

                # Wait for new "generation."  KLUDGE: Mostly.  Locust
                # 0.11.0 is broken and often sends multiple
                # "hatch_complete" notifications.  The next step is a
                # "big" sleep, so let's hope that will compensate.
                _hatch_complete_event.wait()

                exp_clients = num_clients


register_controller(fn=_locust_clients_manager)


def _locust_stats_handler(*, worker_id, errors, **kwargs):
    # Ignore per-worker details for now.
    if worker_id:
        return

    if not errors:
        return

    at = time.time()

    with _errors_lock:
        global _errors_pair
        _errors_pair = (at, errors)

    # _logger.debug('Stats updated')


register_extra_stats(fn=_locust_stats_handler)
register_zk_metrics()


class Set(ZKLocust):
    task_set = ZKSetTaskSet
