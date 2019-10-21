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

import time
import logging

import gevent.thread
import gevent.queue
import gevent.event

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from locust_extra.control import register_controller
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKSetTaskSet
from zk_dispatch import register_dispatcher

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
_stats_event = gevent.event.Event()

_errors_pair = None
_errors_lock = gevent.thread.LockType()


def _zk_ensemble_manager(controller, members, **kwargs):
    controller.wait_initial_hatch_complete()

    while True:
        # Wait for "continue" signal
        _clients_queue.get()
        controller.disable_leader(members)
        controller.sleep_ms(1000)
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

    last_error_mark = None

    num_workers = controller.get_num_workers()
    exp_clients = controller.get_user_count()

    base_f = 4
    f = base_f

    max_new_clients = num_workers * 64

    while True:
        controller.sleep_ms(5000)

        # Send "continue" token
        _clients_queue.put(True)

        # Wait for "continue" signal
        _ensemble_queue.get()

        controller.sleep_ms(5000)

        # Note: We have to look at the actual running count, which is
        # "published" at the same time as statistics--so we wait for
        # the next report.
        _stats_event.clear()
        _stats_event.wait()
        act_clients = controller.get_user_count()
        _logger.debug('Current client count: %d', act_clients)

        generation = controller.get_generation()

        derr, dt, last_error_mark = _compute_error_rate(last_error_mark)

        is_failing = False

        if derr > 0:
            _logger.info('Noticed %r new errors in %gs', derr, dt)
            is_failing = True

        if act_clients < exp_clients:
            _logger.info('Noticed %r failed clients; exp_clients=%r',
                         exp_clients - act_clients, exp_clients)
            is_failing = True

        if is_failing:
            # Reduce rate, so that we won't come back so fast
            base_f = max(base_f / 4, 1 + 1 / max_new_clients)
            # And back off
            f = max(1 / base_f, 3 / 4)
        elif derr == 0:
            f = base_f

        exp_clients = act_clients

        num_clients = int(act_clients * f)
        num_clients = min(num_clients, act_clients + max_new_clients)
        num_clients = max(num_clients, num_workers)

        if num_clients != act_clients:
            _logger.debug(
                'Adjusting client count (%r); derr=%r, dt=%gs, f=%r, '
                'act_clients=%r, num_clients=%r', num_clients - act_clients,
                derr, dt, f, act_clients, num_clients)

            controller.start_hatching(
                num_clients=num_clients, hatch_rate=num_clients)

            if num_clients > act_clients:
                # Wait for new "generation."
                while controller.get_generation() == generation:
                    controller.sleep_ms(250)

            exp_clients = num_clients


register_controller(fn=_locust_clients_manager)


def _locust_stats_handler(*, worker_id, errors, **kwargs):
    # Ignore per-worker details for now.
    if worker_id:
        return

    # Unlock any waiter.
    _stats_event.set()

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
