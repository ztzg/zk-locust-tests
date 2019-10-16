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
#             -f locust_max_load_binary_search.py
#
# can generate "interesting" response curves such as the ones in
# `doc/locust_max_load_binary_search.html`.  The "ZK Client Count"
# plot shows an intense initial ramp-up of ZK Locust clients, followed
# by a strong decrease as the error rate spikes.  The script then
# tries and continually adjusts the ramp-up/ramp-down rate following
# the error rate.
#
# Note that this is an initial, mostly-untested proof-of-concept at
# this point.  The analysis ought to be quite a bit smarter, and more
# metrics should be considered (reasonable latencies, outstanding
# requests, etc.).

import time
import logging

import gevent.thread
import gevent.queue

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

_logger = logging.getLogger(__name__)
_logger.setLevel(_logging_level)

_ensemble_queue = gevent.queue.Queue(maxsize=1)
_clients_queue = gevent.queue.Queue(maxsize=1)

_errors_pair = None
_errors_lock = gevent.thread.LockType()


def _zk_ensemble_manager(controller, hosts_and_ports, quorum_size):
    controller.wait_initial_hatch_complete()

    while True:
        _clients_queue.get()
        _logger.debug('TODO: Degrade ensemble')
        controller.sleep_ms(1000)
        _logger.debug('TODO: Heal ensemble')
        controller.sleep_ms(7000)
        _ensemble_queue.put(True)


register_dispatcher(fn=_zk_ensemble_manager)


def _count_errors(errors_map):
    total_count = 0
    if errors_map:
        for count in errors_map.values():
            total_count += count
    return total_count


def _compute_error_rate(last_tuple):
    with _errors_lock:
        errors_pair = _errors_pair

    if not errors_pair:
        return (0, None)

    at, errors_map = errors_pair
    count = _count_errors(errors_map)
    next_tuple = (at, count)

    if not last_tuple:
        return (0, next_tuple)

    last_at, last_count = last_tuple

    delta_t = at - last_at
    delta_count = count - last_count
    if delta_t <= 0 or delta_count <= 0:
        return (0, None)

    return (delta_count / delta_t, next_tuple)


def _locust_clients_manager(controller):
    controller.wait_initial_hatch_complete()

    last_error_mark = None
    last_error_rate = 0
    last_num_clients = controller.get_num_clients()

    base_f = 4
    f = base_f

    while True:
        controller.sleep_ms(7000)

        current_num_clients = controller.get_num_clients()

        error_rate, last_error_mark = _compute_error_rate(last_error_mark)

        is_failing = False

        if error_rate > last_error_rate * 2:
            _logger.info('Noticed increased error rate')
            is_failing = True

        if current_num_clients < last_num_clients:
            _logger.info('Noticed %d dead clients',
                         last_num_clients - current_num_clients)
            is_failing = True

        if is_failing:
            # Reduce rate, so that we won't come back so fast
            base_f = max(base_f / 2, 1.125)
            # And back off
            f = 1 / base_f
        elif error_rate == 0 or error_rate < last_error_rate / 2:
            f = base_f

        num_clients = int(current_num_clients * f)
        delta = num_clients - current_num_clients

        if delta != 0:
            _logger.debug(
                f'Adjusting num_clients (%s), error_rate=%s, f=%s, current_num_clients=%s, num_clients=%s, delta=%s',
                'increase' if delta > 0 else 'decrease', error_rate, f,
                current_num_clients, num_clients, delta)

            controller.start_hatching(
                num_clients=num_clients, hatch_rate=abs(delta))

        last_error_rate = error_rate
        last_num_clients = num_clients

        controller.sleep_ms(1000)
        _clients_queue.put(True)
        _ensemble_queue.get()


register_controller(fn=_locust_clients_manager)


def _locust_stats_handler(*, worker_id=None, errors=None, **kwargs):
    # Ignore per-worker details for now.
    if worker_id:
        return

    if not errors:
        return

    at = time.time()

    with _errors_lock:
        global _errors_pair
        _errors_pair = (at, errors)


register_extra_stats(fn=_locust_stats_handler)
register_zk_metrics()


class Set(ZKLocust):
    task_set = ZKSetTaskSet
