# A small locustfile which dynamically controls the number of
# ZooKeeper clients.
#
# It can be run without a control program, in which case it will use
# the "_coded_example" function below, or with a program such as:
#
#     CONTROL_PROGRAM='
#         poll_initial_hatch_complete 500
#         set_min_num_clients 16
#         set_max_num_clients 512
#         sleep 5000
#         multiply_num_clients <>1.75
#     '
#
# Note that the above runs "in a loop"; consequently, it repeatedly
# sleeps for 5s before multiplying the number of clients by a factor.
# The '<>' prefix signifies that the factor "bounces" between 1.75 and
# 1/1.75 when min_num_clients resp. max_num_clients are reached.
#
# Example invocation:
#
#     ./parameterized-locust.sh \
#         --hosts "$Q3_HOSTS" \
#         --multi 16 \
#         --multi-workdir "$REPORT_DIR/clients" \
#         --kazoo-handler gevent \
#         --kazoo-timeout-s 60 \
#         --ignore-connection-down \
#         --min-wait 25 \
#         --max-wait 50 \
#         --stats-collect 100 \
#         --zk-metrics-collect 100 \
#         --report-dir "$REPORT_DIR" \
#         --control-program "$CONTROL_PROGRAM" \
#         --force \
#         -- \
#             --no-web \
#             -c 16 -r 128 -t 120s \
#             -f locust_set_with_controller.py

import os
import logging

import gevent

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from locust_extra.control import register_controller

from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKSetTaskSet

logging.basicConfig()
logging.getLogger('locust_extra.control').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


def _coded_example(controller):
    controller.wait_initial_hatch_complete()

    sleep_s = 5
    min_num_clients = controller.get_num_clients()
    max_num_clients = min_num_clients * 32
    base_f = 1.75
    num_clients = min_num_clients
    while True:
        _logger.debug('Sleeping %ds', sleep_s)
        gevent.sleep(sleep_s)

        old_num_clients = controller.get_num_clients()
        if old_num_clients <= min_num_clients:
            f = base_f
        elif old_num_clients >= max_num_clients:
            f = 1 / base_f

        num_clients = max(
            min(int(old_num_clients * f), max_num_clients), min_num_clients)
        delta = num_clients - old_num_clients

        _logger.debug('Changing client count: %d -> %d (%d)', old_num_clients,
                      num_clients, delta)
        controller.start_hatching(num_clients=num_clients, hatch_rate=delta)


register_zk_metrics()
register_extra_stats()

register_controller(
    fn=None if os.getenv('LOCUST_EXTRA_CONTROL_PROGRAM') else _coded_example)


class Set(ZKLocust):
    task_set = ZKSetTaskSet
