# A small locustfile which "messes" with the ZooKeeper ensemble.
#
# It can be run without an ensemble manipulation program, in which
# case it will use the "_coded_manipulation_example" function below, or
# with a "program" such as:
#
#     MANIPULATION_PROGRAM='
#         poll_initial_hatch_complete 500
#         sleep 10000
#         disable_leader
#         sleep 5000
#         enable_all
#     '
#
# Notes:
#
#  1. Such programs run "in a loop"; consequently, the above normally
#     causes a new election every ~15s;
#
#  2. Disabling and enabling ensemble members is very environment-
#     specific; please see the ZK_DISPATCH_ENABLE_SCRIPT and
#     ZK_DISPATCH_DISABLE_SCRIPT environment variables for
#     configuration.
#
# Example invocation:
#
#     ./parameterized-locust.sh \
#         --hosts "$Q3_HOSTS" \
#         --multi 4 \
#         --multi-workdir "$REPORT_DIR/clients" \
#         --kazoo-handler gevent \
#         --kazoo-timeout-s 60 \
#         --ignore-connection-down \
#         --min-wait 25 \
#         --max-wait 50 \
#         --stats-collect 100 \
#         --zk-metrics-collect 100 \
#         --report-dir "$REPORT_DIR" \
#         --zk-dispatch-program "$MANIPULATION_PROGRAM" \
#         --force \
#         -- \
#             --no-web \
#             -c 16 -r 128 -t 120s \
#             -f locust_set_with_dispatcher.py

import os
import logging

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKSetTaskSet
from zk_dispatch import register_dispatcher

logging.basicConfig()
logging.getLogger('zk_dispatch').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

register_extra_stats()
register_zk_metrics()


def _coded_manipulation_example(controller, members, **kwargs):
    controller.wait_initial_hatch_complete()
    controller.sleep_ms(10000)

    while True:
        controller.disable_leader(members)
        controller.sleep_ms(1000)
        controller.enable_all(members)
        controller.sleep_ms(9000)


if os.getenv('ZK_DISPATCH_PROGRAM'):
    register_dispatcher()
else:
    register_dispatcher(fn=_coded_manipulation_example)


class Set(ZKLocust):
    task_set = ZKSetTaskSet
