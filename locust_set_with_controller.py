import logging

import gevent

from zk_locust import ZKLocust
from locust_extra.stats import register_extra_stats
from locust_extra.control import register_controller

from zk_metrics import register_zk_metrics

from zk_locust.task_sets import ZKSetTaskSet

logging.basicConfig()
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


def _controller_available(controller):
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


register_extra_stats()
register_controller(fn=_controller_available)
register_zk_metrics()


class Set(ZKLocust):
    task_set = ZKSetTaskSet
