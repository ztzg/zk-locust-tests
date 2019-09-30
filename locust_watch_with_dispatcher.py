# A small locustfile to troubleshoot Kazoo a connection loss issue
# matching this ticket:
#
#     https://github.com/python-zk/kazoo/issues/374
#
# Run with the following "perturbing" program:
#
#     export ZK_DISPATCH_PROGRAM='
#         poll_initial_hatch_complete 500
#         sleep 10000
#         disable_leader
#         sleep 5000
#         enable_all
#     '
#
# E.g.:
#
#     ./parameterized-locust.sh \
#         --hosts "$Q3_HOSTS" \
#         --multi 6 \
#         --multi-workdir "$REPORT_DIR/clients" \
#         --kazoo-handler gevent \
#         --kazoo-timeout-s 60 \
#         --min-wait 250 \
#         --max-wait 500 \
#         --zk-dispatch-program "$ZK_DISPATCH_PROGRAM" \
#         -- \
#             --reset-stats --no-web \
#             -c 6 -r 128 -t 120s \
#             -f locust_watch_with_dispatcher.py \
#     2>&1 | tee "$REPORT_DIR/locust.log"

import logging

import gevent.lock

# from kazoo.protocol.states import EventType

from zk_locust import ZKLocust, ZKLocustTaskSequence
from locust_extra.stats import register_extra_stats
from zk_metrics import register_zk_metrics

from zk_dispatch import register_dispatcher

logging.basicConfig()
logging.getLogger('zk_dispatch').setLevel(logging.DEBUG)
logging.getLogger('zk_metrics').setLevel(logging.DEBUG)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# register_extra_stats()
# register_zk_metrics()
register_dispatcher()


class WatchTaskSet(ZKLocustTaskSequence):
    def __init__(self, parent, *, name='watch', **kwargs):
        super(WatchTaskSet, self).__init__(parent, **kwargs)

        self.do_ephemeral()

        self.tasks = [self.do_watch]

        self._sem = gevent.lock.Semaphore(value=0)

    def do_ephemeral(self):
        try:
            path = self.client.create_default_node()
            _logger.debug("Created ephemeral '%s'", path)
        except Exception:
            _logger.exception('Creating ephemeral')
            raise

    def do_watch(self):
        try:
            path = self.client.join_path('/blackout')
            stat = self.client.get_zk_client().exists(
                path, watch=self.watch_trigger)
            _logger.debug("Watching '%s', %s", path, stat)
            self._sem.acquire()
        except Exception:
            _logger.exception('Watching /blackout')
            raise

    def watch_trigger(self, event):
        _logger.debug('Trigger %s', event)
        # if event.type != EventType.NONE:
        self._sem.release()


class Watch(ZKLocust):
    task_set = WatchTaskSet
