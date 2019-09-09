import logging
import threading
import csv

from datetime import datetime

_logger = logging.getLogger(__name__)
_outputs = {}
_lock = threading.Lock()


class Output(object):
    lock = None
    f = None
    w = None


def ensure_output(path, for_csv=True):
    with _lock:
        output = _outputs.get(path)
        if output:
            return output

        output = Output()
        try:
            output.lock = threading.Lock()
            if for_csv:
                output.f = open(path, 'w', newline='')
                output.w = csv.writer(output.f)
            else:
                output.f = open(path, 'w')
        except OSError:
            _logger.exception("Creating output '%s'" % path)
        _outputs[path] = output
        return output


def format_timestamp(ts=None):
    if not ts:
        ts = datetime.utcnow()
    return ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
