import sys
import queue
import logging
import traceback
from threading import Thread
from logging import StreamHandler
from multiprocessing import Queue, JoinableQueue

from pydat.ingest.config import LOGGER


class StatClient:
    '''
    Client for the worker processes to use for updating the stats
    '''
    def __init__(self, statqueue):
        self._queue = statqueue

    def addChanged(self, field):
        self._queue.put(('chn', field))

    def incr(self, field):
        self._queue.put(('stat', field))


class StatTracker(Thread):
    '''
    Thread to run the stat tracker in, the queue is passed along to the worker processes for communicating back
    '''
    def __init__(self, **kwargs):
        Thread.__init__(self, **kwargs)
        self._stats = {'total': 0,
                       'new': 0,
                       'updated': 0,
                       'unchanged': 0,
                       'duplicates': 0}
        self.stat_queue = Queue()
        self._shutdown = False
        self._changed = dict()

    @property
    def total(self):
        return self._stats['total']

    @property
    def new(self):
        return self._stats['new']

    @property
    def updated(self):
        return self._stats['updated']

    @property
    def unchanged(self):
        return self._stats['unchanged']

    @property
    def duplicates(self):
        return self._stats['duplicates']

    @property
    def changed_stats(self):
        return self._changed

    def seed(self, stats):
        self._stats = stats

    def seedChanged(self, changed):
        for (name, value) in changed.items():
            self._changed[name] = int(value)

    def shutdown(self):
        self._shutdown = True

    def run(self):
        while 1:
            try:
                (typ, field) = self.stat_queue.get(True, 0.2)
            except queue.Empty:
                if self._shutdown:
                    break
                continue

            if typ == 'stat':
                if field not in self._stats:
                    LOGGER.error("Unknown field %s" % (field))

                self._stats[field] += 1
            elif typ == 'chn':
                if field not in self._changed:
                    self._changed[field] = 0
                self._changed[field] += 1
            else:
                LOGGER.error("Unknown stat type")

        self.stat_queue.close()


class LogClient:
    '''
    Client for the worker processes to use for logging
    '''
    def __init__(self, name, logqueue, debug):
        self.name = name
        self._queue = logqueue
        self._logger = logging.getLogger()
        self._debug = debug
        self._prefix = None

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, value):
        if not isinstance(value, str):
            raise TypeError("Expected a string type")
        self._prefix = value

    def log(self, lvl, msg, *args, **kwargs):
        if self.prefix is not None and self._debug:
            msg = self.prefix + msg

        if kwargs.get('exc_info', False) is not False:
            if (not (isinstance(kwargs['exc_info'], tuple) and
                     len(kwargs['exc_info']) == 3)):
                kwargs['exc_info'] = sys.exc_info()
            (etype, eclass, tb) = kwargs['exc_info']
            exc_msg = ''.join(traceback.format_exception(etype,
                                                         eclass,
                                                         tb))
            kwargs['_exception_'] = exc_msg

        if kwargs.get('_exception_', None) is not None:
            msg += "\n%s" % (kwargs['_exception_'])

        (name, line, func, _) = self._logger.findCaller()
        log_data = (self.name, lvl, name, line, msg, args, None,
                    func, kwargs.get('extra', None))
        self._queue.put(log_data)

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs['_exception_'] = traceback.format_exc()
        self.log(logging.ERROR, msg, *args, **kwargs)


class CentralLogger(Thread):
    '''
    Central logger thread that receives log messages from all the worker processes over a queue
    '''
    def __init__(self, name=__name__, debug=False, **kwargs):
        Thread.__init__(self, **kwargs)
        self._debug = debug
        self.daemon = True
        self.name = name
        self.logQueue = JoinableQueue()
        self._logger = None
        self._stop = False

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(self.name)
        return self._logger

    def getLogger(self, name=__name__):
        return LogClient(name=name,
                         logqueue=self.logQueue,
                         debug=self._debug)

    def stop(self):
        self._stop = True

    def join(self):
        self.logQueue.join()

    def run(self):
        default_level = logging.INFO
        root_debug_level = logging.WARNING
        debug_level = logging.DEBUG
        root_default_level = logging.WARNING

        try:
            logHandler = StreamHandler(sys.stdout)
        except Exception as e:
            raise Exception(("Unable to setup logger to stdout\n"
                             "Error Message: %s\n") % str(e))

        if self._debug:
            log_format = ("%(levelname) -10s %(asctime)s %(funcName) "
                          "-20s %(lineno) -5d: %(message)s")
        else:
            log_format = ("%(message)s")

        logFormatter = logging.Formatter(log_format)

        # Set defaults for all loggers
        root_logger = logging.getLogger()
        root_logger.handlers = []
        logHandler.setFormatter(logFormatter)
        root_logger.addHandler(logHandler)

        if self._debug:
            root_logger.setLevel(root_debug_level)
        else:
            root_logger.setLevel(root_default_level)

        logger = logging.getLogger(self.name)

        if self._debug:
            logger.setLevel(debug_level)
        else:
            logger.setLevel(default_level)

        while 1:
            try:
                raw_record = self.logQueue.get(True, 0.2)
                try:
                    if logger.isEnabledFor(raw_record[1]):
                        logger.handle(logger.makeRecord(*raw_record))
                finally:
                    self.logQueue.task_done()
            except EOFError:
                break
            except queue.Empty:
                if self._stop:
                    break