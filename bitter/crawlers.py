import time 
import urllib
import random
import json

import logging
logger = logging.getLogger(__name__)

from twitter import *
from collections import OrderedDict
from threading import Lock
from . import utils
from . import config


class AttrToFunc(object):
    def __init__(self, uriparts=None, handler=None):
        if uriparts:
            self.__uriparts = uriparts
        else:
            self.__uriparts = []
        #self.__uriparts = []
        self.handler = handler

    def __getattr__(self, k):
        def extend_call(arg):
            return AttrToFunc(
                uriparts=self.__uriparts + [arg,],
                handler=self.handler)
        if k == "_":
            return extend_call
        else:
            return extend_call(k)

    def __call__(self, *args, **kwargs):
        # for i, a in enumerate(args)e
        #     kwargs[i] = a
        return self.handler(self.__uriparts, *args, **kwargs)

class TwitterWorker(object):
    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.throttled_time = False
        self._lock = Lock()
        self.busy = False

    @property
    def throttled(self):
        if not self.throttled_time:
            return False
        t = time.time()
        delta = self.throttled_time - t
        if delta > 0:
            return True
        return False

    def throttle_until(self, epoch=None):
        self.throttled_time = int(epoch)
        logger.info("Worker %s throttled for %s seconds" % (self.name, str(epoch-time.time())))


class TwitterQueue(AttrToFunc):
    def __init__(self, wait=True):
        logger.debug('Creating worker queue')
        self.queue = set()
        self.index = 0
        self.wait = wait
        AttrToFunc.__init__(self, handler=self.handle_call)

    def ready(self, worker):
        self.queue.add(worker)

    def handle_call(self, uriparts, *args, **kwargs):
        logger.debug('Called: {}'.format(uriparts))
        logger.debug('With: {} {}'.format(args, kwargs))
        while True:
            c = None
            try:
                c = self.next()
                c._lock.acquire()
                c.busy = True
                logger.debug('Next: {}'.format(c.name))
                ping = time.time()
                resp = getattr(c.client, "/".join(uriparts))(*args, **kwargs)
                pong = time.time()
                logger.debug('Took: {}'.format(pong-ping))
                return resp
            except TwitterHTTPError as ex:
                if ex.e.code in (429, 502, 503, 504):
                    limit = ex.e.headers.get('X-Rate-Limit-Reset', time.time() + 30)
                    logger.info('{} limited'.format(c.name))
                    c.throttle_until(limit)
                    continue
                else:
                    raise
            except urllib.error.URLError as ex:
                time.sleep(5)
                logger.info('Something fishy happened: {}'.format(ex))                
            finally:
                if c:
                    c.busy = False
                    c._lock.release()
                    

    @property
    def client(self):
        return self.next().client

    @classmethod
    def from_credentials(self, cred_file=None):
        wq = TwitterQueue()

        for cred in utils.get_credentials(cred_file):
            c = Twitter(auth=OAuth(cred['token_key'],
                                   cred['token_secret'],
                                   cred['consumer_key'],
                                   cred['consumer_secret']))
            wq.ready(TwitterWorker(cred["user"], c))
        return wq

    def _next(self):
        logger.debug('Getting next available')
        s = list(self.queue)
        random.shuffle(s)
        for worker in s:
            if not worker.throttled and not worker.busy:
                return worker
        raise Exception('No worker is available')

    def next(self):
        if not self.wait:
            return self._next()
        while True:
            try:
                return self._next()
            except Exception:
                available = filter(lambda x: not x.busy, self.queue)
                if available:
                    first_worker = min(available, key=lambda x: x.throttled_time)
                    diff = first_worker.throttled_time - time.time()
                    logger.info("All workers are throttled. Waiting %s seconds" % diff)
                else:
                    diff = 5
                    logger.info("All workers are busy. Waiting %s seconds" % diff)
                time.sleep(diff)

