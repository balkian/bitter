import time 
import datetime
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
        self._lock = Lock()
        self.busy = False
        self.limits = self.client.application.rate_limit_status()

    def is_limited(self, uriparts):
        limit = self.get_limit(uriparts)
        if limit and limit['remaining'] <=0:
            t = datime.datetime.now()
            delta = limit['reset'] -  t
            if delta < datetime.timedelta(seconds=1):
                return True
        return False

    def get_wait(self, uriparts):
        limits = self.get_limit(uriparts)
        if limits['remaining'] > 0:
            return 0
        reset = datetime.datetime.fromtimestamp(limits.get('reset', 0))
        now = datetime.datetime.now()
        return max(0, (reset-now).total_seconds())

    def get_limit(self, uriparts):
        uri = '/'+'/'.join(uriparts)
        return self.limits.get('resources', {}).get(uriparts[0], {}).get(uri, {})

    def set_limit(self, uriparts, value):
        uri = '/'+'/'.join(uriparts)
        if 'resources' not in self.limits:
            self.limits['resources'] = {}
        resources = self.limits['resources']
        if uriparts[0] not in resources:
            resources[uriparts[0]] = {}
        resource = resources[uriparts[0]]
        resource[uri] = value

    def update_limits(self, uriparts, remaining, reset, limit):
        self.set_limit(uriparts, {'remaining': remaining,
                                  'reset': reset,
                                  'limit': limit})
        
    def update_limits_from_headers(self, uriparts, headers):
        reset = float(headers.get('X-Rate-Limit-Reset', time.time() + 30))
        remaining = int(headers.get('X-Rate-Limit-Remaining', 0))
        limit = int(headers.get('X-Rate-Limit-Limit', -1))
        self.update_limits(uriparts=uriparts, remaining=remaining, reset=reset, limit=limit)



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
                c = self.next(uriparts)
                c._lock.acquire()
                c.busy = True
                logger.debug('Next: {}'.format(c.name))
                ping = time.time()
                resp = getattr(c.client, "/".join(uriparts))(*args, **kwargs)
                pong = time.time()
                c.update_limits_from_headers(uriparts, resp.headers)
                logger.debug('Took: {}'.format(pong-ping))
                return resp
            except TwitterHTTPError as ex:
                if ex.e.code in (429, 502, 503, 504):
                    logger.info('{} limited'.format(c.name))
                    c.update_limits_from_headers(uriparts, ex.e.headers)
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

    def _next(self, uriparts):
        logger.debug('Getting next available')
        s = list(self.queue)
        random.shuffle(s)
        for worker in s:
            if not worker.is_limited(uriparts) and not worker.busy:
                return worker
        raise Exception('No worker is available')

    def next(self, uriparts):
        if not self.wait:
            return self._next(uriparts)
        while True:
            try:
                return self._next(uriparts)
            except Exception:
                available = filter(lambda x: not x.busy, self.queue)
                if available:
                    first_worker = min(available, key=lambda x: x.get_wait(uriparts))
                    diff = first_worker.get_wait(uriparts)
                    logger.info("All workers are throttled. Waiting %s seconds" % diff)
                else:
                    diff = 5
                    logger.info("All workers are busy. Waiting %s seconds" % diff)
                time.sleep(diff)

