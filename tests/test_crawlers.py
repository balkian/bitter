from unittest import TestCase

import os
import types
import datetime
import time

from bitter import utils, easy
from bitter.crawlers import QueueException
from bitter import config as c

class TestCrawlers(TestCase):

    def setUp(self):
        CONF_PATH = os.path.join(os.path.dirname(__file__), '.bitter.yaml')
        if os.path.exists(CONF_PATH):
            self.wq = easy(CONF_PATH)
        else:
            self.wq = easy()

    def test_create_worker(self):
        assert len(self.wq.queue)==1

    def test_get_limits(self):
        w1 = list(self.wq.queue)[0]
        print(w1.limits)
        limitslook = w1.get_limit(['statuses', 'lookup'])
        assert limitslook['remaining'] == limitslook['limit']

    def test_set_limits(self):
        w1 = list(self.wq.queue)[0]
        w1.set_limit(['test', 'test2'], {'remaining': 0})
        assert w1.get_limit(['test', 'test2']) == {'remaining': 0}

    def test_await(self):
        w1 = list(self.wq.queue)[0]
        w1.set_limit(['test', 'wait'], {'remaining': 0, 'reset': time.time()+2})
        assert w1.get_wait(['test', 'wait']) > 1
        time.sleep(2)
        assert w1.get_wait(['test', 'wait']) == 0
        assert w1.get_wait(['statuses', 'lookup']) == 0

    def test_is_limited(self):
        w1 = list(self.wq.queue)[0]
        assert not w1.is_limited(['statuses', 'lookup'])
        w1.set_limit(['test', 'limited'], {'remaining': 0, 'reset': time.time()+100})
        assert  w1.is_limited(['test', 'limited'])

    def test_call(self):
        w1 = list(self.wq.queue)[0]
        l1 = w1.get_limit(['users', 'lookup'])
        resp = self.wq.users.lookup(screen_name='balkian')
        l2 = w1.get_limit(['users', 'lookup'])
        assert l1['remaining']-l2['remaining'] == 1

    def test_consume(self):
        w1 = list(self.wq.queue)[0]
        l1 = w1.get_limit(['friends', 'list'])
        self.wq.wait = False
        for i in range(l1['remaining']):
            print(i)
            resp = self.wq.friends.list(screen_name='balkian')
        # l2 = w1.get_limit(['users', 'lookup'])
        # assert l2['remaining'] == 0
        # self.wq.users.lookup(screen_name='balkian')
        
        failed = False
        try:
            # resp = self.wq.friends.list(screen_name='balkian')
            self.wq.next(['friends', 'list'])
        except QueueException:
            failed = True
        assert failed
        l2 = w1.get_limit(['friends', 'list'])
        assert self.wq.get_wait(['friends', 'list']) > (l2['reset']-time.time())
        assert self.wq.get_wait(['friends', 'list']) < (l2['reset']-time.time()+2)
