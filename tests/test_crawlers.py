from unittest import TestCase

from bitter.crawlers import TwitterWorker, TwitterQueue

class TestWorker(TestCase):
    

    def test_worker(self):
        w = TwitterWorker()
