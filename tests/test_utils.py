from unittest import TestCase

import os
import types

from bitter import utils
from bitter import config as c

class TestUtils(TestCase):

    def setUp(self):
        self.credfile = '/tmp/credentials.txt'
        c.CREDENTIALS = self.credfile
        if os.path.exists(self.credfile):
            os.remove(self.credfile)
        utils.create_credentials(self.credfile)
        

    def test_create_credentials(self):
        assert os.path.exists(self.credfile)
        os.remove(self.credfile)
        utils.create_credentials() # From config
        assert os.path.exists(self.credfile)

    def test_add_credentials(self):
        utils.add_credentials(self.credfile, user="test")
        assert utils.get_credentials(self.credfile)
        assert utils.get_credentials(user="test")
        assert list(utils.get_credentials(user="test"))[0]["user"] == "test"

    def test_get_credentials(self):
        utils.add_credentials(self.credfile, user="test")
        assert utils.get_credentials(user="test")
        assert not utils.get_credentials(user="test", inverse=True)

    def test_add_two_credentials(self):
        utils.add_credentials(self.credfile, user="test")
        utils.add_credentials(self.credfile, user="test2")
        assert utils.get_credentials(user="test")
        assert utils.get_credentials(user="test2")


    def test_delete_credentials(self):
        utils.add_credentials(self.credfile, user="test")
        assert utils.get_credentials(user="test")
        utils.delete_credentials(user="test")
        print(utils.get_credentials())
        assert not utils.get_credentials(user="test")

    def test_parallel(self):
        import time
        def echo(i):
            time.sleep(2)
            return i
        tic = time.time()
        resp = utils.parallel(echo, [1,2,3])
        assert isinstance(resp, types.GeneratorType)
        assert list(resp) == [1,2,3]
        toc = time.time()
        assert (tic-toc) < 6000
        resp2 = utils.parallel(echo, [1,2,3,4], chunksize=2)
        assert list(resp2) == [1,2,3,4]
        
