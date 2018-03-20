from unittest import TestCase

import os
import types

from bitter import utils
from bitter import config as c

class TestUtils(TestCase):

    configfile = '/tmp/bitter.yaml'

    def setUp(self):
        c.CONFIG_FILE = self.configfile
        if os.path.exists(self.configfile):
            os.remove(self.configfile)
        assert not os.path.exists(self.configfile)
        utils.create_config_file(self.configfile)
        assert os.path.exists(self.configfile)
        
    def test_add_credentials(self):
        utils.add_credentials(self.configfile, user="test")
        assert utils.get_credentials(self.configfile)
        assert utils.get_credentials(self.configfile, user="test")
        assert list(utils.get_credentials(self.configfile, user="test"))[0]["user"] == "test"

    def test_get_credentials(self):
        utils.add_credentials(self.configfile, user="test")
        assert utils.get_credentials(self.configfile, user="test")
        assert not utils.get_credentials(self.configfile, user="test", inverse=True)

    def test_add_two_credentials(self):
        utils.add_credentials(self.configfile, user="test")
        utils.add_credentials(self.configfile, user="test2")
        assert utils.get_credentials(self.configfile, user="test")
        assert utils.get_credentials(self.configfile, user="test2")


    def test_delete_credentials(self):
        utils.add_credentials(self.configfile, user="test")
        assert utils.get_credentials(self.configfile, user="test")
        utils.delete_credentials(self.configfile, user="test")
        assert not utils.get_credentials(self.configfile, user="test")

    def test_parallel(self):
        import time
        def echo(i):
            time.sleep(0.5)
            return i
        tic = time.time()
        resp = utils.parallel(echo, [1,2,3])
        assert isinstance(resp, types.GeneratorType)
        assert list(resp) == [1,2,3]
        toc = time.time()
        assert (tic-toc) < 600
        resp2 = utils.parallel(echo, [1,2,3,4], chunksize=2)
        assert list(resp2) == [1,2, 3,4]


class TestUtilsEnv(TestUtils):
    configfile = None

    def setUp(self):
        if 'BITTER_CONFIG' in os.environ:
          self.oldenv = os.environ['BITTER_CONFIG']
        os.environ['BITTER_CONFIG'] = ''

    def tearDown(self):
        if hasattr(self, 'oldenv'):
            os.environ['BITTER_CONFIG'] = self.oldenv
