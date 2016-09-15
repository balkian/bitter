from unittest import TestCase

import os

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
        
        
        
