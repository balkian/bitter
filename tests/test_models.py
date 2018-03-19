from unittest import TestCase

import os
import types

from bitter import utils
from bitter.models import *
from sqlalchemy import exists

class TestModels(TestCase):

    def setUp(self):
        self.session = make_session('sqlite://')

    def test_user(self):
        fake_user = User(name="Fake user", id=1548)
        self.session.add(fake_user)
        self.session.commit()
        fake_committed = self.session.query(User).filter_by(name="Fake user").first()
        assert fake_committed
        self.session.delete(fake_committed)
        self.session.commit()
        assert not list(self.session.execute('SELECT 1 from users where id=\'%s\'' % 1548))
