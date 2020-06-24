"""
Bitter module. A library and cli for Twitter using python-twitter.
http://github.com/balkian/bitter
"""

import os

from .version import __version__
from . import config as bconf

def easy(conffile=bconf.CONFIG_FILE):
    from .crawlers import TwitterQueue

    return TwitterQueue.from_config(conffile=conffile)

__all__ = ['cli', 'config', 'crawlers', 'models', 'utils' ]


