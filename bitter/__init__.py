"""
Bitter module. A library and cli for Twitter using python-twitter.
http://github.com/balkian/bitter
"""

import os

from .version import __version__

def easy(*args, **kwargs):
    from .crawlers import TwitterQueue
    return TwitterQueue.from_credentials(*args, **kwargs)

__all__ = ['cli', 'config', 'crawlers', 'models', 'utils' ]


