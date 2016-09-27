"""
Bitter module. A library and cli for Twitter using python-twitter.
http://github.com/balkian/bitter
"""

try:
    from future.standard_library import install_aliases
    install_aliases()
except ImportError:
    # Avoid problems at setup.py and py3.x
    pass

import os

from .version import __version__

__all__ = ['cli', 'config', 'crawlers', 'models', 'utils' ]
