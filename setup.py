import pip
from setuptools import setup
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
# pip 6 introduces the *required* session argument
try:
    install_reqs = parse_requirements("requirements.txt", session=pip.download.PipSession())
    py2_reqs = parse_requirements("requirements-py2.txt", session=pip.download.PipSession())
    test_reqs = parse_requirements("test-requirements.txt", session=pip.download.PipSession())
except AttributeError:
    install_reqs = parse_requirements("requirements.txt")
    py2_reqs = parse_requirements("requirements-py2.txt")
    test_reqs = parse_requirements("test-requirements.txt")

import sys
import os
import itertools
if sys.version_info <= (3, 0):
    install_reqs = itertools.chain(install_reqs, py2_reqs)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
install_reqs = [str(ir.req) for ir in install_reqs]
test_reqs = [str(ir.req) for ir in test_reqs]

with open(os.path.join('bitter', 'VERSION'), 'r') as f:
    __version__ = f.read().strip()

setup(
    name="bitter",
    packages=['bitter'],
    description=" Simplifying how researchers access Data. It includes a CLI and a library.",
    author='J. Fernando Sanchez',
    author_email='balkian@gmail.com',
    url="http://balkian.com",
    version=__version__,
    install_requires=install_reqs,
    tests_require=test_reqs,
    extras_require = {
        'server': ['flask', 'flask-oauthlib']
        },
    setup_requires=['pytest-runner',],
    include_package_data=True,
    entry_points="""
        [console_scripts]
        bitter=bitter.cli:main
    """,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ]
)
