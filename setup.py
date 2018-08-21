from setuptools import setup

def parse_requirements(filename):
    """ load requirements from a pip requirements file """
    with open(filename, 'r') as f:
        lineiter = list(line.strip() for line in f)
    return [line for line in lineiter if line and not line.startswith("#")]

install_reqs = parse_requirements("requirements.txt")
test_reqs = parse_requirements("test-requirements.txt")
extra_reqs = parse_requirements("extra-requirements.txt")

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
