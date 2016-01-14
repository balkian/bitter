import pip
from setuptools import setup
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
# pip 6 introduces the *required* session argument
try:
    install_reqs = parse_requirements("requirements.txt", session=pip.download.PipSession())
    test_reqs = parse_requirements("test-requirements.txt", session=pip.download.PipSession())
except AttributeError:
    install_reqs = parse_requirements("requirements.txt")
    test_reqs = parse_requirements("test-requirements.txt")

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
install_reqs = [str(ir.req) for ir in install_reqs]
test_reqs = [str(ir.req) for ir in test_reqs]


setup(
    name="bitter",
    packages=['bitter'],
    description='''
    Simplifying how researchers access Data.
    It includes a CLI and a library.
    ''',
    author='J. Fernando Sanchez',
    author_email='balkian@gmail.com',
    url="http://balkian.com",
    version="0.2",
    install_requires=install_reqs,
    tests_require=test_reqs,
    include_package_data=True,
    entry_points="""
        [console_scripts]
        bitter=bitter.cli:main
    """
)
