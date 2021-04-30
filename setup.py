#!/usr/bin/env python3

from setuptools import setup

setup(
    name='fastdbfs',
    version='0.2',
    description="Interactive command line client for Databricks DBFS",
    url="http://github.com/salva/fastdbfs",
    author="Salvador Fandiño García",
    author_email="sfandino@yahoo.com",
    license='GPLv3+',
    license_files = ('LICENSE.txt',),
    packages=['fastdbfs'],
    install_requires=[ "progressbar2", "aiohttp" ],
    entry_points={'console_scripts': ['fastdbfs=fastdbfs.runner:run']}
);
