#!/usr/bin/env python3

from setuptools import setup

import os
pkg_dir = os.path.abspath(os.path.dirname(__file__))
readme_fn = os.path.join(pkg_dir, 'README.md')
with open(readme_fn, encoding='utf-8') as file:
    long_description = file.read()

setup(
    name='fastdbfs',
    version='0.2',
    description="Interactive command line client for Databricks DBFS",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="http://github.com/salva/fastdbfs",
    author="Salvador Fandiño García",
    author_email="sfandino@yahoo.com",
    license='GPLv3+',
    license_files = ('LICENSE.txt',),
    packages=['fastdbfs'],
    install_requires=[ "progressbar2", "aiohttp" ],
    entry_points={'console_scripts': ['fastdbfs=fastdbfs.runner:run']}
);
