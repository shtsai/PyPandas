# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

try:
    long_description = open("README.rst").read()
except IOError:
    long_description = ""

setup(
    name="PyPandas",
    version="0.2.2",
    description="A data cleaning framework for Spark",
    license="MIT",
    author="plliao, chiahsienlin, shtsai7",
    author_email="pll273@nyu.edu, clh566@nyu.edu, st3127@nyu.edu",
    url = 'https://github.com/shtsai7/PyPandas',
    download_url = 'https://github.com/shtsai7/PyPandas/archive/0.2.2.tar.gz',
    packages=find_packages(),
    install_requires=['pyspark'],
    long_description=long_description,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.4",
    ]
)
