#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Nathanael C. Fritz
# All Rights Reserved
#
# This software is licensed as described in the README file,
# which you should have received as part of this distribution.
#

# from ez_setup import use_setuptools
from distutils.core import setup
import sys

# if 'cygwin' in sys.platform.lower():
#     min_version = '0.6c6'
# else:
#     min_version = '0.6a9'
#
# try:
#     use_setuptools(min_version=min_version)
# except TypeError:
#     # locally installed ez_setup won't have min_version
#     use_setuptools()
#
# from setuptools import setup, find_packages, Extension, Feature

VERSION          = '1.0.1.0'
DESCRIPTION      = 'Thoonk is a clusterable, Redis based, Publish-Subscribe, Queue, and Job Distrubtion system based on the philosophies of XMPP Pubsub (XEP-0060).'
LONG_DESCRIPTION      = 'Thoonk is a clusterable, Redis based, Publish-Subscribe, Queue, and Job Distrubtion system based on the philosophies of XMPP Pubsub (XEP-0060).'

CLASSIFIERS      = [ 'Intended Audience :: Developers',
                     'License :: OSI Approved :: MIT',
                     'Programming Language :: Python',
                     'Topic :: Software Development :: Libraries :: Python Modules',
                   ]

packages     = [ 'thoonk', 'thoonk/feeds' ]


setup(
    name             = "thoonk",
    version          = VERSION,
    description      = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    author       = 'Nathanael Fritz',
    author_email = 'fritzy [at] netflint.net',
    url          = 'http://github.com/fritzy/thoonk.py',
    license      = 'MIT',
    platforms    = [ 'any' ],
    packages     = packages,
    scripts      = ['scripts/thoonk-cli'],
    py_modules   = [],
    requires     = ['redis'],
    )

