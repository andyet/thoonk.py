#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2011 Nathanael C. Fritz
# All Rights Reserved
#
# This software is licensed as described in the README file,
# which you should have received as part of this distribution.
#
import os
import sys
import logging
import unittest
import distutils.core

from glob import glob
from os.path import splitext, basename, join as pjoin
from setuptools import setup


class TestCommand(distutils.core.Command):

    user_options = []

    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        testfiles = []
        exclude = ['__init__.py']
        for t in glob(pjoin('tests', '*.py')):
            if True not in [t.endswith(ex) for ex in exclude]:
                if basename(t).startswith('test_'):
                    testfiles.append('tests.%s' % splitext(basename(t))[0])

        suites = []
        for file in testfiles:
            __import__(file)
            suites.append(sys.modules[file].suite)

        tests = unittest.TestSuite(suites)
        runner = unittest.TextTestRunner(verbosity=2)

        # Disable logging output
        logging.basicConfig(level=100)
        logging.disable(100)

        result = runner.run(tests)
        return result


VERSION = '2.0.0'
DESCRIPTION = 'Thoonk is a clusterable, Redis based, Publish-Subscribe, Queue, and Job Distrubtion system based on the philosophies of XMPP Pubsub (XEP-0060).'
LONG_DESCRIPTION  = 'Thoonk is a clusterable, Redis based, Publish-Subscribe, Queue, and Job Distrubtion system based on the philosophies of XMPP Pubsub (XEP-0060).'

CLASSIFIERS = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(
    name             = "thoonk",
    version          = VERSION,
    description      = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    classifiers      = CLASSIFIERS,
    author       = 'Nathanael Fritz',
    author_email = 'fritzy [at] netflint.net',
    url          = 'http://github.com/fritzy/thoonk.py',
    license      = 'MIT',
    platforms    = ['any'],
    packages     = ['thoonk'],
    package_data = {'thoonk': ['scripts/**.lua']},
    include_package_data = True,
    requires     = ['redis'],
    cmdclass     = {'test': TestCommand}
)
