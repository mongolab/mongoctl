# The MIT License

# Copyright (c) 2012 ObjectLabs Corporation

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

###############################################################################
# Imports
###############################################################################
import os

from setuptools import setup

###############################################################################
# CONSTANTS
###############################################################################
DOT_MONGOCTL_DIR = os.path.join(os.path.expanduser( "~"), ".mongoctl")


###############################################################################
# Setup
###############################################################################
setup(
    name='mongoctl',
    version='0.1.0',
    author='ObjectLabs staff',
    author_email='staff@objectlabs.com',
    description='Mongo Control',
    long_description="Controls the Mongo",
    packages=['mongoctl',
              'mongoctl/tests',
              'mongoctl/tests/testing_conf',
              'mongoctl/minify_json'],
    package_data = {'mongoctl.tests.testing_conf':
                        ['mongoctl.config',
                         'servers.config',
                         'clusters.config']},
    test_suite="mongoctl.tests.test_suite",
    include_package_data=True,
    scripts=['bin/mongoctl'],
    data_files=[
        (DOT_MONGOCTL_DIR, ['conf/sample_mongoctl.config']),
        (DOT_MONGOCTL_DIR, ['conf/sample_servers.config']),
        (DOT_MONGOCTL_DIR, ['conf/sample_clusters.config'])
    ],
    url='http://objectlabs.org',
    ##license='LICENSE.txt',
    install_requires=[
        'dargparse==0.1.0',
        'dampier-pymongo==2.1.1',
        'verlib==0.1',
        'prettytable==0.6'],
    dependency_links=[
        "https://github.com/dampier/mongo-python-driver/tarball/master#egg=dampier-pymongo-2.1.1",
        "https://github.com/objectlabs/dargparse/tarball/master#egg=dargparse-0.1.0"
    ]

)
