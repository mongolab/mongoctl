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
