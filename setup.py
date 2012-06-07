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
import shutil
import stat
import pwd

from setuptools import setup

###############################################################################
# CONSTANTS
###############################################################################

SAMPLE_CONF_FILE_NAMES = ["mongoctl.config",
                          "servers.config",
                          "clusters.config"]

###############################################################################
# Calculating sample_conf_files
###############################################################################

def copy_sample_configs():
    

    # create the dot_mongoctl_dir if it does not exist
    # mode of folder is user RW and R for group and others
    try:
        login = os.getlogin()
        home_dir = os.path.expanduser( "~%s" % login)
        dot_mongoctl_dir = os.path.join(home_dir, ".mongoctl")
        owner = pwd.getpwnam(login)
        owner_uid = owner[2]
        owner_gid = owner[3]

        if not os.path.exists(dot_mongoctl_dir):
            os.makedirs(dot_mongoctl_dir)

        os.chown(dot_mongoctl_dir, owner_uid, owner_gid)
        os.chmod(dot_mongoctl_dir, 00755)

        for fname in SAMPLE_CONF_FILE_NAMES:
            data_file_path = os.path.join(dot_mongoctl_dir, fname)
            if not os.path.exists(data_file_path):
                src_file = os.path.join("sample_conf", fname)
                # copy file
                print ("Creating sample configuration file '%s' in '%s' " %
                       (fname,dot_mongoctl_dir))
                shutil.copyfile(src_file, data_file_path)
                # make file writable
                os.chown(data_file_path, owner_uid, owner_gid)
                os.chmod(data_file_path, 00644)
    except Exception, e:
        print ("Error while copying sample config files. "
               "This is not harmful. The error happened while trying to "
               "determine owner/mode of sample config files: %s" % e)



###############################################################################
# NOW CALL IT !
copy_sample_configs()

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
    url='http://objectlabs.org',
    ##license='LICENSE.txt',
    install_requires=[
        'dargparse==0.1.0',
        'dampier-pymongo==2.1.1',
        'verlib==0.1'],
    dependency_links=[
        "https://github.com/dampier/mongo-python-driver/tarball/master#egg=dampier-pymongo-2.1.1",
        "https://github.com/objectlabs/dargparse/tarball/master#egg=dargparse-0.1.0"
    ]

)
