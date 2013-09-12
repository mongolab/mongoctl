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

import unittest

import os
import shutil
import mongoctl.config as config

from mongoctl.tests.test_base import MongoctlTestBase

TEMP_MONGO_INSTALL_DIR = "temp_mongo_installs_dir"
MONGO_INSTALL_DIR = config.get_mongodb_installs_dir()
class InstallTest(MongoctlTestBase):

###########################################################################
    def setUp(self):
        print ("setUp(): Temporarily setting mongoDBInstallationsDirectory=%s" %
               TEMP_MONGO_INSTALL_DIR)

        config.set_mongodb_installs_dir(TEMP_MONGO_INSTALL_DIR)
        super(InstallTest, self).setUp()

    ###########################################################################
    def tearDown(self):
        super(InstallTest, self).tearDown()
        if os.path.exists(TEMP_MONGO_INSTALL_DIR):
            print ("tearDown(): Deleting temp mongoDBInstallationsDirectory=%s" %
                   TEMP_MONGO_INSTALL_DIR)
            shutil.rmtree(TEMP_MONGO_INSTALL_DIR)

        print ("tearDown(): Resetting  mongoDBInstallationsDirectory back to"
               " '%s'" % MONGO_INSTALL_DIR)

        config.set_mongodb_installs_dir(MONGO_INSTALL_DIR)

    ###########################################################################
    def test_install(self):

        # install and list
        self.mongoctl_assert_cmd("list-versions")
        self.mongoctl_assert_cmd("install 2.0.3")
        self.mongoctl_assert_cmd("list-versions")
        self.mongoctl_assert_cmd("install 2.0.4")
        self.mongoctl_assert_cmd("list-versions")
        self.mongoctl_assert_cmd("install 2.0.5")
        self.mongoctl_assert_cmd("list-versions")

        # uninstall and list
        self.mongoctl_assert_cmd("uninstall 2.0.3")
        self.mongoctl_assert_cmd("list-versions")
        self.mongoctl_assert_cmd("uninstall 2.0.4")
        self.mongoctl_assert_cmd("list-versions")
        self.mongoctl_assert_cmd("uninstall 2.0.5")
        self.mongoctl_assert_cmd("list-versions")

    ###########################################################################
# booty
if __name__ == '__main__':
    unittest.main()

