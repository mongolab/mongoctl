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
import time
import os
import shutil

from mongoctl.tests.test_base import MongoctlTestBase

TEMP_MONGO_VERS = "temp_mongo_versions"
class InstallTest(MongoctlTestBase):

###########################################################################
    def setUp(self):
        print ("setUp(): Temporarily setting $MONGO_VERSIONS=%s" %
               TEMP_MONGO_VERS)
        os.environ['MONGO_VERSIONS'] = TEMP_MONGO_VERS
        super(InstallTest, self).setUp()

    ###########################################################################
    def tearDown(self):
        super(InstallTest, self).tearDown()
        if os.path.exists(TEMP_MONGO_VERS):
            print ("tearDown(): Deleting temp $MONGO_VERSIONS=%s" %
                   TEMP_MONGO_VERS)
            shutil.rmtree(TEMP_MONGO_VERS)

    def test_install(self):

        # list servers
        self.mongoctl_assert_cmd("install 2.0.3")
        # list clusters
        self.mongoctl_assert_cmd("install 2.0.4")
        # show server
        self.mongoctl_assert_cmd("install 2.0.4")

# booty
if __name__ == '__main__':
    unittest.main()

