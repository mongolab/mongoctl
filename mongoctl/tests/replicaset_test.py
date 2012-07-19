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

from mongoctl.tests.test_base import MongoctlTestBase

class ReplicasetTest(MongoctlTestBase):

    def test_replicaset(self):
        # assert that all servers are down
        self.assert_server_stopped("arbiter_test_server")
        self.assert_server_stopped("node1_test_server")
        self.assert_server_stopped("node2_test_server")
        # start all servers and make sure they started...
        self.assert_start_server("arbiter_test_server")
        self.assert_server_running("arbiter_test_server")

        self.assert_start_server("node1_test_server")
        self.assert_server_running("node1_test_server")

        self.assert_start_server("node2_test_server")
        self.assert_server_running("node2_test_server")

        # Configure the cluster
        self.mongoctl_assert_cmd("configure-cluster ReplicasetTestCluster")

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)
        # RE-Configure the cluster
        self.mongoctl_assert_cmd("configure-cluster ReplicasetTestCluster"
                                 " -u abdulito")

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)

        # reconfigure with FORCE
        self.mongoctl_assert_cmd("configure-cluster ReplicasetTestCluster "
                                 "--force node2_test_server")

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)

        ## Stop all servers
        self.assert_stop_server("arbiter_test_server")
        self.assert_server_stopped("arbiter_test_server")

        self.assert_stop_server("node1_test_server", force=True)
        self.assert_server_stopped("node1_test_server")

        self.assert_stop_server("node2_test_server", force=True)
        self.assert_server_stopped("node2_test_server")

    ###########################################################################
    def get_my_test_servers(self):
        return ["arbiter_test_server",
                "node1_test_server",
                "node2_test_server"]

# booty
if __name__ == '__main__':
    unittest.main()

