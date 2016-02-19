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

from mongoctl.tests.test_base import MongoctlTestBase, append_user_arg

class SSLReplicasetTest(MongoctlTestBase):

    def test_ssl_replicaset(self):

        # install ssl mongodb
        self.mongoctl_assert_cmd(["install-mongodb", "3.0.7",  "--edition", "community_ssl"])
        # assert that all servers are down
        self.assert_server_stopped("ssl_arbiter_test_server")
        self.assert_server_stopped("ssl_node1_test_server")
        self.assert_server_stopped("ssl_node2_test_server")
        # start all servers and make sure they started...
        self.assert_start_server("ssl_arbiter_test_server")
        self.assert_server_running("ssl_arbiter_test_server")

        self.assert_start_server("ssl_node1_test_server")
        self.assert_server_online("ssl_node1_test_server")

        self.assert_start_server("ssl_node2_test_server")
        self.assert_server_online("ssl_node2_test_server")

        # Configure the cluster
        conf_cmd = ["configure-cluster", "SSLReplicasetTestCluster"]
        append_user_arg(conf_cmd)
        self.mongoctl_assert_cmd(conf_cmd)

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)
        # RE-Configure the cluster
        self.mongoctl_assert_cmd(conf_cmd)

        print ("Sleeping for 15 seconds. Hopefully credentials would "
              "be replicated by then. If not then authentication will fail and"
              " passwords will be prompted and then the test will fail...")
        # sleep for a couple of seconds
        time.sleep(15)

        ## Stop all servers
        self.assert_stop_server("ssl_arbiter_test_server")
        self.assert_server_stopped("ssl_arbiter_test_server")

        self.assert_stop_server("ssl_node1_test_server", force=True)
        self.assert_server_stopped("ssl_node1_test_server")

        self.assert_stop_server("ssl_node2_test_server", force=True)
        self.assert_server_stopped("ssl_node2_test_server")

    ###########################################################################
    def get_my_test_servers(self):
        return ["ssl_arbiter_test_server",
                "ssl_node1_test_server",
                "ssl_node2_test_server"]

# booty
if __name__ == '__main__':
    unittest.main()

