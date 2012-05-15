import unittest
import time

from mongoctl.tests.test_base import MongoctlTestBase

class AuthReplicasetTest(MongoctlTestBase):

    def test_auth_replicaset(self):
        # assert that all servers are down
        self.assert_server_stopped("auth_arbiter_test_server")
        self.assert_server_stopped("auth_node1_test_server")
        self.assert_server_stopped("auth_node2_test_server")
        # start all servers and make sure they started...
        self.assert_start_server("auth_arbiter_test_server")
        self.assert_server_running("auth_arbiter_test_server")

        self.assert_start_server("auth_node1_test_server")
        self.assert_server_running("auth_node1_test_server")

        self.assert_start_server("auth_node2_test_server")
        self.assert_server_running("auth_node2_test_server")

        # Configure the cluster
        self.mongoctl_assert_cmd("configure-cluster AuthReplicasetTestCluster")

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)
        # RE-Configure the cluster
        self.mongoctl_assert_cmd("configure-cluster AuthReplicasetTestCluster")

        print "Sleeping for 2 seconds..."
        # sleep for a couple of seconds
        time.sleep(2)

        ## Stop all servers
        self.assert_stop_server("auth_arbiter_test_server")
        self.assert_server_stopped("auth_arbiter_test_server")

        self.assert_stop_server("auth_node1_test_server", force=True)
        self.assert_server_stopped("auth_node1_test_server")

        self.assert_stop_server("auth_node2_test_server", force=True)
        self.assert_server_stopped("auth_node2_test_server")

    ###########################################################################
    def get_my_test_servers(self):
        return ["auth_arbiter_test_server",
                "auth_node1_test_server",
                "auth_node2_test_server"]

# booty
if __name__ == '__main__':
    unittest.main()

