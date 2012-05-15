import unittest
import time

from mongoctl.tests.test_base import MongoctlTestBase

class MiscTest(MongoctlTestBase):

    def test_miscs(self):
        # list servers
        self.mongoctl_assert_cmd("list-servers")
        # list clusters
        self.mongoctl_assert_cmd("list-clusters")
        # show server
        self.mongoctl_assert_cmd("show-server master_test_server")
        # show cluster
        self.mongoctl_assert_cmd("show-cluster ReplicasetTestCluster")

# booty
if __name__ == '__main__':
    unittest.main()

