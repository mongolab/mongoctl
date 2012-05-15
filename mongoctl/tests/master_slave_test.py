import unittest

from mongoctl.tests.test_base import MongoctlTestBase

class MasterSlaveTest(MongoctlTestBase):

    def test_master_slave(self):
        # assert servers stopped
        self.assert_server_stopped("master_test_server")
        self.assert_server_stopped("slave_test_server")

        # start master
        self.assert_start_server("master_test_server")
        self.assert_server_running("master_test_server")

        # start slave
        self.assert_start_server("slave_test_server")
        self.assert_server_running("slave_test_server")

        # stop master
        self.assert_stop_server("master_test_server")
        self.assert_server_stopped("master_test_server")

        # stop slave
        self.assert_stop_server("slave_test_server")
        self.assert_server_stopped("slave_test_server")

    ###########################################################################
    def get_my_test_servers(self):
        return ["master_test_server",
                "slave_test_server"]
# booty
if __name__ == '__main__':
    unittest.main()

