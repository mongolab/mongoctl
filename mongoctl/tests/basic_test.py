import unittest
import commands
import json
import inspect
import os
import shutil

from mongoctl.tests.test_base import MongoctlTestBase

class BasicMongoctlTest(MongoctlTestBase):

    def test_start_stop_server(self):
        self.assert_server_stopped("simple_test_server")
        self.assert_start_server("simple_test_server")
        self.assert_server_running("simple_test_server")
        self.assert_stop_server("simple_test_server")
        self.assert_server_stopped("simple_test_server")

    ###########################################################################
    def test_restart_server(self):
        self.assert_server_stopped("simple_test_server")
        self.assert_restart_server("simple_test_server")
        self.assert_restart_server("simple_test_server")
        self.assert_restart_server("simple_test_server")
        self.assert_server_running("simple_test_server")
        self.assert_stop_server("simple_test_server")
        self.assert_server_stopped("simple_test_server")

    ###########################################################################
    def get_my_test_servers(self):
        return ["simple_test_server"]
# booty
if __name__ == '__main__':
    unittest.main()

