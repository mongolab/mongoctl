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

