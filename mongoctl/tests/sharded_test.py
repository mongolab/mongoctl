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

########################################################################################################################
# Servers
SHARD_TEST_SERVERS = [
    "ConfigServer1",
    "ConfigServer2",
    "ConfigServer3",
    "Mongos1",
    "Mongos2",
    "ShardServer1",
    "ShardServer2",
    "ShardServer3",
    "ShardServer4",
    "ShardServer5",
    "ShardServer6",
    "ShardArbiter"


]
########################################################################################################################
### Sharded Servers
class ShardedTest(MongoctlTestBase):


    ########################################################################################################################
    def test_sharded(self):
        # Start all sharded servers

        for s_id in SHARD_TEST_SERVERS:
            self.assert_start_server(s_id, start_options=["--rs-add"])

        print "Sleeping for 10 seconds..."
        # sleep for 10 of seconds
        time.sleep(10)
        conf_cmd = ["configure-shard-cluster", "ShardedCluster"]
        append_user_arg(conf_cmd)
        # Configure the sharded cluster
        self.mongoctl_assert_cmd(conf_cmd)

    ###########################################################################
    def get_my_test_servers(self):
        return SHARD_TEST_SERVERS

# booty
if __name__ == '__main__':
    unittest.main()

