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
from mongoctl.utils import which, call_command, ensure_dir
from mongoctl import repository
from mongoctl.tests.test_base import MongoctlTestBase, append_user_arg, get_test_dir

class SSLReplicasetTest(MongoctlTestBase):

    def test_ssl_replicaset(self):

        # install ssl mongodb
        self.mongoctl_assert_cmd(["install-mongodb", "3.0.7",  "--edition", "community_ssl"])

        # generate ssl key files
        self.generate_ssl_key_files()

        # assert that all servers are down
        self.assert_server_stopped("ssl_arbiter_test_server")
        self.assert_server_stopped("ssl_node1_test_server")
        self.assert_server_stopped("ssl_node2_test_server")
        # start all servers and make sure they started...

        self.assert_start_server("ssl_node1_test_server", ["--rs-add"])
        self.assert_server_online("ssl_node1_test_server")

        self.assert_start_server("ssl_node2_test_server", ["--rs-add"])
        self.assert_server_online("ssl_node2_test_server")

        self.assert_start_server("ssl_arbiter_test_server", ["--rs-add"])
        self.assert_server_running("ssl_arbiter_test_server")

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

    def tearDown(self):
        pass

    ###########################################################################
    def generate_ssl_key_files(self):
        ssl_dir = os.path.join(get_test_dir(), "ssl")
        ensure_dir(ssl_dir)

        ssl_cmd = [which("openssl"), "req", "-newkey","rsa:2048", "-new", "-x509", "-days", "1", "-nodes", "-out",
                   "test-mongodb-cert.crt", "-keyout", "test-mongodb-cert.key", "-subj",
                   "/C=US/ST=CA/L=SF/O=mongoctl/CN=test"

                   ]

        call_command(ssl_cmd, cwd=ssl_dir)

        # create the .pem file

        crt_file = os.path.join(ssl_dir, "test-mongodb-cert.crt")
        key_file = os.path.join(ssl_dir, "test-mongodb-cert.key")
        pem_file = os.path.join(ssl_dir, "test-mongodb.pem")
        with open(pem_file, 'w') as outfile:
            with open(crt_file) as infile1:
                outfile.write(infile1.read())

            with open(key_file) as infile2:
                outfile.write(infile2.read())

        for server_id in self.get_my_test_servers():
            server = repository.lookup_server(server_id)
            server.set_cmd_option("sslPEMKeyFile", pem_file)
            server.set_cmd_option("sslCAFile", pem_file)


# booty
if __name__ == '__main__':
    unittest.main()

