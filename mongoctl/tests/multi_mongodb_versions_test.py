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
from mongoctl import config

import shutil
import os

########################################################################################################################
# Servers
SERVERS = [
    {
        "_id": "version_arbiter_test_server",
        "address": "localhost:27667",
        "cmdOptions": {
            "port": 27667,
            "dbpath": "$MONGOCTL_TEST_DB_DIR/version_arbiter_test_server",
            "auth": True,
            "journal": False,
            "quota": False,
            "quotaFiles": 4
        }
    },

    {
        "_id": "version_node1_test_server",
        "address": "localhost:27668",
        "cmdOptions": {
            "port": 27668,
            "dbpath": "$MONGOCTL_TEST_DB_DIR/version_node1_test_server",
            "auth": True,
            "journal": False,
            "quota": False,
            "quotaFiles": 4,
            "fork": True
        }
    },

    {
        "_id": "version_node2_test_server",
        "address": "localhost:27669",
        "cmdOptions": {
            "port": 27669,
            "dbpath": "$MONGOCTL_TEST_DB_DIR/version_node2_test_server",
            "auth": True,
            "journal": False,
            "quota": False,
            "quotaFiles": 4,
            "fork": True
        }
    },

]
########################################################################################################################
# Clusters
CLUSTERS = [

    {
        "_id": "VersionReplicasetTestCluster",
        "description": "Testing Cluster for multiple mongodb versions",
        "replKey": "abcd123",

        "members": [
            {
                "server": {
                    "$ref": "servers",
                    "$id": "version_arbiter_test_server"
                },
                "arbiterOnly": True
            },

            {
                "server": {
                    "$ref": "servers",
                    "$id": "version_node1_test_server"
                }
            },

            {
                "server": {
                    "$ref": "servers",
                    "$id": "version_node2_test_server"
                }
            }

        ]
    },
]


TEST_VERSIONS = [
    {
        "version": "2.6.5",
        "edition": "community"
    },
    {
        "version": "2.4.5",
        "edition": "community"
    },
    {
        "version": "3.0.7",
        "edition": "community_ssl"
    },

    {
        "version": "3.2.1",
        "edition": "community"
    }

]

########################################################################################################################
### Servers and Clusters for this test
class MultiMongoDBVersionsTest(MongoctlTestBase):

    ####################################################################################################################
    def setUp(self):
        super(MultiMongoDBVersionsTest, self).setUp()
        self.servers = SERVERS
        self.clusters = CLUSTERS

    ####################################################################################################################
    def test_multi_versions(self):
        for version_conf in TEST_VERSIONS:
            self.do_test_mongodb_version(version_conf["version"], version_conf["edition"])

    ####################################################################################################################
    def do_test_mongodb_version(self, mongo_version, mongo_edition):

        # install version
        self.mongoctl_assert_cmd(["install-mongodb", mongo_version,  "--edition", mongo_edition])
        for server in SERVERS:
            server["mongoVersion"] = mongo_version
            server["mongoEdition"] = mongo_edition
        # assert that all servers are down
        self.assert_server_stopped("version_arbiter_test_server")
        self.assert_server_stopped("version_node1_test_server")
        self.assert_server_stopped("version_node2_test_server")
        # start all servers and make sure they started...
        self.assert_start_server("version_arbiter_test_server")
        self.assert_server_running("version_arbiter_test_server")

        self.assert_start_server("version_node1_test_server")
        self.assert_server_online("version_node1_test_server")

        self.assert_start_server("version_node2_test_server")
        self.assert_server_online("version_node2_test_server")

        # Configure the cluster
        conf_cmd = ["configure-cluster", "VersionReplicasetTestCluster"]
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
        self.assert_stop_server("version_arbiter_test_server")
        self.assert_server_stopped("version_arbiter_test_server")

        self.assert_stop_server("version_node1_test_server", force=True)
        self.assert_server_stopped("version_node1_test_server")

        self.assert_stop_server("version_node2_test_server", force=True)
        self.assert_server_stopped("version_node2_test_server")

        self.tearDown()

    ####################################################################################################################
    def get_my_test_servers(self):
        return map(lambda server_conf: server_conf["_id"], SERVERS)

########################################################################################################################
# booty
if __name__ == '__main__':
    unittest.main()

