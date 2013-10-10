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
#

__author__ = 'abdul'

import unittest
import commands
import inspect
import os
import shutil
import mongoctl.mongoctl as mongoctl_main
from mongoctl.utils import is_pid_alive, kill_process
import traceback

###############################################################################
# Constants
###############################################################################
MONGOCTL_TEST_DBS_DIR_ENV = "MONGOCTL_TEST_DIR"

###############################################################################
# Base test class
###############################################################################
class MongoctlTestBase(unittest.TestCase):

    ###########################################################################
    def setUp(self):
        # set the test dir env
        test_dir = self.get_test_dbs_dir()
        os.environ[MONGOCTL_TEST_DBS_DIR_ENV] = test_dir
        # assure that the testing dir does not exist
        print "--- Creating test db directory %s " % test_dir
        if os.path.exists(test_dir):
            print ("Warning: %s already exists. Deleting and creating"
                   " again..." % test_dir)
            shutil.rmtree(test_dir)

        os.makedirs(test_dir)

        # cleanup pids before running
        self.cleanup_test_server_pids()

    ###########################################################################
    def tearDown(self):
        test_dir = self.get_test_dbs_dir()
        print "Tearing down: Cleaning up all used resources..."
        # delete the database dir when done
        self.cleanup_test_server_pids()

        print "--- Deleting the test db directory %s " % test_dir
        shutil.rmtree(test_dir)

    ###########################################################################
    def cleanup_test_server_pids(self):
        # delete the database dir when done
        print ("Ensuring all test servers processes are killed. "
              "Attempt to force kill all servers...")

        for server_id in self.get_my_test_servers():
            self.quiet_kill_test_server(server_id)

    ###########################################################################
    def quiet_kill_test_server(self, server_id):
        server_ps_output =  commands.getoutput("ps -o pid -o command -ae | "
                                               "grep %s" % server_id)

        pid_lines = server_ps_output.split("\n")

        for line in pid_lines:
            if line == '':
                continue
            pid = line.strip().split(" ")[0]
            if pid == '':
                continue
            pid = int(pid)
            if is_pid_alive(pid):
                print ("PID for server %s is still alive. Killing..." %
                       server_id)
                kill_process(pid, force=True)

    ###########################################################################
    # This should be overridden by inheritors
    def get_my_test_servers(self):
        return []

    ###########################################################################
    def assert_server_running(self, server_id):
        self.assert_server_status(server_id, is_running=True)

    ###########################################################################
    def assert_server_stopped(self, server_id):
        self.assert_server_status(server_id, is_running=False)

    ###########################################################################
    def assert_server_status(self, server_id, is_running):
        status=  self.mongoctl_assert_cmd("status %s -u abdulito" % server_id)
        self.assertEquals(status['connection'], is_running)

    ###########################################################################
    def assert_start_server(self, server_id):
        return self.mongoctl_assert_cmd("start %s -u abdulito" % server_id)

    ###########################################################################
    def assert_stop_server(self, server_id, force=False):

        args = ("--force %s" % server_id) if force else server_id
        return self.mongoctl_assert_cmd("stop %s -u abdulito" % args)

    ###########################################################################
    def assert_restart_server(self, server_id):
        return self.mongoctl_assert_cmd("restart %s -u abdulito" % server_id)

    ###########################################################################
    def mongoctl_assert_cmd(self, cmd, exit_code=0):
        return self.exec_assert_cmd(self.to_mongoctl_test_command(cmd))

    ###########################################################################
    def exec_assert_cmd(self, cmd, exit_code=0):
        print "++++++++++ Testing command : %s" % cmd

        try:
            return mongoctl_main.do_main(cmd.split(" ")[1:])
        except Exception, e:
            print("Error while executing test command '%s'. Cause: %s " %
                  (cmd, e))
            print "================= STACK TRACE ================"
            traceback.print_exc()
            print "Failing..."
            self.fail()

    ###########################################################################
    def quiet_exec_cmd(self, cmd, exit_code=0):
        print "Quiet Executing command : %s" % cmd

        try:
            return mongoctl_main.do_main(cmd.split(" ")[1:])
        except Exception, e:
            print("WARNING: failed to quiet execute command '%s'. Cause: %s " %
                  (cmd, e))

    ###########################################################################
    def to_mongoctl_test_command(self,cmd):
        return ("mongoctl -v --yes --config-root %s %s" %
                (self.get_testing_conf_root(), cmd))

    ###########################################################################
    def get_testing_conf_root(self):
        tests_pkg_path = os.path.dirname(
            inspect.getfile(inspect.currentframe()))
        return os.path.join(tests_pkg_path, "testing_conf")

    ###########################################################################
    def get_test_dbs_dir(self):
        return os.path.join(self.get_testing_conf_root(), "mongoctltest_dbs")