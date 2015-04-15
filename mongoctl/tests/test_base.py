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
from mongoctl.repository import lookup_server
from mongoctl.utils import is_pid_alive, kill_process, document_pretty_string, execute_command
from mongoctl.commands.command_utils import get_mongo_executable
from mongoctl.minify_json import minify_json
from subprocess import CalledProcessError
from mongoctl.objects.server import VERSION_3_0
import traceback

## static methods
###########################################################################
def get_testing_conf_root():
    tests_pkg_path = get_test_pkg_path()
    return os.path.join(tests_pkg_path, "testing_conf")

###########################################################################
def get_test_dbs_dir():
    return os.path.join(get_testing_conf_root(), "mongoctltest_dbs")

###############################################################################
def get_test_pkg_path():
    return os.path.dirname(inspect.getfile(inspect.currentframe()))

###############################################################################
def get_mongoctl_module_dir():
    tests_pkg_path = get_test_pkg_path()
    return os.path.dirname(os.path.dirname(tests_pkg_path))

###############################################################################
def get_mongoctl_exe():
    return os.path.join(get_mongoctl_module_dir(), "bin", "mongoctl")

###############################################################################
# Constants
###############################################################################
MONGOCTL_TEST_DBS_DIR_ENV = "MONGOCTL_TEST_DIR"

MONGOCTL_TEST_OPTIONS = {
    "--verbose": True,
    "--noninteractive": True,
    "--config-root": get_testing_conf_root()

}
###############################################################################
# Base test class
###############################################################################
class MongoctlTestBase(unittest.TestCase):

    ###########################################################################
    @property
    def servers(self):
        return self._servers

    @servers.setter
    def servers(self, servers):
        self._servers = servers

    ###########################################################################
    @property
    def clusters(self):
        return self._clusters

    @clusters.setter
    def clusters(self, clusters):
        self._clusters = clusters

    ###########################################################################
    def setUp(self):
        super(MongoctlTestBase, self).setUp()
        # _servers, _clusters are list of dicts
        # those will be passed as the --servers --clusters property
        self._servers = None
        self._clusters = None

        # set the test dir env
        test_dir = get_test_dbs_dir()
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
        test_dir = get_test_dbs_dir()
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
        status = self.mongoctl_assert_cmd("status %s -u abdulito" % server_id)
        connection_str = '"connection": %s' % "true" if is_running else "false"
        self.assertTrue(connection_str in status)

    ###########################################################################
    def assert_start_server(self, server_id, **kwargs):
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
            output =  execute_command(cmd, shell=True, cwd=get_mongoctl_module_dir())
            print output
            return output
        except Exception, e:
            print("Error while executing test command '%s'. Cause: %s " %
                  (cmd, e))
            if isinstance(e, CalledProcessError):
                print "#### Command output ####"
                print e.output
                print "###################"

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
    def to_mongoctl_test_command(self, cmd, mongoctl_options=None):
        mongoctl_options = mongoctl_options or {}
        mongoctl_options.update(MONGOCTL_TEST_OPTIONS)
        if self.servers:
            mongoctl_options["--servers"] = _dict_list_to_option_str(self.servers)

        if self.clusters:
            mongoctl_options["--clusters"] = _dict_list_to_option_str(self.clusters)

        mongoctl_options_str = _to_options_str(mongoctl_options)
        return "%s %s %s" % (get_mongoctl_exe(), mongoctl_options_str, cmd)

########################################################################################################################
def _to_options_str(options):
    options_list = list()
    for key, val in options.items():
        if val is True:
            options_list.append(key)
        else:
            options_list.append(key)
            options_list.append(val)

    return " ".join(options_list)

########################################################################################################################
def _dict_list_to_option_str(dict_list):
    return "'%s'" % minify_json.json_minify(document_pretty_string(dict_list))

########################################################################################################################
__testing_version__ = None

def get_testing_mongo_version():
    global __testing_version__
    if not __testing_version__:
        mongod_exe = get_mongo_executable(None, 'mongod')
        __testing_version__ = mongod_exe.version
    return __testing_version__