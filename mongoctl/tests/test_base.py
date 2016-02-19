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
from mongoctl import repository
from mongoctl.utils import is_pid_alive, kill_process, document_pretty_string, execute_command
from mongoctl.commands.command_utils import get_mongo_executable
from mongoctl.minify_json import minify_json
from subprocess import CalledProcessError
from mongoctl import config

from mongoctl.commands.misc.install import install_mongodb

import traceback

## static methods
###########################################################################
def get_testing_conf_root():
    tests_pkg_path = get_test_pkg_path()
    return os.path.join(tests_pkg_path, "testing_conf")

###########################################################################
def get_test_dir():
    return os.path.join(get_testing_conf_root(), "mongoctl_tests_temp")

###########################################################################
def get_test_db_dir():
    return os.path.join(get_test_dir(), "dbpaths")

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
MONGOCTL_TEST_DIR_ENV = "MONGOCTL_TEST_DIR"

MONGOCTL_TEST_DBS_DIR_ENV = "MONGOCTL_TEST_DB_DIR"

MONGOCTL_TEST_CONF_DIR_ENV = "MONGOCTL_TEST_CONF_DIR"

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

        # set the config root to test conf root
        config.set_config_root(get_testing_conf_root())

        # set the test dir env
        test_db_dir = get_test_db_dir()
        os.environ[MONGOCTL_TEST_DIR_ENV] = get_test_dir()
        os.environ[MONGOCTL_TEST_DBS_DIR_ENV] = test_db_dir
        os.environ[MONGOCTL_TEST_CONF_DIR_ENV] = get_testing_conf_root()
        # assure that the testing dir does not exist
        print "--- Creating test db directory %s " % test_db_dir
        if os.path.exists(test_db_dir):
            print ("Warning: %s already exists. Deleting and creating"
                   " again..." % test_db_dir)
            shutil.rmtree(test_db_dir)

        os.makedirs(test_db_dir)

        # cleanup pids before running
        self.cleanup_test_server_pids()

        # install latest mongodb
        print "Installing latest stable MongoDB for testing..."
        # pass None to install latest version
        install_mongodb(mongodb_version=None)


    ###########################################################################
    def tearDown(self):
        test_db_dir = get_test_db_dir()
        print "Tearing down: Cleaning up all used resources..."
        # delete the database dir when done
        self.cleanup_test_server_pids()

        print "--- Deleting the test db directory %s " % test_db_dir
        shutil.rmtree(test_db_dir)
        # Clear repository cache: This is needed since we are deleting db dirs, we want to make sure that servers
        # objects are new after tear down
        repository.clear_repository_cache()

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
    def assert_server_online(self, server_id):
        server = repository.lookup_server(server_id)
        self.assertTrue(server.is_online())

    ###########################################################################
    def assert_server_offline(self, server_id):
        server = repository.lookup_server(server_id)
        self.assertTrue(not server.is_online())

    ###########################################################################
    def assert_server_stopped(self, server_id):
        self.assert_server_status(server_id, is_running=False)

    ###########################################################################
    def assert_server_status(self, server_id, is_running):
        cmd = ["status", server_id]
        append_user_arg(cmd)
        status = self.mongoctl_assert_cmd(cmd)

        #connection_str = '"connection": %s' % "true" if is_running else "false"
        self.assertTrue(status and "connection" in status and
                        status["connection"] == is_running)

    ###########################################################################
    def assert_start_server(self, server_id, start_options=None):
        cmd = ["start", server_id]
        append_user_arg(cmd)
        if start_options:
            cmd.extend(start_options)

        return self.mongoctl_assert_cmd(cmd)

    ###########################################################################
    def assert_stop_server(self, server_id, force=False):
        cmd = ["stop", server_id]
        if force:
            cmd.append("--force")
        append_user_arg(cmd)

        return self.mongoctl_assert_cmd(cmd)

    ###########################################################################
    def assert_restart_server(self, server_id):
        cmd = ["restart", server_id]
        append_user_arg(cmd)
        return self.mongoctl_assert_cmd(cmd)

    ###########################################################################
    def mongoctl_assert_cmd(self, cmd, exit_code=0):
        return self.exec_assert_cmd(self.to_mongoctl_test_command(cmd))

    ###########################################################################
    def exec_assert_cmd(self, cmd, exit_code=0):
        print "++++++++++ Testing command : %s" % cmd_display_str(cmd)

        try:
            #output =  execute_command(cmd, shell=True, cwd=get_mongoctl_module_dir())
            #print output
            return mongoctl_main.do_main(cmd)
        except Exception, e:
            print("Error while executing test command '%s'. Cause: %s " %
                  (cmd_display_str(cmd), e))
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
                  (cmd_display_str(cmd), e))

    ###########################################################################
    def to_mongoctl_test_command(self, cmd, mongoctl_options=None):
        mongoctl_options = mongoctl_options or {}
        mongoctl_options.update(MONGOCTL_TEST_OPTIONS)
        if self.servers:
            mongoctl_options["--servers"] = _dict_list_to_option_str(self.servers)

        if self.clusters:
            mongoctl_options["--clusters"] = _dict_list_to_option_str(self.clusters)

        option_list = _to_options_list(mongoctl_options)

        return option_list + cmd


########################################################################################################################
def _to_options_list(options):
    options_list = list()
    for key, val in options.items():
        if val is True:
            options_list.append(key)
        else:
            options_list.append(key)
            options_list.append(val)

    return options_list


########################################################################################################################
def _to_options_str(options):
    return " ".join(_to_options_list(options))

########################################################################################################################
def _dict_list_to_option_str(dict_list):
    return minify_json.json_minify(document_pretty_string(dict_list))

########################################################################################################################
def append_user_arg(cmd):
    cmd.extend(["-u", "abdulito"])
    return cmd

########################################################################################################################
def cmd_display_str(cmd):
    return "mongoctl %s" % (" ".join(cmd))

########################################################################################################################
__testing_version__ = None

def get_testing_mongo_version():
    global __testing_version__
    if not __testing_version__:
        mongod_exe = get_mongo_executable(None, 'mongod')
        __testing_version__ = mongod_exe.version
    return __testing_version__