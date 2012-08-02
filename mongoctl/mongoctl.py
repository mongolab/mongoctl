#!/usr/bin/env python

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

__author__ = 'abdul'

###############################################################################
# Imports
###############################################################################

import sys
import traceback
import pymongo
import os
import re
import time
import json
import bson
import stat
import subprocess
import resource
import datetime
import dargparse
import socket
import shutil
import platform
import urllib
import urlparse
import signal
import getpass

from dargparse import dargparse
from pymongo import Connection
from pymongo import uri_parser

from pymongo import errors
from bson import json_util
from bson.son import SON
from minify_json import minify_json
from mongoctl_command_config import MONGOCTL_PARSER_DEF
from verlib import NormalizedVersion, suggest_normalized_version
###############################################################################
# Constants
###############################################################################

MONGO_HOME_ENV_VAR = "MONGO_HOME"

MONGO_VERSIONS_ENV_VAR = "MONGO_VERSIONS"

CONF_ROOT_ENV_VAR = "MONGOCTL_CONF"

SERVER_ID_PARAM = "server"

CLUSTER_ID_PARAM = "cluster"

# default pid file name
PID_FILE_NAME = "pid.txt"

LOG_FILE_NAME = "mongodb.log"

DEFAULT_CONF_ROOT = "~/.mongoctl"

MONGOCTL_CONF_FILE_NAME = "mongoctl.config"

KEY_FILE_NAME = "keyFile"

DEFAULT_SERVERS_FILE = "servers.config"

DEFAULT_CLUSTERS_FILE = "clusters.config"

DEFAULT_SERVERS_COLLECTION = "servers"

DEFAULT_CLUSTERS_COLLECTION = "clusters"

DEFAULT_ACTIVITY_COLLECTION = "logs.server-activity"

# This is mongodb's default port
DEFAULT_PORT = 27017

# This is mongodb's default dbpath
DEFAULT_DBPATH='/data/db'

# db connection timeout, 10 seconds
CONN_TIMEOUT = 10000

# OS resource limits to impose on the 'mongod' process (see setrlimit(2))
PROCESS_LIMITS = [
    # Many TCP/IP connections to mongod ==> many threads to handle them ==>
    # RAM footprint of many stacks.  Ergo, limit the stack size per thread:
    ('RLIMIT_STACK', "stack size (in bytes)", 1024 * 1024),
    # Speaking of connections, we'd like to be able to have a lot of them:
    ('RLIMIT_NOFILE', "number of file descriptors", 65536)
    ]

# VERSION CHECK PREFERENCE CONSTS
VERSION_PREF_EXACT = 0
VERSION_PREF_GREATER = 1
VERSION_PREF_MAJOR_GE = 2
VERSION_PREF_LATEST_STABLE = 3

# Version support stuff
MIN_SUPPORTED_VERSION = "1.8"
REPL_KEY_SUPPORTED_VERSION = '2.0.0'

LATEST_VERSION_FILE_URL = "https://raw.github.com/mongolab/mongoctl/master/" \
                          "mongo_latest_stable_version.txt"
###############################################################################
# MAIN
###############################################################################
def main(args):
    try:
        do_main(args)
    except MongoctlException,e:
        log_error(e)
        exit(1)

###############################################################################
def do_main(args):
    header = """
-------------------------------------------------------------------------------------------
  __ _  ___  ___  ___ ____  ____/ /_/ /
 /  ' \/ _ \/ _ \/ _ `/ _ \/ __/ __/ / 
/_/_/_/\___/_//_/\_, /\___/\__/\__/_/  
                /___/ 
-------------------------------------------------------------------------------------------
   """

    # Parse options
    parser = get_mongoctl_cmd_parser()

    if len(args) < 1:
        print(header)
        parser.print_help()
        return

    # Parse the arguments and call the function of the selected cmd
    parsed_args = parser.parse_args(args)

    # turn on verbose if specified
    if namespace_get_property(parsed_args,"mongoctlVerbose"):
        turn_logging_verbose_on()

    # set interactive mode
    non_interactive = namespace_get_property(parsed_args,'noninteractive')
    non_interactive = False if non_interactive is None else non_interactive

    set_interactive_mode(not non_interactive)

    if not is_interactive_mode():
        log_verbose("Running with non-interactive mode")

    # set global prompt value
    yes_all = parsed_args.yesToEverything
    no_all = parsed_args.noToEverything

    if yes_all and no_all:
        raise MongoctlException("Cannot have --yes and --no at the same time. "
                                "Please choose either --yes or --no")
    elif yes_all:
        say_yes_to_everything()
    elif no_all:
        say_no_to_everything()

    # set conf root if specified
    if parsed_args.configRoot is not None:
        _set_config_root(parsed_args.configRoot)
    elif os.getenv(CONF_ROOT_ENV_VAR) is not None:
        _set_config_root(os.getenv(CONF_ROOT_ENV_VAR))

    # get the function to call from the parser framework
    command_function = parsed_args.func

    # parse global login if present
    parse_global_login_user_arg(parsed_args)
    server_id = namespace_get_property(parsed_args,SERVER_ID_PARAM)

    if server_id is not None:
        # check if assumeLocal was specified
        assume_local = namespace_get_property(parsed_args,"assumeLocal")
        if assume_local:
            assume_local_server(server_id)
    # execute command
    log_info("")
    return command_function(parsed_args)

###############################################################################
########################                       ################################
######################## Commandline functions ################################
########################                       ################################
###############################################################################

###############################################################################
# start command
###############################################################################
def start_command(parsed_options):
    options_override = extract_mongod_options(parsed_options)

    if parsed_options.dryRun:
        dry_run_start_server_cmd(parsed_options.server, options_override)
    else:
        start_server(parsed_options.server,
                     options_override=options_override,
                     rs_add=parsed_options.rsAdd)

###############################################################################
# stop command
###############################################################################
def stop_command(parsed_options):
    stop_server(parsed_options.server, force=parsed_options.forceStop)

###############################################################################
# restart command
###############################################################################
def restart_command(parsed_options):
    options_override = extract_mongod_options(parsed_options)

    restart_server(parsed_options.server, options_override)

###############################################################################
# status command TODO: parsed?
###############################################################################
def status_command(parsed_options):
    log_info("Status for server '%s':" % parsed_options.server)
    # we need to print status json to stdout so that its seperate from all
    # other messages that are printed on stderr. This is so scripts can read
    # status json and parse it if it needs
    status = get_server_status(parsed_options.server,
                                   parsed_options.statusVerbose)
    status_str = document_pretty_string(status)
    stdout_log(status_str)
    return status

###############################################################################
# list servers command
###############################################################################
def list_servers_command(parsed_options):
    servers = lookup_all_servers()
    if not servers or len(servers) < 1:
        log_info("No servers have been configured.");
        return

    servers = sorted(servers, key=lambda s: s.get_id())
    bar = "-"*80
    print bar
    formatter = "%-25s %-40s %s"
    print formatter % ("_ID", "DESCRIPTION", "CONNECT TO")
    print bar


    for server in servers:
        print formatter % (server.get_id(), 
                           _lista(server.get_description()),
                           _lista(server.get_address_display()))
    print "\n"

def _lista(thing):
    return "" if thing is None else str(thing)

###############################################################################
# show server command
###############################################################################
def show_server_command(parsed_options):
    server = lookup_server(parsed_options.server)
    if server is None:
        raise MongoctlException("Could not find server '%s'." %
                                parsed_options.server)
    log_info("Configuration for server '%s':" % parsed_options.server)
    print server

###############################################################################
# connect command
###############################################################################
def connect_command(parsed_options):
    shell_options = extract_mongo_shell_options(parsed_options)
    open_mongo_shell_to(parsed_options.dbAddress,
                        username=parsed_options.username,
                        password=parsed_options.password,
                        shell_options=shell_options,
                        js_files=parsed_options.jsFiles)

###############################################################################
# tail log command
###############################################################################
def tail_log_command(parsed_options):
    server = lookup_server(parsed_options.server)
    validate_local_op(server, "tail")
    log_path = server.get_log_file_path()
    # check if log file exists
    if os.path.exists(log_path):
        log_tailer = tail_server_log(server)
        log_tailer.communicate()
    else:
        log_info("Log file '%s' does not exist." % log_path)

###############################################################################
# configure cluster command
###############################################################################
def configure_cluster_command(parsed_options):
    cluster_id = parsed_options.cluster
    force_primary_server_id = parsed_options.forcePrimaryServer

    if parsed_options.dryRun:
        dry_run_configure_cluster(cluster_id=cluster_id,
                                  force_primary_server_id=
                                    force_primary_server_id)
    else:
        configure_cluster(cluster_id=cluster_id,
                          force_primary_server_id=
                            force_primary_server_id)

###############################################################################
# list clusters command
###############################################################################
def list_clusters_command(parsed_options):
    clusters = lookup_all_clusters()
    if not clusters or len(clusters) < 1:
        log_info("No clusters configured");
        return

    bar = "-"*80
    print bar
    formatter = "%-25s %-40s %s"
    print formatter % ("_ID", "DESCRIPTION", "MEMBERS")
    print bar

    for cluster in clusters:
        desc = _lista(cluster.get_description())

        members_info = "[ %s ]" % ", ".join(cluster.get_members_info())

        print formatter % (cluster.get_id(), desc, members_info)
    print "\n"

###############################################################################
# show cluster command
###############################################################################
def show_cluster_command(parsed_options):
    cluster = lookup_cluster(parsed_options.cluster)
    if cluster is None:
        raise MongoctlException("Could not find cluster '%s'." %
                            parsed_options.cluster)
    log_info("Configuration for cluster '%s':" % parsed_options.cluster)
    print cluster

###############################################################################
# install command
###############################################################################
def install_command(parsed_options):
    install_mongodb(version=parsed_options.version)

###############################################################################
# uninstall command
###############################################################################
def uninstall_command(parsed_options):
    uninstall_mongodb(version=parsed_options.version)

###############################################################################
# list-versions command
###############################################################################
def list_versions_command(parsed_options):
    mongo_installations = find__all_mongo_installations()

    bar = "-" * 80
    print bar
    formatter = "%-20s %s"
    print formatter % ("VERSION", "LOCATION")
    print bar

    for install_dir,version in mongo_installations:
        print formatter % (version, install_dir)
    print "\n"


###############################################################################
########################                   ####################################
########################    Mongoctl API   ####################################
########################                   ####################################
###############################################################################

###############################################################################
# start server
###############################################################################
def start_server(server_id, options_override=None, rs_add=False):
    do_start_server(lookup_and_validate_server(server_id),
                    options_override=options_override,
                    rs_add=rs_add)

###############################################################################
__mongod_pid__ = None
__current_server__ = None

###############################################################################
def do_start_server(server, options_override=None, rs_add=False):
    # ensure that the start was issued locally. Fail otherwise
    validate_local_op(server, "start")

    log_info("Checking to see if server '%s' is already running"
             " before starting it..." % server.get_id())
    status = server.get_status()
    if status['connection']:
        log_info("Server '%s' is already running." %
                 server.get_id())
        return
    elif "timedOut" in status:
        raise MongoctlException("Unable to start server: Server '%s' seems to"
                                " be already started but is"
                                " not responding (connection timeout)."
                                " Or there might some server running on the"
                                " same port %s" %
                                (server.get_id(), server.get_port()))

    is_my_repo = is_mongoctl_repo_db(server)
    if is_my_repo:
        # Prevent consulting db repo until it's started AND prepared.
        am_bootstrapping(sez_i=True)
    else:
        log_server_activity(server, "start")

    mongod_pid = start_server_process(server,options_override)

    maybe_config_server_repl_set(server, rs_add=rs_add)

    try:
        prepare_server(server)
    except Exception,e:
        log_error("Unable to fully prepare server '%s'. Cause: %s \n"
                  "Stop server now if more preparation is desired..." %
                  (server.get_id(), e))
        if shall_we_terminate(mongod_pid):
            return

    if is_my_repo:
        # The db repo is now open for business.
        # If seeded, we can now start ignoring the seeds in the file repo.
        am_bootstrapping(sez_i=False)
        log_server_activity(server, "start")

    # Note: The following block has to be the last block
    # because mongod_process.communicate() will not return unless you
    # interrupt the mongod process which will kill mongoctl, so nothing after
    # this block will be executed. Almost never...

    if not is_forking(server, options_override):
        communicate_to_child_process(mongod_pid)

###############################################################################
def maybe_config_server_repl_set(server, rs_add=False):
    # if the server belongs to a replica set cluster,
    # then prompt the user to init the replica set IF not already initialized
    # AND server is NOT an Arbiter
    # OTHERWISE prompt to add server to replica if server is not added yet

    cluster = lookup_cluster_by_server(server)

    if cluster is not None:
        log_verbose("Server '%s' is a member in the configuration for"
                    " cluster '%s'." % (server.get_id(),cluster.get_id()))

        if not cluster.is_replicaset_initialized():
            log_info("Replica set cluster '%s' has not been initialized yet." %
                     cluster.get_id())
            if cluster.get_member_for(server).can_become_primary():
                if rs_add:
                    cluster.initialize_replicaset(server)
                else:
                    prompt_init_replica_cluster(cluster, server)
            else:
                log_info("Skipping replica set initialization because "
                         "server '%s' cannot be elected primary." %
                         server.get_id())
        else:
            log_verbose("No need to initialize cluster '%s', as it has"
                        " already been initialized." % cluster.get_id())
            if not cluster.is_member_configured_for(server):
                if rs_add:
                    cluster.add_member_to_replica(server)
                else:
                    prompt_add_member_to_replica(cluster, server)
            else:
                log_verbose("Server '%s' is already added to the replicaset"
                            " conf of cluster '%s'." %
                            (server.get_id(),cluster.get_id()))

###############################################################################
def _start_server_process_4real(server, options_override=None):
    server_dir_exists = mk_server_dir(server)
    first_time = not server_dir_exists

    # generate key file if needed
    if needs_repl_key(server):
        get_generate_key_file(server)

    # create the start command line
    start_cmd = generate_start_command(server, options_override)

    start_cmd_str = " ".join(start_cmd)
    first_time_msg = " for the first time" if first_time else ""

    log_info("Starting server '%s'%s..." % (server.get_id(), first_time_msg))
    log_info("\nExecuting command:\n%s\n" % start_cmd_str)

    child_process_out = None
    if is_forking(server, options_override):
        child_process_out = subprocess.PIPE

    global __mongod_pid__
    global __current_server__

    parent_mongod = create_subprocess(start_cmd,
                                      stdout=child_process_out,
                                      preexec_fn=server_process_preexec)


    
    if is_forking(server, options_override):
        __mongod_pid__ = get_forked_mongod_pid(parent_mongod)
    else:
        __mongod_pid__ = parent_mongod.pid

    __current_server__ = server
    return __mongod_pid__

###############################################################################
def get_forked_mongod_pid(parent_mongod):
    output = parent_mongod.communicate()[0]
    pid_re_expr = "forked process: ([0-9]+)"
    pid_str = re.search(pid_re_expr, output).groups()[0]

    return int(pid_str)

###############################################################################
def start_server_process(server,options_override=None):

    mongod_pid = _start_server_process_4real(server, options_override)

    log_info("Will now wait for server '%s' to start up."
             " Enjoy mongod's log for now!" %
             server.get_id())
    log_info("\n****************************************************************"
             "***************")
    log_info("* START: tail of log file at '%s'" % server.get_log_file_path())
    log_info("******************************************************************"
             "*************\n")

    log_tailer = tail_server_log(server)
    # wait until the server starts
    try:
        is_online = wait_for(server_started_predicate(server, mongod_pid),
                             timeout=300)
    finally:
        # stop tailing
        stop_tailing(log_tailer)

    log_info("\n****************************************************************"
             "***************")
    log_info("* END: tail of log file at '%s'" % server.get_log_file_path())
    log_info("******************************************************************"
             "*************\n")

    if not is_online:
        raise MongoctlException("Unable to start server '%s'" %
                                server.get_id())

    log_info("Server '%s' started successfully! (pid=%s)\n" %
             (server.get_id(), mongod_pid))

    return mongod_pid

###############################################################################
def server_process_preexec():
    """ make the server ignore ctrl+c signals and have the global mongoctl
        signal handler take care of it
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _set_process_limits()

###############################################################################
def _set_process_limits():
    for (res_name, description, desired_limit) in PROCESS_LIMITS :
        _set_a_process_limit(res_name, desired_limit, description)

###############################################################################
def _set_a_process_limit(resource_name, desired_limit, description):
    which_resource = getattr(resource, resource_name)
    (soft, hard) = resource.getrlimit(which_resource)
    def set_resource(attempted_value):
        log_verbose("Trying setrlimit(resource.%s, (%d, %d))" %
                    (resource_name, attempted_value, hard))
        resource.setrlimit(which_resource, (attempted_value, hard))

    log_info("Setting OS limit on %s for mongod process (desire up to %d)..."
             "\n\t Current limit values:   soft = %d   hard = %d" %
             (description, desired_limit, soft, hard))

    _negotiate_process_limit(set_resource, desired_limit, soft, hard)
    log_info("Resulting OS limit on %s for mongod process:  " % description +
             "soft = %d   hard = %d" % resource.getrlimit(which_resource))

###############################################################################
def _rlimit_min(one_val, nother_val):
    """Returns the more stringent rlimit value.  -1 means no limit."""
    if one_val < 0 or nother_val < 0 :
        return max(one_val, nother_val)
    else:
        return min(one_val, nother_val)

###############################################################################
def _negotiate_process_limit(set_resource, desired_limit, soft, hard):

    best_possible = _rlimit_min(hard, desired_limit)
    worst_possible = soft
    attempt = best_possible           # be optimistic for initial attempt

    while abs(best_possible - worst_possible) > 1 :
        try:
            set_resource(attempt)
            log_verbose("  That worked!  Should I negotiate further?")
            worst_possible = attempt
        except:
            log_verbose("  Phooey.  That didn't work.")
            if attempt < 0 :
                log_info("\tCannot remove soft limit on resource.")
                return
            best_possible = attempt + (1 if best_possible < attempt else -1)

        attempt = (best_possible + worst_possible) / 2

###############################################################################
def tail_server_log(server):
    try:
        logpath = server.get_log_file_path()
        # touch log file to make sure it exists
        log_verbose("Touching log file '%s'" % logpath)
        execute_command(["touch", logpath])

        tail_cmd = ["tail", "-f", logpath]
        log_verbose("Executing command: %s" % (" ".join(tail_cmd)))
        return create_subprocess(tail_cmd)
    except Exception,e:
        log_error("Unable to tail server log file. Cause: %s" % e)
        return None

###############################################################################
def stop_tailing(log_tailer):
    try:
        if log_tailer:
            log_verbose("-- Killing tail log path subprocess")
            log_tailer.terminate()
    except Exception,e:
        log_verbose("Failed to kill tail subprocess. Cause: %s" % e)

###############################################################################
def shall_we_terminate(mongod_pid):
    def killit():
        kill_process(mongod_pid, force=true)
        log_info("Server process terminated at operator behest.")

    (condemned, _) = prompt_execute_task("Kill server now?", killit)
    return condemned

###############################################################################
def dry_run_start_server_cmd(server_id, options_override=None):
    server = lookup_and_validate_server(server_id)
    # ensure that the start was issued locally. Fail otherwise
    validate_local_op(server, "start")

    log_info("************ Dry Run ************\n")

    start_cmd = generate_start_command(server, options_override)
    start_cmd_str = " ".join(start_cmd)

    log_info("\nCommand:")
    log_info("%s\n" % start_cmd_str)

###############################################################################
# stop server
###############################################################################
def stop_server(server_id, force=False):
    do_stop_server(lookup_and_validate_server(server_id), force)

###############################################################################
def do_stop_server(server, force=False):
    # ensure that the stop was issued locally. Fail otherwise
    validate_local_op(server, "stop")

    log_info("Checking to see if server '%s' is actually running before"
             " stopping it..." % server.get_id())

    # init local flags
    can_stop_mongoly = True
    shutdown_success = False

    status = server.get_status()
    if not status['connection']:
        if "timedOut" in status:
            log_info("Unable to issue 'shutdown' command to server: "
                     "Server '%s' is not responding (connection timed out); "
                     "though port %s is open, possibly for mongod." %
                     (server.get_id(), server.get_port()))
            can_stop_mongoly = False
        else:
            log_info("Server '%s' is not running." %
                     server.get_id())
            return

    pid = get_server_pid(server)
    pid_disp = pid if pid else "[Cannot be determined]"
    log_info("Stopping server '%s' (PID=%s) ..." %
             (server.get_id(), pid_disp))
    # log server activity stop
    log_server_activity(server, "stop")
    # TODO: Enable this again when stabilized
    # step_down_if_needed(server, force)

    if can_stop_mongoly:
        log_verbose("  ... issuing db 'shutdown' command ... ")
        shutdown_success = mongo_stop_server(server, pid, force=False)

    if not can_stop_mongoly or not shutdown_success:
        log_verbose("  ... taking more forceful measures ... ")
        shutdown_success = \
            prompt_or_force_stop_server(server, pid, force,
                                        try_mongo_force=can_stop_mongoly)

    if shutdown_success:
        log_info("Server '%s' has stopped." % server.get_id())
    else:
        raise MongoctlException("Unable to stop server '%s'." %
                                server.get_id())

###############################################################################
def step_down_if_needed(server, force):
    ## if server is a primary replica member then step down
    if is_replica_primary(server):
        if force:
            step_server_down(server, force)
        else:
            prompt_step_server_down(server, force)

###############################################################################
def mongo_stop_server(server, pid, force=False):

    try:
        shutdown_cmd = SON( [('shutdown', 1),('force', force)])
        log_info("\nSending the following command to %s:\n%s\n" %
                 (server.get_connection_address(),
                  document_pretty_string(shutdown_cmd)))
        server.disconnecting_db_command(shutdown_cmd, "admin")

        log_info("Will now wait for server '%s' to stop." % server.get_id())
        # Check that the server has stopped
        stop_pred = server_stopped_predicate(server, pid)
        wait_for(stop_pred,timeout=3)

        if not stop_pred():
            log_error("Shutdown command failed...")
            return False
        else:
            return True
    except Exception, e:
        log_error("Failed to gracefully stop server '%s'. Cause: %s" %
                  (server.get_id(), e))
        return False

###############################################################################
def force_stop_server(server, pid, try_mongo_force=True):
    success = False
    # try mongo force stop if server is still online
    if server.is_online() and try_mongo_force:
        success = mongo_stop_server(server, pid, force=True)

    if not success or not try_mongo_force:
        success = kill_stop_server(server, pid)

    return success

###############################################################################
def kill_stop_server(server, pid):
    if pid is None:
        log_error("Cannot forcibly stop the server because the server's process"
                  " ID cannot be determined; pid file '%s' does not exist." %
                  server.get_pid_file_path())
        return False

    log_info("Forcibly stopping server '%s'...\n" % server.get_id())
    log_info("Sending kill -1 (HUP) signal to server '%s'... (pid=%s)" %
             (server.get_id(), pid))

    kill_process(pid, force=False)

    log_info("Will now wait for server '%s' pid '%s' to die." %
             (server.get_id(), pid))
    wait_for(pid_dead_predicate(pid), timeout=3)

    if is_pid_alive(pid):
        log_error("Failed to kill server process with -1 (HUP).")
        log_info("Sending kill -9 (SIGKILL) signal to server"
                 "'%s'... (pid=%s)" % (server.get_id(), pid))
        kill_process(pid, force=True)

        log_info("Will now wait for server '%s' pid '%s' to die." %
                 (server.get_id(), pid))
        wait_for(pid_dead_predicate(pid), timeout=3)

    if not is_pid_alive(pid):
        log_info("Forcefully-stopped server '%s'." % server.get_id())
        return True
    else:
        log_error("Forceful stop of server '%s' failed." % server.get_id())
        return False

###############################################################################
def prompt_or_force_stop_server(server, pid,
                                force=False, try_mongo_force=True):
    if force:
        return force_stop_server(server, pid,
                                 try_mongo_force=try_mongo_force)

    def stop_func():
        return force_stop_server(server, pid,
                                 try_mongo_force=try_mongo_force)

    if try_mongo_force:
        result = prompt_execute_task("Issue the shutdown with force command?",
                                    stop_func)
    else:
        result = prompt_execute_task("Forcefully stop the server process?",
                                    stop_func)

    return result[1]

###############################################################################
def step_server_down(server, force=False):
    log_info("step down server '%s' ..." % server.get_id())

    try:
        cmd = SON( [('replSetStepDown', 10),('force', force)])
        server.disconnecting_db_command(cmd, "admin")
        log_info("Server '%s' Stepped down successfully!" % server.get_id())
        return True
    except (Exception), e:
        log_error("Failed to step down server '%s'. Cause: %s" %
                  (server.get_id(), e))
        return False

###############################################################################
def prompt_step_server_down(server, force):
    def step_down_func():
        step_server_down(server, force)

    return prompt_execute_task("Server '%s' is a primary server. "
                               "Step it down before proceeding to shutdown?" %
                               server.get_id(),
        step_down_func)

###############################################################################
# restart server
###############################################################################
def restart_server(server_id, options_override=None):
    do_restart_server(lookup_and_validate_server(server_id), options_override)

###############################################################################
def do_restart_server(server, options_override=None):
    log_info("Restarting server '%s'..." % server.get_id())

    if server.is_online():
        do_stop_server(server)
    else:
        log_info("Server '%s' is not running." % server.get_id())

    do_start_server(server, options_override)

###############################################################################
# get server status
###############################################################################
def get_server_status(server_id, verbose=False):
    server  = lookup_and_validate_server(server_id)
    return server.get_status(admin=True)

###############################################################################
# Cluster Methods
###############################################################################
def configure_cluster(cluster_id, force_primary_server_id=None):
    cluster = lookup_and_validate_cluster(cluster_id)
    force_primary_server = None
    # validate force primary
    if force_primary_server_id:
        force_primary_server = \
            lookup_and_validate_server(force_primary_server_id)

    configure_replica_cluster(cluster,force_primary_server=
                                       force_primary_server)

###############################################################################
def configure_replica_cluster(replica_cluster, force_primary_server=None):
    replica_cluster.configure_replicaset(force_primary_server=
                                           force_primary_server)

###############################################################################
def dry_run_configure_cluster(cluster_id, force_primary_server_id=None):
    cluster = lookup_and_validate_cluster(cluster_id)
    log_info("\n************ Dry Run ************\n")
    db_command = None
    force = force_primary_server_id is not None
    if cluster.is_replicaset_initialized():
        log_info("Replica set already initialized. "
                 "Making the replSetReconfig command...")
        db_command = cluster.get_replicaset_reconfig_db_command(force=force)
    else:
        log_info("Replica set has not yet been initialized."
                 " Making the replSetInitiate command...")
        db_command = cluster.get_replicaset_init_all_db_command()

    log_info("Executing the following command on the current primary:")
    log_info(document_pretty_string(db_command))

###############################################################################
def prompt_init_replica_cluster(replica_cluster,
                                suggested_primary_server):

    prompt = ("Do you want to initialize replica set cluster '%s' using "
              "server '%s'?" %
              (replica_cluster.get_id(), suggested_primary_server.get_id()))

    def init_repl_func():
        replica_cluster.initialize_replicaset(suggested_primary_server)
    prompt_execute_task(prompt, init_repl_func)

###############################################################################
def prompt_add_member_to_replica(replica_cluster, server):

    prompt = ("Do you want to add server '%s' to replica set cluster '%s'?" %
              (server.get_id(), replica_cluster.get_id()))

    def add_member_func():
        replica_cluster.add_member_to_replica(server)
    prompt_execute_task(prompt, add_member_func)

###############################################################################
# open_mongo_shell_to
###############################################################################
def open_mongo_shell_to(db_address,
                        username=None,
                        password=None,
                        shell_options={},
                        js_files=[]):
    if is_mongo_uri(db_address):
        open_mongo_shell_to_uri(db_address, username, password,
                                shell_options, js_files)
        return

    # db_address is an id string
    id_path = db_address.split("/")
    id = id_path[0]
    database = id_path[1] if len(id_path) == 2 else None

    server = lookup_server(id)
    if server:
        open_mongo_shell_to_server(server, database, username, password,
                                   shell_options, js_files)
        return

    # Maybe cluster?
    cluster = lookup_cluster(id)
    if cluster:
        open_mongo_shell_to_cluster(cluster, database, username, password,
                                    shell_options, js_files)
        return
    # Unknown destination
    raise MongoctlException("Unknown db address '%s'" % db_address)

###############################################################################
def open_mongo_shell_to_server(server,
                               database=None,
                               username=None,
                               password=None,
                               shell_options={},
                               js_files=[]):
    validate_server(server)

    if not database:
        if server.is_arbiter_server():
            database = "local"
        else:
            database = "admin"

    if server.needs_to_auth(database):
        if not username:
            username = read_input("Enter username for database"
                                  " '%s':" % database)
            password = getpass.getpass()
        elif not password:
            password = server.get_password_from_seed_users(database, username)

        if not password:
            password = getpass.getpass()

    do_open_mongo_shell_to(server.get_connection_address(),
                           database,
                           username,
                           password,
                           server.get_mongo_version(),
                           shell_options,
                           js_files)

###############################################################################
def open_mongo_shell_to_cluster(cluster,
                                database=None,
                                username=None,
                                password=None,
                                shell_options={},
                                js_files=[]):
    log_info("Locating primary server for cluster '%s' ..." % cluster.get_id())
    primary_member = cluster.get_primary_member()
    if primary_member:
        primary_server = primary_member.get_server()
        log_info("Connecting to primary server '%s'" % primary_server.get_id())
        open_mongo_shell_to_server(primary_server,
                                   database=database,
                                   username=username,
                                   password=password,
                                   shell_options=shell_options,
                                   js_files=js_files)
    else:
        log_error("No primary server found for cluster '%s'" %
                  cluster.get_id())

###############################################################################
def open_mongo_shell_to_uri(uri,
                            username=None,
                            password=None,
                            shell_options={},
                            js_files=[]):
    try:
        uri_obj = uri_parser.parse_uri(uri)
        node = uri_obj["nodelist"][0]
        host = node[0]
        port = node[1]
        database = uri_obj["database"]
        username = username if username else uri_obj["username"]
        password = password if password else uri_obj["password"]
        if not host:
            raise MongoctlException("URI '%s' is missing a host." % uri)

        address = "%s:%s" % (host, port)
        do_open_mongo_shell_to(address, database, username, password,
                               shell_options, js_files)

    except errors.ConfigurationError, e:
        raise MongoctlException("Malformed URI '%s'. %s" % (uri, e))

###############################################################################
def do_open_mongo_shell_to(address,
                           database=None,
                           username=None,
                           password=None,
                           server_version=None,
                           shell_options={},
                           js_files=[]):

    # default database to admin
    database = database if database else "admin"


    connect_cmd = [get_mongo_shell_executable(server_version),
                   "%s/%s" % (address, database)]

    if username:
        connect_cmd.extend(["-u",username, "-p"])
        if password:
            connect_cmd.extend([password])

    # append shell options
    if shell_options:
        connect_cmd.extend(options_to_command_args(shell_options))

    # append js files
    if js_files:
        connect_cmd.extend(js_files)

    cmd_display =  connect_cmd[:]
    # mask password
    if username and password:
        cmd_display[5] =  "****"


    log_info("Executing command: \n%s" % " ".join(cmd_display))
    connect_process = create_subprocess(connect_cmd)

    connect_process.communicate()

###############################################################################
def is_mongo_uri(str):
    return str and str.startswith("mongodb://")

###############################################################################
# install_mongodb
###############################################################################
def install_mongodb(version):

    bits = platform.architecture()[0].replace("bit", "")
    os_name = platform.system().lower()

    if os_name == 'darwin' and platform.mac_ver():
        os_name = "osx"

    return do_install_mongodb(os_name, bits, version)

###############################################################################
def do_install_mongodb(os_name, bits, version):

    if version is None:
        version = fetch_latest_stable_version()
        log_info("Installing latest stable MongoDB version '%s'" % version)
    # validate version string
    elif not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongodb_installs_dir = get_mongodb_installs_dir()
    if not mongodb_installs_dir:
        raise MongoctlException("No mongoDBInstallationsDirectory configured"
                                " in mongoctl.config")

    platform_spec = get_validate_platform_spec(os_name, bits)

    log_info("Running install for %s %sbit to "
             "mongoDBInstallationsDirectory=%s" % (os_name, bits,
                                                  mongodb_installs_dir))


    mongo_installation = get_mongo_installation(version)

    if mongo_installation is not None: # no-op
        log_info("You already have MongoDB %s installed ('%s'). "
                 "Nothing to do..." % (version, mongo_installation))
        return mongo_installation

    archive_name = "mongodb-%s-%s.tgz" % (platform_spec, version)
    url = "http://fastdl.mongodb.org/%s/%s" % (os_name, archive_name)

    # Validate if the version exists
    response = urllib.urlopen(url)

    if response.getcode() != 200:
        msg = ("Unable to download from url '%s' (response code '%s'). "
               "It could be that version '%s' you specified does not exist."
              " Please double check the version you provide" %
               (url, response.getcode(), version))
        raise MongoctlException(msg)

    mongo_dir_name = "mongodb-%s-%s" % (platform_spec, version)
    install_dir = os.path.join(mongodb_installs_dir, mongo_dir_name)

    ensure_dir(mongodb_installs_dir)

    if not dir_exists(install_dir):
        try:
            ## download the url
            download(url)
            extract_archive(archive_name)

            log_info("Moving extracted folder to %s" % mongodb_installs_dir)
            shutil.move(mongo_dir_name, mongodb_installs_dir)

            os.remove(archive_name)
            log_info("Deleting archive %s" % archive_name)

            log_info("MongoDB %s installed successfully!" % version)
            return install_dir
        except Exception, e:
            log_error("Failed to install MongoDB '%s'. Cause: %s" %
                      (version, e))

###############################################################################
# uninstall_mongodb
###############################################################################
def uninstall_mongodb(version):

    # validate version string
    if not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongo_installation = get_mongo_installation(version)

    if mongo_installation is None: # no-op
        msg = ("Cannot find a MongoDB installation for version '%s'. Please"
               " use list-versions to see all possible versions " % version)
        log_info(msg)
        return

    log_info("Found MongoDB '%s' in '%s'" % (version, mongo_installation))

    def rm_mongodb():
        log_info("Deleting '%s'" % mongo_installation)
        shutil.rmtree(mongo_installation)
        log_info("MongoDB '%s' Uninstalled successfully!" % version);

    prompt_execute_task("Proceed uninstall?" , rm_mongodb)

###############################################################################
def fetch_latest_stable_version():
    response = urllib.urlopen(LATEST_VERSION_FILE_URL)
    if response.getcode() == 200:
        return response.read().strip()
    else:
        raise MongoctlException("Unable to fetch MongoDB latest stable version"
                                " from '%s' (Response code %)" %
                                (LATEST_VERSION_FILE_URL, response.getcode()))
###############################################################################
def get_mongo_installation(version_str):
    # get all mongod installation dirs and return the one
    # whose version == specified version. If any...
    version = version_obj(version_str)
    for install_dir, install_version in find__all_mongo_installations():
        if install_version == version:
            return install_dir

    return None

###############################################################################
def find__all_mongo_installations():
    all_installs = []
    all_mongod_exes = find_all_executables('mongod')
    for exe_path, exe_version in all_mongod_exes:
        # install dir is exe parent's (bin) parent
        install_dir = os.path.dirname(os.path.dirname(exe_path))
        all_installs.append((install_dir,exe_version))

    return all_installs

###############################################################################
def get_validate_platform_spec(os_name, bits):

    if os_name not in ["linux", "osx", "win32", "sunos5"]:
        raise MongoctlException("Unsupported OS %s" % os_name)

    if bits == "64":
        return "%s-x86_64" % os_name
    else:
        if os_name == "linux":
            return "linux-i686"
        elif os_name in ["osx" , "win32"]:
            return "%s-i386" % os_name
        elif os_name == "sunos5":
            return "i86pc"

###############################################################################
def download(url):
    log_info("Downloading %s ..." % url)

    download_cmd = None

    if which("curl"):
        download_cmd = ['curl', '-O', url]
    elif which("wget"):
        download_cmd = ['wget', url]
    else:
        msg = ("Cannot download file.You need to have 'curl' or 'wget"
               "' command in your path in order to proceed.")
        raise MongoctlException(msg)

    execute_command(download_cmd, call=True)

def extract_archive(archive_name):
    log_info("Extracting %s ..." % archive_name)
    if not which("tar"):
        msg = ("Cannot extract archive.You need to have 'tar' command in your"
               " path in order to proceed.")
        raise MongoctlException(msg)

    tar_cmd = ['tar', 'xvf', archive_name]
    execute_command(tar_cmd, call=True)
###############################################################################
# HELPER functions
###############################################################################

###############################################################################
def prompt_execute_task(message, task_function):

    yes = prompt_confirm(message)
    if yes:
        return (True,task_function())
    else:
        return (False,None)

###############################################################################
def prompt_confirm(message):

    # return False if non-interactive or --no was specified
    if (not is_interactive_mode() or
        is_say_no_to_everything()):
        return False

    # return True if --yes was specified
    if is_say_yes_to_everything():
        return True

    valid_choices = {"yes":True,
                     "y":True,
                     "ye":True,
                     "no":False,
                     "n":False}

    while True:
        print >> sys.stderr, message + " [y/n] ",
        sys.stderr.flush()
        choice = raw_input().lower()
        if not valid_choices.has_key(choice):
            print >> sys.stderr, ("Please respond with 'yes' or 'no' "
                                  "(or 'y' or 'n').\n")
        elif valid_choices[choice]:
            return True
        else:
            return False
###############################################################################
def read_input(message, allow_null=True):
    print >> sys.stderr, message,
    return raw_input()

###############################################################################
def read_password(message='', allow_null=True):
    print >> sys.stderr, message
    return getpass.getpass()

###############################################################################
def server_stopped_predicate(server, pid):
    def server_stopped():
        return (not server.is_online() and
                (pid is None or not is_pid_alive(pid)))

    return server_stopped

###############################################################################
def server_started_predicate(server, mongod_pid):
    def server_started():
        # check if the command failed
        if not is_pid_alive(mongod_pid):
            raise MongoctlException("Could not start the server. Please check"
                                    " the log file.")

        return server.is_online()

    return server_started

###############################################################################
def pid_dead_predicate(pid):
    def pid_dead():
        return not is_pid_alive(pid)

    return pid_dead

###############################################################################
def is_forking(server, options_override):
    fork = server.is_fork()
    if options_override is not None:
        fork = get_document_property(options_override, 'fork', fork)

    if fork is None:
        fork = True

    return fork

###############################################################################
def validate_server(server):
    errors = []

    version = server.get_mongo_version()
    # None versions are ok
    if version is not None:
        if not is_valid_version(version):
            errors.append("** Invalid mongoVersion value '%s'" % version)
        elif not is_supported_mongo_version(version):
            errors.append("** mongoVersion '%s' is not supported. Please refer"
                          " to mongoctl documentation for supported"
                          " versions." % version)
    return errors

def validate_local_op(server, op):

    # If the server has been assumed to be local then skip validation
    if is_assumed_local_server(server.get_id()):
        log_verbose("Skipping validation of server's '%s' address '%s' to be"
                    " local because --assume-local is on" %
                    (server.get_id(), server.get_host_address()))
        return

    log_verbose("Validating server address: "
                "Ensuring that server '%s' address '%s' is local on this "
                "machine" % (server.get_id(), server.get_host_address()))
    if not server.is_local():
        log_verbose("Server address validation failed.")
        raise MongoctlException("Cannot %s server '%s' on this machine "
                                "because server's address '%s' is not local to"
                                " this machine." % (op,
                                                   server.get_id(),
                                                   server.get_host_address()))
    else:
        log_verbose("Server address validation passed. "
                    "Server '%s' address '%s' is local on this "
                    "machine !" % (server.get_id(), server.get_host_address()))

###############################################################################
# Server lookup functions
###############################################################################
def lookup_server(server_id):
    validate_repositories()

    server = None
    # lookup server from the config first
    if consulting_file_repository():
        server = config_lookup_server(server_id)

    # if server is not found then try from db
    if server is None and has_db_repository():
        server = db_lookup_server(server_id)

    return server

###############################################################################
def lookup_and_validate_server(server_id):
    server = lookup_server(server_id)
    if server is None:
        raise MongoctlException("Cannot find configuration for a server "
                                "with _id of '%s'." % server_id)

    validation_errors = validate_server(server)
    if len(validation_errors) > 0:
        raise MongoctlException(
            "Server '%s' configuration is not valid. Please fix errors below"
            " and try again.\n%s" % (server_id,"\n".join(validation_errors)))

    return server

###############################################################################
def db_lookup_server(server_id):
    server_collection = get_mongoctl_server_db_collection()
    server_doc =  server_collection.find_one({"_id": server_id})

    if server_doc:
        return Server(server_doc)
    else:
        return None

###############################################################################
def db_lookup_servers_by_host(host_id):
    server_collection = get_mongoctl_server_db_collection()
    return new_server_list(server_collection.find({"host.$id": host_id}))

###############################################################################
## Looks up the server from config file
def config_lookup_server(server_id):
    servers = get_configured_servers()
    return get_document_property(servers, server_id)

###############################################################################
# returns all servers configured in both DB and config file
def lookup_all_servers():
    validate_repositories()

    all_servers = []
    if consulting_file_repository():
        all_servers = list(get_configured_servers().values())
    if has_db_repository():
        all_servers.extend(db_lookup_all_servers())

    return all_servers

###############################################################################
# returns servers saved in the db collection of servers
def db_lookup_all_servers():
    servers = get_mongoctl_server_db_collection()
    return new_server_list(servers.find())

###############################################################################
# Cluster lookup functions
###############################################################################
def lookup_and_validate_cluster(cluster_id):
    cluster = lookup_cluster(cluster_id)

    if cluster is None:
        raise MongoctlException("Unknown cluster: %s" % cluster_id)

    validate_cluster(cluster)

    return cluster

###############################################################################
# Lookup by cluster id
def lookup_cluster(cluster_id):
    validate_repositories()
    cluster = None
    # lookup cluster from the config first
    if consulting_file_repository():
        cluster = config_lookup_cluster(cluster_id)

    # if cluster is not found then try from db
    if cluster is None and has_db_repository():
        cluster = db_lookup_cluster(cluster_id)

    return cluster

###############################################################################
# Looks up the server from config file
def config_lookup_cluster(cluster_id):
    clusters = get_configured_clusters()
    return get_document_property(clusters,cluster_id)

###############################################################################
def db_lookup_cluster(cluster_id):
    cluster_collection = get_mongoctl_cluster_db_collection()
    cluster_doc = cluster_collection.find_one({"_id": cluster_id})

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None

###############################################################################
# returns all clusters configured in both DB and config file
def lookup_all_clusters():
    validate_repositories()
    all_clusters = []

    if consulting_file_repository():
        all_clusters = list(get_configured_clusters().values())
    if has_db_repository():
        all_clusters.extend(db_lookup_all_clusters())

    return all_clusters

###############################################################################
# returns clusters saved in the db collection of servers
def db_lookup_all_clusters():
    clusters = get_mongoctl_cluster_db_collection()
    return new_replicaset_cluster_list(clusters.find())

###############################################################################
# Lookup by server id
def db_lookup_cluster_by_server(server):
    cluster_collection = get_mongoctl_cluster_db_collection()
    cluster_doc = cluster_collection.find_one({"members.server.$id":
                                                   server.get_id()})

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None


###############################################################################
def config_lookup_cluster_by_server(server):
    clusters = get_configured_clusters()
    return find(lambda c: c.has_member_server(server), clusters.values())

###############################################################################
def validate_cluster(cluster):
    log_info("Validating cluster '%s'..." % cluster.get_id() )

    errors = []

    ## validate repl key if needed

    if (cluster.has_any_server_that(needs_repl_key) and
        cluster.get_repl_key() is None):
        errors.append(
            "** no replKey configured. replKey is required because at least "
            "one member has 'auth' enabled.")

    if len(errors) > 0:
        raise MongoctlException("Cluster %s configuration is not valid. "
                                "Please fix errors below and try again.\n%s" %
                                (cluster.get_id() , "\n".join(errors)))

    return cluster

###############################################################################
def lookup_validate_cluster_by_server(server):
    cluster = lookup_cluster_by_server(server)

    if cluster is not None:
        validate_cluster(cluster)

    return cluster

###############################################################################
def lookup_cluster_by_server(server):
    validate_repositories()
    cluster = None

    ## look for the cluster in config
    if consulting_file_repository():
        cluster = config_lookup_cluster_by_server(server)

    ## If nothing found then look in db
    ##   (but not if server is the Mothership;
    ##    for now, its cluster gotta be in da file repository. -- TODO XXX &c.)
    if (cluster is None and has_db_repository() and
        not is_mongoctl_repo_db(server)):
        cluster = db_lookup_cluster_by_server(server)

    return cluster

def is_cluster_member(server):
    return lookup_cluster_by_server(server) is not None


def is_replica_primary(server):
    cluster =  lookup_cluster_by_server(server)
    if cluster is not None:
        member = cluster.get_member_for(server)
        return member.is_primary_member()

    return False

###############################################################################
# MONGOD Start Command functions
###############################################################################
def generate_start_command(server, options_override=None):
    command = []

    """
        Check if we need to use numactl if we are running on a NUMA box.
        10gen recommends using numactl on NUMA. For more info, see
        http://www.mongodb.org/display/DOCS/NUMA
        """
    if mongod_needs_numactl():
        log_info("Running on a NUMA machine...")
        apply_numactl(command)

    # append the mongod executable
    command.append(get_mongod_executable(server.get_mongo_version()))


    # create the command args
    cmd_options = server.export_cmd_options()

    # set the logpath if forking..

    if is_forking(server, options_override):
        cmd_options['fork'] = True
        set_document_property_if_missing(
            cmd_options,
            "logpath",
            server.get_log_file_path())

    # Add ReplicaSet args if a cluster is configured

    cluster = lookup_validate_cluster_by_server(server)
    if cluster is not None:

        set_document_property_if_missing(cmd_options,
            "replSet",
            cluster.get_id())

        # Specify the keyFile arg if needed
        if needs_repl_key(server):
            key_file_path = server.get_key_file_path()
            set_document_property_if_missing(cmd_options,
                                             "keyFile",
                                             key_file_path)

    # apply the options override
    if options_override is not None:
        for (option_name,option_val) in options_override.items():
            cmd_options[option_name] = option_val


    command.extend(options_to_command_args(cmd_options))
    return command

###############################################################################
def options_to_command_args(args):

    command_args=[]

    for (arg_name,arg_val) in sorted(args.iteritems()):
            # append the arg name and val as needed
        if not arg_val:
            continue
        elif arg_val == True:
            command_args.append("--%s" % arg_name)
        else:
            command_args.append("--%s" % arg_name)
            command_args.append(str(arg_val))

    return command_args

###############################################################################
def get_mongo_executable(server_version,
                         executable_name,
                         version_check_pref=VERSION_PREF_EXACT,
                         prompt_install=False):

    mongo_home = os.getenv(MONGO_HOME_ENV_VAR)
    mongo_installs_dir = get_mongodb_installs_dir()


    ver_disp = "[Unspecified]" if server_version is None else server_version
    log_verbose("Looking for a compatible %s for mongoVersion=%s." %
                (executable_name, ver_disp))
    exe_version_tuples = find_all_executables(executable_name)

    if len(exe_version_tuples) > 0:
        selected_exe = best_executable_match(executable_name,
                                             exe_version_tuples,
                                             server_version,
                                             version_check_pref=
                                             version_check_pref)
        if selected_exe is not None:
            log_info("Using %s at '%s'..." % (executable_name, selected_exe))
            return selected_exe

    ## ok nothing found at all. wtf case
    msg = ("Unable to find a compatible '%s' executable "
           "for version %s \n."
           "Here is your enviroment:\n\n"
           "$PATH=%s\n\n"
           "$MONGO_HOME=%s\n\n"
           "mongoDBInstallationsDirectory=%s (in mongoctl.config)" %
           (executable_name, ver_disp,
            os.getenv("PATH"),
            mongo_home,
            mongo_installs_dir))

    def install_compatible_mongodb():
        return install_mongodb(server_version)

    if prompt_install:
        log_info(msg)
        result = prompt_execute_task("Install a MongoDB compatible with "
                                     " '%s'?" % server_version,
                                     install_compatible_mongodb)
        if result[0]:
            new_mongo_home = result[1]
            return get_mongo_home_exe(new_mongo_home, executable_name)



    raise MongoctlException(msg)

###############################################################################
def find_all_executables(executable_name):
    # create a list of all available executables found and then return the best
    # match if applicable
    executables_found = []

    ####### Look in $PATH
    path_executable = which(executable_name)
    if path_executable is not None:
        add_to_executables_found(executables_found, path_executable)

    #### Look in $MONGO_HOME if set
    mongo_home = os.getenv(MONGO_HOME_ENV_VAR)

    if mongo_home is not None:
        mongo_home = resolve_path(mongo_home)
        mongo_home_exe = get_mongo_home_exe(mongo_home, executable_name)
        add_to_executables_found(executables_found, mongo_home_exe)
        # Look in mongod_installs_dir if set
    mongo_installs_dir = get_mongodb_installs_dir()

    if mongo_installs_dir is not None:
        if os.path.exists(mongo_installs_dir):
            for mongo_installation in os.listdir(mongo_installs_dir):
                child_mongo_home = os.path.join(mongo_installs_dir,
                    mongo_installation)

                child_mongo_exe = get_mongo_home_exe(child_mongo_home,
                    executable_name)

                add_to_executables_found(executables_found, child_mongo_exe)

    return get_exe_version_tuples(executables_found)

###############################################################################
def add_to_executables_found(executables_found, executable):
    if is_valid_mongo_exe(executable):
        if executable not in executables_found:
            executables_found.append(executable)
    else:
        log_verbose("Not a valid executable '%s'. Skipping..." % executable)

###############################################################################
def best_executable_match(executable_name,
                          exe_version_tuples,
                          version_str,
                          version_check_pref=VERSION_PREF_EXACT):
    version = version_obj(version_str)
    match_func = exact_exe_version_match

    exe_versions_str = exe_version_tuples_to_strs(exe_version_tuples)

    log_verbose("Found the following %s's. Selecting best match "
                "for version %s\n%s" %(executable_name, version_str,
                                       exe_versions_str))

    if version is None:
        log_verbose("mongoVersion is null. "
                    "Selecting default %s" % executable_name)
        match_func = default_match
    elif version_check_pref == VERSION_PREF_LATEST_STABLE:
        match_func = latest_stable_exe
    elif version_check_pref == VERSION_PREF_MAJOR_GE:
        match_func = major_ge_exe_version_match

    return match_func(executable_name, exe_version_tuples, version)

###############################################################################
def default_match(executable_name, exe_version_tuples, version):
    default_exe = latest_stable_exe(executable_name, exe_version_tuples)
    if default_exe is None:
        log_verbose("No stable %s found. Looking for any latest available %s "
                    "..." % (executable_name, executable_name))
        default_exe = latest_exe(executable_name, exe_version_tuples)
    return default_exe

###############################################################################
def exact_exe_version_match(executable_name, exe_version_tuples, version):

    for mongo_exe,exe_version in exe_version_tuples:
        if exe_version == version:
            return mongo_exe

    return None

###############################################################################
def latest_stable_exe(executable_name, exe_version_tuples, version=None):
    log_verbose("Find the latest stable %s" % executable_name)
    # find greatest stable exe
    # hold values in a list of (exe,version) tuples
    stable_exes = []
    for mongo_exe,exe_version in exe_version_tuples:
        # get the release number (e.g. A.B.C, release number is B here)
        release_num = exe_version.parts[0][1]
        # stable releases are the even ones
        if (release_num % 2) == 0:
            stable_exes.append((mongo_exe, exe_version))

    return latest_exe(executable_name, stable_exes)
###############################################################################
def latest_exe(executable_name, exe_version_tuples, version=None):

    # Return nothing if nothing compatible
    if len(exe_version_tuples) == 0:
        return None
        # sort desc by version
    exe_version_tuples.sort(key=lambda t: t[1], reverse=True)

    return exe_version_tuples[0][0]
###############################################################################
def major_ge_exe_version_match(executable_name, exe_version_tuples, version):
    # find all compatible exes then return closet match (min version)
    # hold values in a list of (exe,version) tuples
    compatible_exes = []
    for mongo_exe,exe_version in exe_version_tuples:
        if exe_version.parts[0][0] >= version.parts[0][0]:
            compatible_exes.append((mongo_exe, exe_version))

    # Return nothing if nothing compatible
    if len(compatible_exes) == 0:
        return None
    # find the best fit
    compatible_exes.sort(key=lambda t: t[1])
    return compatible_exes[-1][0]

###############################################################################
def get_exe_version_tuples(executables):
    exe_ver_tuples = []
    for mongo_exe in executables:
        try:
            exe_version = mongo_exe_version(mongo_exe)
            exe_ver_tuples.append((mongo_exe, exe_version))
        except Exception, e:
            log_verbose("Skipping executable '%s': %s" % (mongo_exe, e))

    return exe_ver_tuples

###############################################################################
def exe_version_tuples_to_strs(exe_ver_tuples):
    strs = []
    for mongo_exe,exe_version in exe_ver_tuples:
        strs.append("%s = %s" % (mongo_exe, exe_version))
    return "\n".join(strs)

###############################################################################
def is_valid_mongo_exe(path):
    return path is not None and is_exe(path)

###############################################################################
def get_mongod_executable(server_version):
    return get_mongo_executable(server_version,
                                'mongod',
                                version_check_pref=VERSION_PREF_EXACT,
                                prompt_install=True)

def get_mongo_shell_executable(server_version):
    return get_mongo_executable(server_version,
                                'mongo',
                                version_check_pref=VERSION_PREF_MAJOR_GE,
                                prompt_install=True)

def get_mongo_home_exe(mongo_home, executable_name):
    return os.path.join(mongo_home, 'bin', executable_name)

def mongo_exe_version(mongo_exe):
    try:
        re_expr = "v?((([0-9]+)\.([0-9]+)\.([0-9]+))([^, ]*))"
        vers_spew = execute_command([mongo_exe, "--version"])
        vers_grep = re.search(re_expr, vers_spew)
        full_version = vers_grep.groups()[0]
        result = version_obj(full_version)
        if result is not None:
            return result
        else:
            raise MongoctlException("Cannot parse mongo version from the"
                                    " output of '%s --version'" % mongo_exe)
    except Exception, e:
        raise MongoctlException("Unable to get mongo version of '%s'."
                  " Cause: %s" % (mongo_exe, e))

###############################################################################
def is_valid_version(version_str):
    return suggest_normalized_version(version_str) is not None

###############################################################################
# returns true if version is greater or equal to 1.8
def is_supported_mongo_version(version_str):
        return (version_obj(version_str)>=
                version_obj(MIN_SUPPORTED_VERSION))

###############################################################################
def version_obj(version_str):
    if version_str is None:
        return None

    #clean version string
    try:
        version_str = version_str.replace("-pre-" , "-pre")
        return MongoctlNormalizedVersion(version_str)
    except Exception, e:
        return None

###############################################################################
def prepare_server(server):
    log_info("Preparing server '%s' for use as configured..." % server.get_id())

    """ if the server is a cluster member then only setup local user because
        the rest of the users will be setup at cluster init time by the
        setup_cluster_users method
    """
    if is_cluster_member(server):
        setup_server_local_users(server)
    else:
        setup_server_users(server)
    if am_bootstrapping():
        ensure_minimal_bootstrap(server)

###############################################################################
def mk_server_dir(server):
    # ensure the dbpath dir exists
    dbpath = server.get_db_path()
    log_verbose("Ensuring that server's dbpath '%s' exists..." % dbpath)
    if ensure_dir(server.get_db_path()):
        log_verbose("dbpath %s already exists!" % dbpath)
        return True
    else:
        log_verbose("dbpath directory '%s' created successfully" % dbpath)
        return False

###############################################################################
def setup_server_users(server):

    """
    Seeds all users returned by get_seed_users() IF there are no users seed yet
    i.e. system.users collection is empty
    """
    """if not should_seed_users(server):
        log_verbose("Not seeding users for server '%s'" % server.get_id())
        return"""


    log_info("Checking if there are any users that need to be added for "
                "server '%s'..." % server.get_id())

    seed_users = server.get_seed_users()

    count_new_users = 0

    for dbname, db_seed_users in seed_users.items():
        # create the admin ones last so we won't have an auth issue
        if (dbname == "admin"):
            continue
        count_new_users += setup_server_db_users(server, dbname, db_seed_users)

    # Note: If server member of a replica then don't setup admin
    # users because primary server will do that at replinit

    # Now create admin ones
    if not server.is_slave():
        count_new_users += setup_server_admin_users(server)

    if count_new_users > 0:
        log_info("Added %s users." % count_new_users)
    else:
        log_verbose("Did not add any new users.")

###############################################################################
def setup_cluster_users(cluster, primary_server):
    log_verbose("Setting up cluster '%s' users using primary server '%s'" %
                (cluster.get_id(), primary_server.get_id()))
    return setup_server_users(primary_server)

###############################################################################
def should_seed_users(server):
    log_verbose("See if we should seed users for server '%s'" %
                server.get_id())
    try:
        connection = server.get_db_connection()
        dbnames = connection.database_names()
        for dbname in dbnames:
            if connection[dbname]['system.users'].find_one():
                return False
        return True
    except Exception,e:
        return False

###############################################################################
def should_seed_db_users(server, dbname):
    log_verbose("See if we should seed users for database '%s'" % dbname)
    try:
        connection = server.get_db_connection()
        if connection[dbname]['system.users'].find_one():
            return False
        else:
            return True
    except Exception,e:
        return False

###############################################################################
def setup_db_users(server, db, db_users):
    count_new_users = 0
    for user in db_users :
        username = user['username']
        log_verbose("adding user '%s' to db '%s'" % (username, db.name))
        password = get_document_property(user, 'password')
        if not password:
            password = read_seed_password(db.name, username)
        db.add_user(username, password)
        # if there is no login user for this db then set it to this new one
        db_login_user = server.get_login_user(db.name)
        if not db_login_user:
            server.set_login_user(db.name, username, password)
        # inc new users
        count_new_users += 1

    return count_new_users


###############################################################################
def setup_server_db_users(server, dbname, db_users):
    log_verbose("Checking if there are any users that needs to be added for "
                "database '%s'..." % dbname)

    if not should_seed_db_users(server, dbname):
        log_verbose("Not seeding users for database '%s'" % dbname)
        return 0

    db = server.get_authenticate_db(dbname)

    try:
        any_new_user_added = setup_db_users(server, db, db_users)
        if not any_new_user_added:
            log_verbose("No new users added for database '%s'" % dbname)
        return any_new_user_added
    except Exception,e:
        raise MongoctlException(
            "Error while setting up users for '%s'"\
            " database on server '%s'."
            "\n Cause: %s" % (dbname, server.get_id(), e))

###############################################################################
def setup_server_admin_users(server):

    if not should_seed_db_users(server, "admin"):
        log_verbose("Not seeding users for database 'admin'")
        return 0

    admin_users = server.get_admin_users()
    if(admin_users is None or
       len(admin_users) < 1):
        log_verbose("No users configured for admin DB...")
        return 0

    log_verbose("Checking setup for admin users...")
    count_new_users = 0
    try:
        admin_db = server.get_admin_db()

        # potentially create the 1st admin user
        count_new_users += setup_db_users(server, admin_db, admin_users[0:1])

        # the 1st-time init case:
        # BEFORE adding 1st admin user, auth. is not possible --
        #       only localhost cxn gets a magic pass.
        # AFTER adding 1st admin user, authentication is required;
        #      so, to be sure we now have authenticated cxn, re-pull admin db:
        admin_db = server.get_admin_db()

        # create the rest of the users
        count_new_users += setup_db_users(server, admin_db, admin_users[1:])
        return count_new_users
    except Exception,e:
        raise MongoctlException(
            "Error while setting up admin users on server '%s'."
            "\n Cause: %s" % (server.get_id(), e))

###############################################################################
def setup_server_local_users(server):

    if not should_seed_db_users(server, "local"):
        log_verbose("Not seeding users for database 'local'")
        return 0


    try:
        local_users = server.get_db_seed_users("local")
        local_db = server.get_db("local")
        if local_users:
            return setup_db_users(server, local_db, local_users)
        else:
            return 0
    except Exception,e:
        raise MongoctlException(
            "Error while setting up local users on server '%s'."
            "\n Cause: %s" % (server.get_id(), e))

###############################################################################
def read_seed_password(dbname, username):
    return read_password("Please create a password for user '%s' in DB '%s'" %
                         (username, dbname))

###############################################################################
# Mongoctl Database Functions
###############################################################################
def log_server_activity(server, activity):

    if is_logging_activity():
        log_record = {"op": activity,
                      "ts": datetime.datetime.utcnow(),
                      "serverDoc": server.get_document(),
                      "server": server.get_id(),
                      "serverDisplayName": server.get_description()}
        log_verbose("Logging server activity \n%s" %
                    document_pretty_string(log_record))

        get_activity_collection().insert(log_record)

###############################################################################
def is_logging_activity():
    return (has_db_repository() and
            get_mongoctl_config_val("logServerActivity" , False))

###############################################################################
def has_db_repository():
    return get_database_repository_conf() is not None

###############################################################################
def has_file_repository():
    return get_file_repository_conf() is not None

###############################################################################
# Bootstrapping the DB Repository off of the File Repository
#
# If configured with "seedDatabaseRepository" : true , then the file repository
# is only consulted long enough to start the database repository.
# We assume (for now) that the only place to be seeding the db repository from
# is the file repository, and so once seeded we only consult the latter.
#
# This seems a little overcomplicated, but then ... bootstrapping is tricky.

def consulting_file_repository():
    return has_file_repository() and not has_seeded_repository()

def is_seeding_repository():
    """returns True iff config indicates file repo intended to seed db repo"""
    return (has_db_repository() and
            get_mongoctl_config_val("seedDatabaseRepository", default=False))

def has_seeded_repository():
    """returns True iff we believe that seeding is complete & available"""
    if not is_seeding_repository() or am_bootstrapping():
        return False

    global __db_repo_seeded__
    if not __db_repo_seeded__ :
        log_verbose("Hmmm ... I wonder if db repo is up & seeded?")
        __db_repo_seeded__ = _does_db_repo_appear_seeded()
        if not __db_repo_seeded__ :
            log_verbose("Cannot confirm this here db repo is up & seeded.")
    return __db_repo_seeded__

def _does_db_repo_appear_seeded():
    """returns True iff we see seeded db repo is online & ready for action."""
    global __db_repo_checking_seeded_
    if __db_repo_checking_seeded_ :
        # if we're asking a second time, let's not blow out the stack, eh?
        return False
    try:
        __db_repo_checking_seeded_ = True # that's what we're doing, yo.
        return (is_seeding_repository() and
                get_mongoctl_database() is not None)
    except:
        return False
    finally:
        __db_repo_checking_seeded_ = False

def am_bootstrapping(sez_i=None):
    """This is basically a flag set during startup of the repo db server."""
    global __db_repo_starting__
    if sez_i is None:
        return __db_repo_starting__
    else:
        __db_repo_starting__ = sez_i

def ensure_minimal_bootstrap(server):
    """
    Make sure db repo has a document representing the mongoctl db server
    that is to serve the db repo after handoff.
    If necessary, insert the one we've been using from the file repo.
    """
    if db_lookup_server(server.get_id()) is None:
        try:
            log_verbose("Bootstrapping db repo server record from file...")
            get_mongoctl_server_db_collection().insert(server.get_document(),
                                                       safe=True)
        except Exception, e:
            log_error("Unable to ensure bootstrap of db repository!  %s" % e)


###############################################################################
# Global variables used to govern the mechanics of db repo startup & seeding
#
# Global variable: True ==> file repo contents presumed represented in db repo
__db_repo_seeded__ = False
# Global variable: True ==> checking on seeded repo (prevent inf. recursion)
__db_repo_checking_seeded_ = False
# Global variable: True ==> bootstrapping the db repo
__db_repo_starting__ = False


###############################################################################
def validate_repositories():
    if ((not has_file_repository()) and
        (not has_db_repository())):
        raise MongoctlException("Invalid 'mongoctl.config': No fileRepository"
                                " or databaseRepository configured. At least"
                                " one repository has to be configured.")

    # ensure that if db repo is a server then must have file repo
    if (has_db_repository() and
        is_db_repository_via_server_id() and
        not has_file_repository()):
        msg = ("Invalid 'mongoctl.config': You must have a fileRepository"
               " configured because databaseRepository is configured via a "
               "server id.")
        raise MongoctlException(msg)

###############################################################################
# Global variable: mongoctl's mongodb object
__mongoctl_db__ = None

###############################################################################
def get_mongoctl_database():

    # if not using db then return
    if not has_db_repository():
        return

    global __mongoctl_db__

    if __mongoctl_db__ is not None:
        return __mongoctl_db__

    log_verbose("Connecting to mongoctl db...")
    try:
        if is_db_repository_via_server_id():
            (conn, dbname) = _db_repo_connect_byref()
        else:
            (conn, dbname) = _db_repo_connect_byuri()

        __mongoctl_db__ = conn[dbname]

        return __mongoctl_db__
    except Exception, e:
        raise MongoctlException(
            "Could not establish a database"\
            " connection to mongoctl's configuration database: %s" % (e))

def is_db_repository_via_server_id():
    return (has_db_repository() and
            "server_id" in get_database_repository_conf())

def is_mongoctl_repo_db(server):
    return (is_db_repository_via_server_id() and
            (server.get_id() == get_database_repository_conf()["server_id"]))

def _db_repo_connect_byuri():
    db_conf = get_database_repository_conf()
    uri = db_conf["databaseURI"]
    conn = pymongo.Connection(uri)
    dbname = pymongo.uri_parser.parse_uri(uri)['database']
    return conn, dbname

def _db_repo_connect_byref():
    db_conf = get_database_repository_conf()
    svr = lookup_server(db_conf["server_id"])
    dbname = db_conf["db"]
    conn = svr.get_authenticate_db(dbname).connection
    return conn, dbname

###############################################################################
def get_database_repository_conf():
    return get_mongoctl_config_val('databaseRepository')

###############################################################################
def get_file_repository_conf():
    return get_mongoctl_config_val('fileRepository')

###############################################################################
def get_mongodb_installs_dir():
    installs_dir = get_mongoctl_config_val('mongoDBInstallationsDirectory')
    if installs_dir:
        return resolve_path(installs_dir)

###############################################################################
def set_mongodb_installs_dir(installs_dir):
    set_mongoctl_config_val('mongoDBInstallationsDirectory', installs_dir)

###############################################################################
def get_mongoctl_server_db_collection():

    mongoctl_db = get_mongoctl_database()
    conf = get_database_repository_conf()

    server_collection_name = get_document_property(
        conf, "servers", DEFAULT_SERVERS_COLLECTION)

    return mongoctl_db[server_collection_name]

###############################################################################
def get_mongoctl_cluster_db_collection():

    mongoctl_db = get_mongoctl_database()
    conf = get_database_repository_conf()
    cluster_collection_name = get_document_property(
        conf, "clusters", DEFAULT_CLUSTERS_COLLECTION)

    return mongoctl_db[cluster_collection_name]

###############################################################################
def get_activity_collection():

    mongoctl_db = get_mongoctl_database()

    activity_coll_name = get_mongoctl_config_val('activityCollectionName',
                                                 DEFAULT_ACTIVITY_COLLECTION)

    return mongoctl_db[activity_coll_name]


###############################################################################
#  Server/Cluster file/path related functions
###############################################################################

###############################################################################
def get_server_pid(server):
    pid_file_path = server.get_pid_file_path()
    if os.path.exists(pid_file_path):
        pid_file = open(pid_file_path, 'r')
        pid = pid_file.readline().strip('\n')
        if pid and pid.isdigit():
            return int(pid)
        else:
            log_warning("Unable to determine pid for server '%s'. "
                        "Not a valid number in '%s"'' %
                        (server.get_id(), pid_file_path))
    else:
        log_warning("Unable to determine pid for server '%s'. "
                    "pid file '%s' does not exist" %
                    (server.get_id(), pid_file_path))

    return None


###############################################################################
# we need a repl key if you are auth + a cluster member +
# version is None or >= 2.0.0
def needs_repl_key(server):
    version = server.get_mongo_version_obj()
    return (server.is_auth() and
            is_cluster_member(server) and
            (version is None or
             version >= version_obj(REPL_KEY_SUPPORTED_VERSION)))

###############################################################################
def get_generate_key_file(server):
    cluster = lookup_cluster_by_server(server)
    key_file_path = server.get_key_file_path()

    # Generate the key file if it does not exist
    if not os.path.exists(key_file_path):
        key_file = open(key_file_path, 'w')
        key_file.write(cluster.get_repl_key())
        key_file.close()
        # set the permissions required by mongod
        os.chmod(key_file_path,stat.S_IRUSR)
    return key_file_path


###############################################################################
# Configuration Functions
###############################################################################

###############################################################################
def get_mongoctl_config_val(key, default=None):
    return get_document_property(get_mongoctl_config(), key, default)

###############################################################################
def set_mongoctl_config_val(key, value):
    get_mongoctl_config()[key] = value

###############################################################################
## Global variable CONFIG: a dictionary of configurations read from config file
__mongo_config__ = None

def get_mongoctl_config():

    global __mongo_config__

    if __mongo_config__ is None:
        __mongo_config__ = read_config_json("mongoctl",
                                            MONGOCTL_CONF_FILE_NAME)

    return __mongo_config__

###############################################################################
# Global variable: lazy loaded map that holds servers read from config file
__configured_servers__ = None

###############################################################################
def get_configured_servers():

    global __configured_servers__

    if __configured_servers__ is None:
        __configured_servers__ = {}

        file_repo_conf = get_file_repository_conf()
        servers_path_or_url = get_document_property(file_repo_conf,
                                                    "servers" ,
                                                    DEFAULT_SERVERS_FILE)

        server_documents = read_config_json("servers", servers_path_or_url)
        if not isinstance(server_documents, list):
            raise MongoctlException("Server list in '%s' must be an array" %
                                    servers_path_or_url)
        for document in server_documents:
            server = Server(document)
            __configured_servers__[server.get_id()] = server

    return __configured_servers__

###############################################################################
# Global variable: lazy loaded map that holds clusters read from config file
__configured_clusters__ = None
###############################################################################
def get_configured_clusters():

    global __configured_clusters__

    if __configured_clusters__ is None:
        __configured_clusters__ = {}

        file_repo_conf = get_file_repository_conf()
        clusters_path_or_url =  get_document_property(file_repo_conf,
            "clusters" , DEFAULT_CLUSTERS_FILE)



        cluster_documents = read_config_json("clusters", clusters_path_or_url)
        if not isinstance(cluster_documents, list):
            raise MongoctlException("Cluster list in '%s' must be an array" %
                                    clusters_path_or_url)
        for document in cluster_documents:
            cluster = new_cluster(document)
            __configured_clusters__[cluster.get_id()] = cluster

    return __configured_clusters__

###############################################################################
def read_config_json(name, path_or_url):

    try:
        log_verbose("Reading %s configuration"
                    " from '%s'..." % (name, path_or_url))

        json_str = read_json_string(path_or_url)
        # minify the json/remove comments and sh*t
        json_str = minify_json.json_minify(json_str)
        json_val =json.loads(json_str,
            object_hook=json_util.object_hook)

        if not json_val and not isinstance(json_val,list): # b/c [] is not True
            raise MongoctlException("Unable to load %s "
                                    "config file: %s" % (name, path_or_url))
        else:
            return json_val
    except MongoctlException,e:
        raise e
    except Exception, e:
        raise MongoctlException("Unable to load %s "
                                "config file: %s: %s" % (name, path_or_url, e))

def read_json_string(path_or_url, validate_exists=True):
    path_or_url = to_full_config_path(path_or_url)
    # if the path is just filename then append config root

    # check if its a file
    if not is_url(path_or_url):
        if os.path.isfile(path_or_url):
            return open(path_or_url).read()
        elif validate_exists:
            raise MongoctlException("Config file %s does not exist." %
                                    path_or_url)
        else:
            return None

    # Then its url
    response = urllib.urlopen(path_or_url)

    if response.getcode() != 200:
        msg = ("Unable to open url '%s' (response code '%s')."
               % (path_or_url, response.getcode()))

        if validate_exists:
            raise MongoctlException(msg)
        else:
            log_verbose(msg)
            return None
    else:
        return response.read()

###############################################################################
# Config root / files stuff
###############################################################################
__config_root__ = DEFAULT_CONF_ROOT

def _set_config_root(root_path):
    if not is_url(root_path) and not dir_exists(root_path):
        raise MongoctlException("Invalid config-root value: %s does not"
                                " exist or is not a directory" % root_path)
    global __config_root__
    __config_root__ = root_path

###############################################################################
def to_full_config_path(path_or_url):
    global __config_root__

    # first resolve the path
    path_or_url = resolve_path(path_or_url)
    # handle abs paths and abs URLS
    if os.path.isabs(path_or_url) or is_url(path_or_url):
        return path_or_url

    else:
        return resolve_path(os.path.join(__config_root__, path_or_url))

###############################################################################
def is_url(value):
    scheme = urlparse.urlparse(value).scheme
    return  scheme is not None and scheme != ''
###############################################################################
# OS Functions
###############################################################################
def which(program):

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

###############################################################################
def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

###############################################################################
def ensure_dir(dir_path):
    """
    If DIR_PATH does not exist, makes it. Failing that, raises Exception.
    Returns True if dir already existed; False if it had to be made.
    """
    exists = dir_exists(dir_path)
    if not exists:
        try:
            os.makedirs(dir_path)
        except(Exception,RuntimeError), e:
            raise MongoctlException("Unable to create directory %s. Cause %s" %
                                    (dir_path, e))
    return exists

###############################################################################
def dir_exists(path):
    return os.path.exists(path) and os.path.isdir(path)

###############################################################################
def resolve_path(path):
    return os.path.expandvars(os.path.expanduser(path))

###############################################################################
# sub-processing functions
###############################################################################
def execute_command(command, call=False):
    if call:
        return subprocess.check_call(command)
    # Python 2.7+ : Use the new method because i think its better
    elif  hasattr(subprocess, 'check_output'):
        return subprocess.check_output(command,stderr=subprocess.STDOUT)
    else: # Python 2.6 compatible, check_output is not available in 2.6
        return subprocess.Popen(command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT).communicate()[0]

###############################################################################
__child_subprocesses__ = []

def create_subprocess(command, **kwargs):
    child_process = subprocess.Popen(command, **kwargs)

    global __child_subprocesses__
    __child_subprocesses__.append(child_process)

    return child_process

###############################################################################
def communicate_to_child_process(child_pid):
    get_child_process(child_pid).communicate()

###############################################################################
def get_child_process(child_pid):
    global __child_subprocesses__
    for child_process in __child_subprocesses__:
        if child_process.pid == child_pid:
            return child_process

###############################################################################
def is_pid_alive(pid):

    try:
        os.kill(pid,0)
        return True
    except OSError:
        return False

###############################################################################
def kill_process(pid, force=False):
    signal = 9 if force else 1
    try:
        os.kill(pid, signal)
        return True
    except OSError:
        return False

###############################################################################
# NUMA Related functions
###############################################################################
def mongod_needs_numactl():
    """ Logic kind of copied from MongoDB (mongodb-src/util/version.cpp) ;)

        Return true IF we are on a box with a NUMA enabled kernel and more
        than 1 numa node (they start at node0).
    """
    return dir_exists("/sys/devices/system/node/node1")

###############################################################################
def apply_numactl(command):

    numactl_exe = get_numactl_exe()

    if numactl_exe:
        log_info("Using numactl '%s'" % numactl_exe)
        command.extend([numactl_exe, "--interleave=all"])
    else:
        msg = ("You are running on a NUMA machine. It is recommended to run "
               "your server using numactl but we cannot find a numactl "
               "executable in your PATH. Proceeding might cause problems that"
               " will manifest in strange ways, such as massive slow downs for"
               " periods of time or high system cpu time. Proceed?")
        if not prompt_confirm(msg):
            exit(0)

###############################################################################
def get_numactl_exe():
    return which("numactl")

###############################################################################
# SIGNAL HANDLER FUNCTIONS
###############################################################################

def mongoctl_signal_handler(signal_val, frame):
    global __mongod_pid__

    # otherwise prompt to kill server
    global __child_subprocesses__
    global __current_server__

    def kill_child(child_process):
        try:
            if child_process.poll() is None:
                log_verbose("Killing child process '%s'" % child_process )
                child_process.terminate()
        except Exception, e:
            log_verbose("Unable to kill child process '%s': Cause: %s" %
                        (child_process, e))

    def exit_mongoctl():
        # kill all children then exit
        map(kill_child, __child_subprocesses__)
        exit(0)

        # if there is no mongod server yet then exit
    if __mongod_pid__ is None:
        exit_mongoctl()
    else:
        prompt_execute_task("Kill server '%s'?" % __current_server__.get_id(),
                             exit_mongoctl)

###############################################################################
# Register the global mongoctl signal handler
signal.signal(signal.SIGINT, mongoctl_signal_handler)

###############################################################################
# Network Utils Functions
###############################################################################

def is_host_local(host):
    if (host == "localhost" or
        host == "127.0.0.1"):
        return True

    return is_same_host(socket.gethostname(), host)

###############################################################################
def is_same_host(host1, host2):

    """
    Returns true if host1 == host2 OR map to the same host (using DNS)
    """

    if host1 == host2:
        return True
    else:
        ips1 = get_host_ips(host1)
        ips2 = get_host_ips(host2)
        return len(set(ips1) & set(ips2)) > 0

###############################################################################
def is_same_address(addr1, addr2):
    """
    Where the two addresses are in the host:port
    Returns true if ports are equals and hosts are the same using is_same_host
    """
    hostport1 = addr1.split(":")
    hostport2 = addr2.split(":")

    return (is_same_host(hostport1[0], hostport2[0]) and
            hostport1[1] == hostport2[1])
###############################################################################
def get_host_ips(host):
    try:

        ips = []
        addr_info = socket.getaddrinfo(host, None)
        for elem in addr_info:
            ip = elem[4]
            if ip not in ips:
                ips.append(ip)
        return ips
    except Exception, e:
        raise MongoctlException("Invalid host '%s'. Cause: %s" % (host, e))

###############################################################################
# Utility Methods
###############################################################################

###############################################################################
def set_document_property_if_missing(document, name, value):
    if get_document_property(document, name) is None:
        document[name] = value

###############################################################################
def namespace_get_property(namespace, name):
    if hasattr(namespace, name):
        return getattr(namespace,name)

    return None

###############################################################################
def document_pretty_string(document):
    return json.dumps(document, indent=4, default=json_util.default)

###############################################################################
def listify(object):
    if isinstance(object, list):
        return object

    return [object]

###############################################################################
def get_document_property(document, name, default=None):
    if document.has_key(name):
        return document[name]
    else:
        return default

###############################################################################
def wait_for(predicate, timeout = None, sleep_duration = 2):
    start_time = now()
    while (timeout is None) or (now() - start_time < timeout):

        if predicate():
            return True
        else:
            log_info("-- waiting --")
            time.sleep(sleep_duration)

    return False

###############################################################################
def now():
    return time.time()

###############################################################################
def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item
    return None

###############################################################################
# Log/Display/Input methods
###############################################################################

__logging_verbose__ = False

###############################################################################
def turn_logging_verbose_on():
    global __logging_verbose__
    __logging_verbose__ = True

###############################################################################
def log_info(msg):
    print >>sys.stderr, msg

###############################################################################
def log_error(msg):
    print >>sys.stderr,"ERROR: %s" % msg

###############################################################################
def log_warning(msg):
    print >>sys.stderr,"WARNING: %s" % msg

###############################################################################
def log_verbose(msg):
    if __logging_verbose__:
        log_info(msg)

###############################################################################
def stdout_log(msg):
    print msg

###############################################################################
def log_db_command(cmd):
    log_info( "Executing db command %s" % document_pretty_string(cmd))

###############################################################################
# Global flags and their functions
###############################################################################
__interactive_mode__ = True

def set_interactive_mode(value):
    global __interactive_mode__
    __interactive_mode__ = value

###############################################################################
def is_interactive_mode():
    global __interactive_mode__
    return __interactive_mode__

###############################################################################
__say_yes_to_everything__ = False
__say_no_to_everything__ = False

###############################################################################
def say_yes_to_everything():
    global __say_yes_to_everything__
    __say_yes_to_everything__ = True

###############################################################################
def is_say_yes_to_everything():
    global __say_yes_to_everything__
    return __say_yes_to_everything__

###############################################################################
def say_no_to_everything():
    global __say_no_to_everything__
    __say_no_to_everything__ = True

###############################################################################
def is_say_no_to_everything():
    global __say_no_to_everything__
    return __say_no_to_everything__

###############################################################################
def is_interactive_mode():
    global __interactive_mode__
    return __interactive_mode__

###############################################################################
__assumed_local_servers__ = []

def assume_local_server(server_id):
    global __assumed_local_servers__
    if server_id not in __assumed_local_servers__:
        __assumed_local_servers__.append(server_id)

###############################################################################
def is_assumed_local_server(server_id):
    global __assumed_local_servers__
    return server_id in __assumed_local_servers__

###############################################################################

def get_default_users():
    return get_mongoctl_config_val('defaultUsers', {})

###############################################################################
def get_cluster_member_alt_address_mapping():
    return get_mongoctl_config_val('clusterMemberAltAddressesMapping', {})

###############################################################################
__global_users__ = {}

def get_server_global_users(server_id):
    global __global_users__
    return get_document_property(__global_users__,server_id)

###############################################################################
__global_login_user__= {
    "serverId": None,
    "database": "admin",
    "username": None,
    "password": None}

###############################################################################
def get_global_login_user(server, dbname):
    global __global_login_user__

    # all server or exact server + db match
    if ((not __global_login_user__["serverId"] or
        __global_login_user__["serverId"] == server.get_id()) and
        __global_login_user__["username"] and
        __global_login_user__["database"] == dbname):
        return __global_login_user__

    # same cluster members and DB is not 'local'?
    if (__global_login_user__["serverId"] and
        __global_login_user__["database"] == dbname and
        dbname != "local"):
        global_login_server = lookup_server(__global_login_user__["serverId"])
        global_login_cluster = lookup_cluster_by_server(global_login_server)
        cluster = lookup_cluster_by_server(server)
        if (global_login_cluster and cluster and
            global_login_cluster.get_id() == cluster.get_id()):
            return __global_login_user__



###############################################################################
def parse_global_login_user_arg(parsed_args):
    username = namespace_get_property(parsed_args, "username")
    password = namespace_get_property(parsed_args, "password")
    server_id = namespace_get_property(parsed_args,SERVER_ID_PARAM)

    if username:
        if parsed_args.is_arg_specified("-p"):
            if not password:
                password = read_password()
            # we only validate login if -p is specified
            #server = lookup_and_validate_server(server_id)
            #if not server.is_valid_admin_login(username, password):
             #   raise MongoctlException("Login failed.")


        global __global_login_user__
        __global_login_user__['serverId'] = server_id
        __global_login_user__['username'] = username
        __global_login_user__['password'] = password


###############################################################################
def validate_users(users):
    """
    Checks users document for proper form, and returns filtered version.
    """
    result = {}

    if not isinstance(users, dict):
        log_error("users should be a document with db names for keys, "
                  "precisely like this is not: " + str(users))
        return result

    for dbname, db_users in users.items() :
        result[dbname] = []
        for usr_pass in listify(db_users) : # be lenient on the singleton.
            if ("username" not in usr_pass or "password" not in usr_pass):
                beef = "user must have 'username' and 'password' fields"
            elif not (isinstance(usr_pass["username"], basestring) and
                      isinstance(usr_pass["password"], basestring)):
                beef = "'username' and 'password' fields must be strings"
            else:
                result[dbname].append(usr_pass)
                continue
            log_error("Rejecting user %s for db %s : %s" %
                      (usr_pass, dbname, beef))
        if len(result[dbname]) < 1:
            del result[dbname]
    return result

###############################################################################
########################                   ####################################
######################## Class Definitions ####################################
########################                   ####################################
###############################################################################


###############################################################################
# Document Wrapper Class
###############################################################################
class DocumentWrapper:

    ###########################################################################
    # Constructor
    ###########################################################################

    def __init__(self, document):
        self.__document__ = document

    ###########################################################################
    # Overridden Methods
    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.__document__)

    ###########################################################################
    def get_document(self):
        return self.__document__

    ###########################################################################
    # Properties
    ###########################################################################
    def get_property(self, property_name):
        return get_document_property(self.__document__, property_name)

    ###########################################################################
    def set_property(self, name, value):
        self.__document__[name] = value

    ###########################################################################
    def get_id(self):
        return self.get_property('_id')

    ###########################################################################
    def set_id(self, value):
        self.set_property('_id', value)

###############################################################################
# Server Class
###############################################################################

class Server(DocumentWrapper):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        DocumentWrapper.__init__(self, server_doc)
        self.__db_connection__ = None
        self.__seed_users__ = None
        self.__login_users__ = {}

    ###########################################################################
    # Properties
    ###########################################################################

    ###########################################################################
    def get_description(self):
        return self.get_property("description")

    ###########################################################################
    def set_description(self, desc):
        return self.set_property("description", desc)

    ###########################################################################
    def get_db_path(self):
        dbpath =  self.get_cmd_option("dbpath")
        if dbpath is None:
            dbpath =  DEFAULT_DBPATH

        return resolve_path(dbpath)

    ###########################################################################
    def get_pid_file_path(self):
        return self.get_server_file_path("pidfilepath", PID_FILE_NAME)

    ###########################################################################
    def get_log_file_path(self):
        return self.get_server_file_path("logpath", LOG_FILE_NAME)

    ###########################################################################
    def get_key_file_path(self):
        return self.get_server_file_path("keyFile", KEY_FILE_NAME)

    ###########################################################################
    def get_server_file_path(self , cmd_prop, default_file_name):
        file_path = self.get_cmd_option(cmd_prop)
        if file_path is not None:
            return resolve_path(file_path)
        else:
            return self.get_default_file_path(default_file_name)

    ###########################################################################
    def get_default_file_path(self , file_name):
        return self.get_db_path() + os.path.sep + file_name

    ###########################################################################
    def get_address(self):
        address =  self.get_property("address")

        if address is not None:
            if address.find(":") > 0:
                return address
            else:
                return "%s:%s" % (address, self.get_port())
        else:
            return None

    ###########################################################################
    def get_address_display(self):
        display =  self.get_address()
        if display is  None:
            display = self.get_local_address()
        return display

    ###########################################################################
    def get_host_address(self):
        if self.get_address() is not None:
            return self.get_address().split(":")[0]
        else:
            return None

    ###########################################################################
    def set_address(self, address):
        self.set_property("address", address)

    ###########################################################################
    def get_local_address(self):
        return "localhost:%s" % self.get_port()

    ###########################################################################
    def get_port(self):
        port = self.get_cmd_option("port")
        if port is None:
            port =  DEFAULT_PORT
        return port

    ###########################################################################
    def set_port(self, port):
        self.set_cmd_option("port", port)

    ###########################################################################
    def is_auth(self):
        return (self.get_cmd_option("auth") or
                self.get_cmd_option("keyFile") is not None)

    ###########################################################################
    def set_auth(self,auth):
        self.set_cmd_option("auth", auth)

    ###########################################################################
    def is_master(self):
        return self.get_cmd_option("master")

    ###########################################################################
    def is_slave(self):
        return self.get_cmd_option("slave")

    ###########################################################################
    def is_fork(self):
        return self.get_cmd_option("fork")

    ###########################################################################
    def get_mongo_version(self):
        return self.get_property("mongoVersion")

    ###########################################################################
    def get_mongo_version_obj(self):
        version_str = self.get_mongo_version()
        if version_str is not None:
            return version_obj(version_str)
        else:
            return None

    ###########################################################################
    def get_cmd_option(self, option_name):
        cmd_options = self.get_cmd_options()

        if cmd_options and cmd_options.has_key(option_name):
            return cmd_options[option_name]
        else:
            return None

    ###########################################################################
    def set_cmd_option(self, option_name, option_value):
        cmd_options = self.get_cmd_options()

        if cmd_options:
            cmd_options[option_name] = option_value

    ###########################################################################
    def get_cmd_options(self):
        return self.get_property('cmdOptions')

    ###########################################################################
    def set_cmd_options(self, cmd_options):
        return self.set_property('cmdOptions' , cmd_options)

    ###########################################################################
    def export_cmd_options(self):
        cmd_options =  self.get_cmd_options().copy()
        # reset some props to exporting vals
        cmd_options['dbpath'] = self.get_db_path()
        cmd_options['pidfilepath'] = self.get_pid_file_path()
        if 'repairpath' in cmd_options:
            cmd_options['repairpath'] = resolve_path(cmd_options['repairpath'])

        return cmd_options

    ###########################################################################
    def get_seed_users(self):

        if self.__seed_users__ is None:
            seed_users = self.get_property('seedUsers')

            ## This hidden for internal user and should not be documented
            if not seed_users:
                seed_users = get_default_users()

            self.__seed_users__ = seed_users

        return self.__seed_users__

    ###########################################################################
    def get_login_user(self, dbname):
        login_user =  get_document_property(self.__login_users__, dbname)
        # if no login user found then check global login

        if not login_user:
            login_user = get_global_login_user(self, dbname)

        return login_user

    ###########################################################################
    def get_password_from_seed_users(self, dbname, username):
        # look in seed users
        db_seed_users = self.get_db_seed_users(dbname)
        if db_seed_users:
            user = find(lambda user: user['username'] == username,
                        db_seed_users)
            if user and "password" in user:
                return user["password"]

    ###########################################################################
    def set_login_user(self, dbname, username, password):
        self.__login_users__[dbname] = {
            "username": username,
            "password": password
        }
    ###########################################################################
    def has_users(self):
        users = self.get_users()
        return users is not None and len(users) > 0

    ###########################################################################
    def get_admin_users(self):
        return self.get_db_seed_users("admin")

    ###########################################################################
    def get_db_seed_users(self, dbname):
        return get_document_property(self.get_seed_users(), dbname)

    ###########################################################################
    def has_db_users(self, dbname):
        db_users = get_document_property(self.get_users(), dbname)
        return db_users is not None and db_users

    ###########################################################################
    def get_db_default_user(self, dbname):
        db_users =  self.get_db_users(dbname)
        if db_users:
            return db_users[0]



    ###########################################################################
    # DB Methods
    ###########################################################################

    def disconnecting_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except (errors.AutoReconnect),e:
            log_verbose("This is an expected exception that happens after "
                        "disconnecting db commands: %s" % e)
        finally:
            self.__db_connection__ = None

    ###########################################################################
    def timeout_maybe_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except (Exception),e:
            if "timed out" in str(e):
                log_warning("Command %s is taking a while to complete. "
                            "This is not necessarily bad. " %
                            document_pretty_string(cmd))
            else:
                raise
        finally:
            self.__db_connection__ = None

    ###########################################################################
    def db_command(self, cmd, dbname):

        if not self.is_auth() or not self.command_needs_auth(cmd):
            return self.get_db(dbname).command(cmd)
        else:
            return self.get_authenticate_db(dbname).command(cmd)

    ###########################################################################
    def command_needs_auth(self, cmd):
        if 'shutdown' in cmd and self.is_arbiter_server():
            return False

        return self.is_auth()

    ###########################################################################
    def get_authenticate_db(self, dbname):
        auth_success = False
        db = self.get_db(dbname)

        # check if we already authed
        if db.__dict__['is_authed']:
            return db

        # If the DB doesn't need to be authenticated to (or at least yet)
        # then don't authenticate. this piece of code is important for the case
        # where you are connecting to the DB on local host where --auth is on
        # but there are no admin users yet
        if not self.needs_to_auth(dbname):
            return db

        login_user = self.get_login_user(dbname)
        if not login_user and dbname != "admin":
            # if this passes then we are authed!
            admin_db = self.get_admin_db()
            return admin_db.connection[dbname]

        auth_success = self.authenticate_db(db, dbname)
        if auth_success:
            return db
        else:
            raise MongoctlException("Failed to authenticate"
                                    " to %s db" % dbname)

    ###########################################################################
    def is_valid_admin_login(self, username, password):
        admin_db = self.get_db("admin")
        return admin_db.authenticate(username, password)

    ###########################################################################
    def get_db(self, dbname):
        conn = self.get_db_connection()
        db = conn[dbname]

        if 'is_authed' not in db.__dict__:
            db.__dict__['is_authed'] = False

        return db

    ###########################################################################
    def is_online(self):
        try:
            self.new_db_connection()
            return True
        except Exception, e:
            return False

    ###########################################################################
    def is_administrable(self):
        status = self.get_status()
        if status['connection']:
            if 'error' not in status:
                return True
            else:
                log_verbose("Error while connecting to server '%s': %s " %
                            (self.get_id(), status['error']))

    ###########################################################################
    def is_online_locally(self):
        return self.is_use_local() and self.is_online()

    ###########################################################################
    def is_use_local(self):
        return (self.get_address() is None or
               is_assumed_local_server(self.get_id())
                or self.is_local())

    ###########################################################################
    def is_local(self):
        try:
            server_host = self.get_host_address()
            return server_host is None or is_host_local(server_host)
        except Exception, e:
            log_error("Unable to resolve address '%s' for server '%s'."
                      " Cause: %s" %
                      (self.get_host_address(), self.get_id(), e))
        return False

    ###########################################################################
    def get_admin_db(self):
        if self.is_auth():
            return self.get_authenticate_db("admin")
        else:
            return self.get_db("admin")

    ###########################################################################
    def needs_to_auth(self, dbname="admin"):
        # If the server is NOT local, then this depends on auth
        if not self.is_online_locally():
            return self.is_auth()

        try:
            conn = self.new_db_connection()
            db = conn[dbname]
            coll_names = db.collection_names()
            if dbname == "admin" and coll_names is not None:
                return "system.users" in coll_names
            return False
        except (RuntimeError,Exception), e:
            if "master has changed" in str(e):
                return False
            else:
                return True

    ###########################################################################
    def get_status(self, admin=False):
        status = {}
        ## check if the server is online
        try:
            self.new_db_connection()
            status['connection'] = True

            can_admin = True
            if (self.is_auth() and
                self.needs_to_auth("admin") and
                not self.has_auth_to("admin")):
                status['error'] = "Cannot authenticate"
                self.sever_db_connection()   # better luck next time!
                can_admin = False

            if admin and can_admin:
                server_summary = self.get_server_status_summary()
                status["serverStatusSummary"] = server_summary
                rs_summary = self.get_rs_status_summary()
                if rs_summary:
                    status["selfReplicaSetStatusSummary"] = rs_summary


        except (RuntimeError, Exception),e:
            status['connection'] = False
            if type(e) == MongoctlException:
                status['error'] = "%s" % e.cause
            else:
                status['error'] = "%s" % e
            if "timed out" in status['error']:
                status['timedOut'] = True
        return status

    ###########################################################################
    def get_server_status_summary(self):
        server_status = self.db_command(SON([('serverStatus', 1)]),"admin")
        server_summary  = {
            "host": server_status['host'],
            "version": server_status['version']
        }
        if ("repl" in server_status and
            "ismaster" in server_status["repl"]):
            server_summary["repl"] = {
                "ismaster": server_status["repl"]["ismaster"]
            }

        return server_summary

    ###########################################################################
    def get_rs_status_summary(self):
        if is_cluster_member(self):
            member_rs_status = self.get_member_rs_status()
            if member_rs_status:
                return {
                    "name": member_rs_status['name'],
                    "stateStr": member_rs_status['stateStr']
                }

    ###########################################################################
    def get_default_dbname(self):
        if self.is_arbiter_server():
            return "local"
        elif self.has_db_users("admin"):
            return "admin"
        elif self.has_users():
            return self.get_users().keys()[0]
        else:
            return "test"

    ###########################################################################
    def is_arbiter_server(self):
        cluster = lookup_cluster_by_server(self)
        if cluster is not None:
            return cluster.get_member_for(self).is_arbiter()
        else:
            return False

    ###########################################################################
    def get_db_connection(self):
        if self.__db_connection__ is None:
            self.__db_connection__  = self.new_db_connection()
        return self.__db_connection__

    ###########################################################################
    def sever_db_connection(self):
        if self.__db_connection__ is not None:
            self.__db_connection__.close()
        self.__db_connection__ = None

    ###########################################################################
    def new_db_connection(self):
        return self.make_db_connection(self.get_connection_address())

    ###########################################################################
    def get_connection_address(self):
        if(self.is_use_local()):
            return self.get_local_address()
        else:
            return self.get_address()

    ###########################################################################
    def make_db_connection(self, address):

        try:
            return Connection(address,
                socketTimeoutMS=CONN_TIMEOUT,
                connectTimeoutMS=CONN_TIMEOUT)
        except Exception,e:
            error_msg = "Cannot connect to %s . Cause: %s" %\
                        (address , e)
            raise MongoctlException(error_msg,cause=e)

    ###########################################################################
    def has_auth_to(self,dbname):
        db = self.get_db(dbname)
        auth_success = self.authenticate_db(db, dbname)
        return auth_success

    ###########################################################################
    def authenticate_db(self, db, dbname):
        """
        Returns True if we manage to auth to the given db, else False.
        """
        login_user = self.get_login_user(dbname)
        username = None
        password = None



        if login_user is None:
            username = read_input("Enter username for database '%s': " %
                                  dbname)
            password = read_password()
        else:
            username = login_user["username"]
            if get_document_property(login_user, "password"):
                password = login_user["password"]
            else:
                password = self.get_password_from_seed_users(dbname, username)
                if not password:
                    password = read_password("Enter password for user '%s' for"
                                             " database '%s'" % (username,
                                                                 dbname))

        # memoize the login
        self.set_login_user(dbname, username, password)
        auth_success = db.authenticate(username,password)
        # keep track of is authed on our own because pymongo does not do that
        db.__dict__['is_authed'] = auth_success
        return auth_success


    ###########################################################################
    def get_rs_config(self):
        try:
            return self.get_authenticate_db('local')['system.replset'].find_one()
        except (Exception,RuntimeError), e:
            if type(e) == MongoctlException:
                raise e
            else:
                log_verbose("Cannot get rs config from server '%s'. "
                            "cause: %s" % (self.get_id(), e))
                return None

    ###########################################################################
    def get_member_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status =  self.db_command(rs_status_cmd, 'admin')
            for member in rs_status['members']:
                if 'self' in member and member['self']:
                    return member
        except (Exception,RuntimeError), e:
            log_verbose("Cannot get rs status from server '%s'. cause: %s" %
                        (self.get_id(), e))
            return None


###############################################################################
# ReplicaSet Cluster Member Class
###############################################################################

class ReplicaSetClusterMember(DocumentWrapper):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, member_doc):
        DocumentWrapper.__init__(self, member_doc)
        self.__server__ = None

    ###########################################################################
    # Properties
    ###########################################################################

    def get_server(self):

        server_doc = self.get_property("server")
        host = self.get_property("host")

        if self.__server__ is None:
            if server_doc is not None:
                if type(server_doc) is bson.DBRef:
                    self.__server__ =  lookup_server(server_doc.id)
            elif host is not None:
                self.__server__ = new_host_member_wrapper_server(host)

        return self.__server__

    ###########################################################################
    def get_host(self):
        server = self.get_server()
        if server:
            return server.get_address()

    ###########################################################################
    def is_arbiter(self):
        return self.get_property("arbiterOnly") == True

    ###########################################################################
    def get_priority(self):
        return self.get_property("priority")

    ###########################################################################
    # Interface Methods
    ###########################################################################
    def is_primary_member(self):
        try:
            if self.is_valid() and self.get_server().is_administrable():
                result = self.get_server().db_command({"isMaster" : 1},
                                                       "admin")
                return get_document_property(result,"ismaster")
            else:
                return False
        except(Exception, RuntimeError),e:
            log_verbose("isMaster command failed on server '%s'. Cause %s" %
                        (self.get_server().get_id(), e))
            return False


    ###########################################################################
    def can_become_primary(self):
        return not self.is_arbiter() and self.get_priority() != 0

    ###########################################################################
    def get_member_repl_config(self):

        # create the member repl config with host

        member_conf = {"host": self.get_host()}

        # Add the rest of the properties configured in the document
        #  EXCEPT host/server

        ignore = ['host', 'server']

        for key,value in self.__document__.items():
            if key not in ignore :
                member_conf[key] = value

        self._apply_alt_address_mapping(member_conf)

        return member_conf

    ###########################################################################
    def _apply_alt_address_mapping(self, member_conf):

        # Not applicable to arbiters
        if self.is_arbiter():
            return

        tag_mapping = get_cluster_member_alt_address_mapping()
        if not tag_mapping:
            return

        tags = get_document_property(member_conf, "tags", {})
        for tag_name, alt_address_prop in tag_mapping.items():
            alt_address = self.get_server().get_property(alt_address_prop)

            # set the alt address if it is different than host
            if alt_address and alt_address != member_conf['host']:
                tags[tag_name] = alt_address
            else:
                log_verbose("No alt address tag value created for alt address"
                            " mapping '%s=%s' for member \n%s" %
                            (tag_name, alt_address_prop, self))

        # set the tags property of the member config if there are any
        if tags:
            member_conf['tags'] = tags

    ###########################################################################
    def read_rs_config(self):
        if self.is_valid():
            server = self.get_server()
            if server.is_administrable():
                return server.get_rs_config()
        return None

    ###########################################################################
    def is_valid(self):
        try:
            self.validate()
            return True
        except Exception, e:
            log_error("%s" % e)
            return False

    ###########################################################################
    def validate(self):
        host_conf = self.get_property("host")
        server_conf = self.get_property("server")

        # ensure that 'server' or 'host' are configured

        if server_conf is None and host_conf is None:
            msg = ("Invalid member configuration:\n%s \n"
                   "Please set 'server' or 'host'." %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)

        # validate host if set
        if host_conf and not is_valid_member_address(host_conf):
            msg = ("Invalid 'host' value in member:\n%s \n"
                   "Please make sure 'host' is in the 'address:port' form" %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)

        # validate server if set
        server = self.get_server()
        if server is None:
            msg = ("Invalid 'server' value in member:\n%s \n"
                   "Please make sure 'server' is set or points to a "
                   "valid server." %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)
        validate_server(server)
        if server.get_address() is None:
            raise MongoctlException("Invalid member configuration for server "
                                    "'%s'. address property is not set." %
                                    (server.get_id()))

    ###########################################################################
    def validate_against_current_config(self, current_rs_conf):
        """
        Validates the member document against current rs conf
            1- If there is a member in current config with _id equals to my id
                then ensure hosts addresses resolve to the same host

            2- If there is a member in current config with host resolving to my
               host then ensure that if my id is et then it
               must equal member._id

        """

        # if rs is not configured yet then there is nothing to validate
        if not current_rs_conf:
            return

        my_host = self.get_host()
        current_member_confs = current_rs_conf['members']
        err = None
        for curr_mem_conf in current_member_confs:
            if (self.get_id() and
                self.get_id() == curr_mem_conf['_id'] and
                not is_same_address(my_host, curr_mem_conf['host'])):
                err = ("Member config is not consistent with current rs "
                       "config. \n%s\n. Both have the sam _id but addresses"
                       " '%s' and '%s' do not resolve to the same host." %
                       (document_pretty_string(curr_mem_conf),
                        my_host, curr_mem_conf['host'] ))

            elif (is_same_address(my_host, curr_mem_conf['host']) and
                self.get_id() and
                self.get_id() != curr_mem_conf['_id']):
                err = ("Member config is not consistent with current rs "
                       "config. \n%s\n. Both addresses"
                       " '%s' and '%s' resolve to the same host but _ids '%s'"
                       " and '%s' are not equal." %
                       (document_pretty_string(curr_mem_conf),
                        my_host, curr_mem_conf['host'],
                        self.get_id(), curr_mem_conf['_id']))

        if err:
            raise MongoctlException("Invalid member configuration:\n%s \n%s" %
                                    (self, err))


    ###########################################################################
    def validate_against_other(self, other_member):
        err = None
        # validate _id uniqueness
        if self.get_id() and self.get_id() == other_member.get_id():
            err = ("Duplicate '_id' ('%s') found in a different member." %
                   self.get_id())

        # validate server uniqueness
        elif (self.get_property('server') and
              self.get_server().get_id() == other_member.get_server().get_id()):
            err = ("Duplicate 'server' ('%s') found in a different member." %
                   self.get_server().get_id())
        else:

            # validate host uniqueness
            h1 = self.get_host()
            h2 = other_member.get_host()

            try:

                if is_same_address(h1, h2):
                    err = ("Duplicate 'host' found. Host in '%s' and "
                            "'%s' map to the same host." % (h1, h2))

            except Exception, e:
                err = "%s" % e

        if err:
            raise MongoctlException("Invalid member configuration:\n%s \n%s" %
                                    (self, err))

###############################################################################
def is_valid_member_address(address):
    if address is None:
        return False
    host_port = address.split(":")

    return (len(host_port) == 2
            and host_port[0]
            and host_port[1]
            and str(host_port[1]).isdigit())

###############################################################################
# ReplicaSet Cluster Class
###############################################################################
class ReplicaSetCluster(DocumentWrapper):

    ###########################################################################
    # Constructor and other init methods
    ###########################################################################
    def __init__(self, cluster_document):
        DocumentWrapper.__init__(self, cluster_document)
        self.init_members()

    ###########################################################################
    def init_members(self):
        member_documents = self.get_property("members")
        self.__members__ = []

        # if members are not set then return
        if member_documents is None:
            return

        for mem_doc in member_documents:
            member = new_replicaset_cluster_member(mem_doc)
            self.__members__.append(member)

    ###########################################################################
    # Properties
    ###########################################################################
    def get_description(self):
        return self.get_property("description")

    ###########################################################################
    def get_members(self):
        return self.__members__

    ###########################################################################
    def get_members_info(self):
        info = []
        for member in self.get_members():
            server = member.get_server()
            if server is not None:
                info.append(server.get_id())
            else:
                info.append("<Invalid Member>")

        return info

    ###########################################################################
    def get_repl_key(self):
        return self.get_property("replKey")

    ###########################################################################
    # Interface Methods
    ###########################################################################

    def get_primary_member(self):
        for member in self.get_members():
            if member.is_primary_member():
                return member

        return None

    ###########################################################################
    def suggest_primary_member(self):
        for member in self.get_members():
            if(member.can_become_primary() and
               member.get_server() is not None and
               member.get_server().is_online_locally()):
                return member

    ###########################################################################
    def is_replicaset_initialized(self):
        return self.read_rs_config() is not None

    ###########################################################################
    def initialize_replicaset(self, suggested_primary_server=None):
        log_info("Initializing replica set cluster '%s' %s..." %
                 (self.get_id(),
                  "" if suggested_primary_server is None else
                  "to contain only server '%s'" %
                  suggested_primary_server.get_id()))

        ##### Determine primary server
        log_info("Determining which server should be primary...")
        primary_server = suggested_primary_server
        if primary_server is None:
            primary_member = self.suggest_primary_member()
            if primary_member is not None:
                primary_server = primary_member.get_server()

        if primary_server is None:
            raise MongoctlException("Unable to determine primary server."
                                    " At least one member server has"
                                    " to be online.")
        log_info("Selected server '%s' as primary." % primary_server.get_id())

        init_cmd = self.get_replicaset_init_all_db_command(suggested_primary_server)

        try:

            log_db_command(init_cmd)
            primary_server.timeout_maybe_db_command(init_cmd, "admin")

            # wait for replset to init
            def is_init():
                return self.is_replicaset_initialized()

            log_info("Will now wait for the replica set to initialize.")
            wait_for(is_init,timeout=60, sleep_duration=1)

            if self.is_replicaset_initialized():
                log_info("Successfully initiated replica set cluster '%s'!" %
                         self.get_id())
            else:
                msg = ("Timeout error: Initializing replicaset '%s' took "
                       "longer than expected. This does not necessarily"
                       " mean that it failed but it could have failed. " %
                       self.get_id())
                raise MongoctlException(msg)
            ## add the admin user after the set has been initiated
            ## Wait for the server to become primary though (at MongoDB's end)

            def is_primary_for_real():
                pm = self.get_member_for(primary_server)
                return pm.is_primary_member()

            log_info("Will now wait for the intended primary server to "
                     "become primary.")
            wait_for(is_primary_for_real,timeout=60, sleep_duration=1)

            if not is_primary_for_real():
                msg = ("Timeout error: Waiting for server '%s' to become "
                       "primary took longer than expected. "
                       "Please try again later." % primary_server.get_id())
                raise MongoctlException(msg)

            log_info("Server '%s' is primary now!" % primary_server.get_id())

            # setup cluster users
            setup_cluster_users(self, primary_server)

            log_info("New replica set configuration:\n%s" %
                     document_pretty_string(self.read_rs_config()))
            return True
        except Exception,e:
            raise MongoctlException("Unable to initialize "
                                    "replica set cluster '%s'. Cause: %s" %
                                    (self.get_id(),e) )

    ###########################################################################
    def configure_replicaset(self, add_server=None, force_primary_server=None):

        # Check if this is an init VS an update
        if not self.is_replicaset_initialized():
            self.initialize_replicaset()
            return

        primary_member = self.get_primary_member()

        # force server validation and setup
        if force_primary_server:
            force_primary_member = self.get_member_for(force_primary_server)
            # validate is cluster member
            if not force_primary_member:
                msg = ("Server '%s' is not a member of cluster '%s'" %
                       (force_primary_server.get_id(), self.get_id()))
                raise MongoctlException(msg)

            # validate is administrable
            if not force_primary_server.is_administrable():
                msg = ("Server '%s' is not running or has connection problems. "
                       "For more details, Run 'mongoctl status %s'" %
                       (force_primary_server.get_id(),
                        force_primary_server.get_id()))
                raise MongoctlException(msg)

            if not force_primary_member.can_become_primary():
                msg = ("Server '%s' cannot become primary. Reconfiguration of"
                       " a replica set must be sent to a node that can become"
                       " primary" % force_primary_server.get_id())
                raise MongoctlException(msg)

            if primary_member:
                msg = ("Cluster '%s' has primary server '%s'. Proceed "
                       "reconfiguring with server '%s'?" %
                       (self.get_id(),
                        primary_member.get_server().get_id(),
                        force_primary_server.get_id()))
                if not prompt_confirm(msg):
                    return
            else:
                log_info("No primary server found for cluster '%s'" %
                         self.get_id())
        elif primary_member is None:
            raise MongoctlException("Unable to determine primary server"
                                    " for replica set cluster '%s'" %
                                    self.get_id())

        cmd_server = (force_primary_server if force_primary_server
                                               else primary_member.get_server())

        log_info("Re-configuring replica set cluster '%s'..." % self.get_id())

        force = force_primary_server is not None
        rs_reconfig_cmd = \
            self.get_replicaset_reconfig_db_command(add_server=add_server,
                                                    force=force)

        try:
            log_info("Executing the following command on server '%s':"
                     "\n%s" % (cmd_server.get_id(),
                               document_pretty_string(rs_reconfig_cmd)))

            cmd_server.disconnecting_db_command(rs_reconfig_cmd, "admin")

            log_info("Re-configuration for replica set cluster '%s' ran"
                     " successfully!" % self.get_id())

            # Probably need to reconnect.  May not be primary any more.
            realized_config = self.read_rs_config()
            log_info("New replica set configuration:\n %s" %
                     document_pretty_string(realized_config))

            return True
        except Exception,e:
            raise MongoctlException("Unable to reconfigure "
                                    "replica set cluster '%s'. Cause: %s " %
                                    (self.get_id(),e) )

    ###########################################################################
    def add_member_to_replica(self, server):
        self.configure_replicaset(add_server=server)


    ###########################################################################
    def get_replicaset_reconfig_db_command(self, add_server=None, force=False):
        current_rs_conf = self.read_rs_config()
        new_config = self.make_replset_config(add_server=add_server,
                                              current_rs_conf=current_rs_conf)
        if current_rs_conf is not None:
            # update the rs config version
            new_config['version'] = current_rs_conf['version'] + 1

        log_info("Current replica set configuration:\n %s" %
                 document_pretty_string(current_rs_conf))

        return {"replSetReconfig": new_config, "force": force}

    ###########################################################################
    def get_replicaset_init_all_db_command(self, only_for_server=None):
        replset_config =\
        self.make_replset_config(only_for_server=only_for_server)

        return {"replSetInitiate": replset_config}

    ###########################################################################
    def has_member_server(self, server):
        return self.get_member_for(server) is not None

    ###########################################################################
    def get_member_for(self, server):
        for member in self.get_members():
            if (member.get_server() and
                member.get_server().get_id() == server.get_id()):
                return member

        return None

    ###########################################################################
    def is_member_configured_for(self, server):
        member = self.get_member_for(server)
        mem_conf = member.get_member_repl_config()
        rs_conf = self.read_rs_config()
        return (rs_conf is not None and
                self.match_member_id(mem_conf, rs_conf['members']) is not None)

    ###########################################################################
    def has_any_server_that(self, predicate):
        def server_predicate(member):
            server = member.get_server()
            return predicate(server) if server is not None else False

        return len(filter(server_predicate, self.get_members())) > 0

    ###########################################################################
    def get_all_members_configs(self, current_rs_conf):
        member_configs = []
        for member in self.get_members():
            member_configs.append(member.get_member_repl_config())

        return member_configs

    ###########################################################################
    def validate_members(self, current_rs_conf):

        members = self.get_members()
        length = len(members)
        for i in range(0, length):
            member = members[i]
            # basic validation
            member.validate()
            # validate member against other members
            for j in range(i+1, length):
               member.validate_against_other(members[j])

            # validate members against current config
            member.validate_against_current_config(current_rs_conf)


    ###########################################################################
    def make_replset_config(self,
                            only_for_server=None,
                            add_server=None,
                            current_rs_conf=None):

        # validate members first
        self.validate_members(current_rs_conf)
        member_confs = None
        if add_server is not None:
            member = self.get_member_for(add_server)
            member_confs = []
            member_confs.extend(current_rs_conf['members'])
            member_confs.append(member.get_member_repl_config())
        elif only_for_server is not None:
            member = self.get_member_for(only_for_server)
            member_confs = [member.get_member_repl_config()]
        else:
            member_confs = self.get_all_members_configs(current_rs_conf)

        # populate member ids when needed
        self.populate_member_conf_ids(member_confs, current_rs_conf)

        return {"_id" : self.get_id(),
                "members": member_confs}

    ###########################################################################
    def populate_member_conf_ids(self, member_confs, current_rs_conf=None):
        new_id = 0
        current_member_confs = None
        if current_rs_conf is not None:
            current_member_confs = current_rs_conf['members']
            new_id = self.max_member_id(current_member_confs) + 1

        for mem_conf in member_confs:
            if get_document_property(mem_conf, '_id') is None :
                member_id = self.match_member_id(mem_conf,
                                                 current_member_confs)

                # if there is no match then use increment
                if member_id is None:
                    member_id = new_id
                    new_id = new_id + 1

                mem_conf['_id'] = member_id

    ###########################################################################
    def match_member_id(self, member_conf, current_member_confs):
        """
        Attempts to find an id for member_conf where fom current members confs
        there exists a element.
        Returns the id of an element of current confs
        WHERE member_conf.host and element.host are EQUAL or map to same host
        """
        if current_member_confs is None:
            return None

        for curr_mem_conf in current_member_confs:
            if is_same_address(member_conf['host'], curr_mem_conf['host']):
                return curr_mem_conf['_id']

        return None

    ###########################################################################
    def max_member_id(self, member_confs):
        max_id = 0
        for mem_conf in member_confs:
            if mem_conf['_id'] > max_id:
                max_id = mem_conf['_id']
        return max_id

    ###########################################################################
    def read_rs_config(self):
        rs_conf = None
        # iterate on all members until you get a non null rs-config
        for member in self.get_members():
            rs_conf = member.read_rs_config()
            if rs_conf is not None:
                break

        return rs_conf

###############################################################################
# Mongoctl Exception class
###############################################################################
class MongoctlException(Exception):
    def __init__(self, message,cause=None):
        self.message  = message
        self.cause = cause

    def __str__(self):
        return self.message

###############################################################################
# Factory Functions
###############################################################################
def new_server(server_doc):
    return Server(server_doc)

###############################################################################
def new_host_member_wrapper_server(address):
    if not is_valid_member_address(address):
        return None

    port = int(address.split(":")[1])
    server_doc = {"_id": address,
                  "address": address,
                  "cmdOptions":{
                      "port": port
                  }}
    return Server(server_doc)

###############################################################################
def new_server_list(docs_iteratable):
    return map(new_server, docs_iteratable)

###############################################################################
def new_cluster(cluster_doc):
    return ReplicaSetCluster(cluster_doc)

###############################################################################
def new_replicaset_cluster_list(docs_iteratable):
    return map(new_cluster, docs_iteratable)

###############################################################################
def new_replicaset_cluster_member(cluster_mem_doc):
    return ReplicaSetClusterMember(cluster_mem_doc)

###############################################################################
def new_replicaset_cluster_member_list(docs_iteratable):
    return map(new_replicaset_cluster_member, docs_iteratable)

###############################################################################
# MongoctlNormalizedVersion class
# we had to inherit and override __str__ because the suggest_normalized_version
# method does not maintain the release candidate version properly
###############################################################################
class MongoctlNormalizedVersion(NormalizedVersion):
    def __init__(self, version_str):
        sugg_ver = suggest_normalized_version(version_str)
        super(MongoctlNormalizedVersion,self).__init__(sugg_ver)
        self.version_str = version_str

    def __str__(self):
        return self.version_str

###############################################################################
########################                      #################################
########################  Commandline parsing #################################
########################                      #################################
###############################################################################


###############################################################################
# Mongoctl main parser
###############################################################################


###############################################################################
def get_mongoctl_cmd_parser():
    parser = dargparse.build_parser(MONGOCTL_PARSER_DEF)
    return parser

###############################################################################
def extract_mongod_options(parsed_args):
    return extract_mongo_exe_options(parsed_args, SUPPORTED_MONGOD_OPTIONS)

###############################################################################
def extract_mongo_shell_options(parsed_args):
    return extract_mongo_exe_options(parsed_args,
                                     SUPPORTED_MONGO_SHELL_OPTIONS)

###############################################################################
def extract_mongo_exe_options(parsed_args, supported_options):
    options_extract = {}

    # Iterating over parsed options dict
    # Yeah in a hacky way since there is no clean documented way of doing that
    # See http://bugs.python.org/issue11076 for more details
    # this should be changed when argparse provides a cleaner way

    for (option_name,option_val) in parsed_args.__dict__.items():
        if option_name in supported_options and option_val is not None:
            options_extract[option_name] = option_val

    return options_extract

###############################################################################
# Supported mongod command-line options/flags
###############################################################################

SUPPORTED_MONGOD_OPTIONS = [
    "verbose",
    "quiet",
    "port",
    "bind_ip",
    "maxConns",
    "objcheck",
    "logpath",
    "logappend",
    "pidfilepath",
    "keyFile",
    "nounixsocket",
    "unixSocketPrefix",
    "fork",
    "auth",
    "cpu",
    "dbpath",
    "diaglog",
    "directoryperdb",
    "journal",
    "journalOptions",
    "journalCommitInterval",
    "ipv6",
    "jsonp",
    "noauth",
    "nohttpinterface",
    "nojournal",
    "noprealloc",
    "notablescan",
    "nssize",
    "profile",
    "quota",
    "quotaFiles",
    "rest",
    "repair",
    "repairpath",
    "slowms",
    "smallfiles",
    "syncdelay",
    "sysinfo",
    "upgrade",
    "fastsync",
    "oplogSize",
    "master",
    "slave",
    "source",
    "only",
    "slavedelay",
    "autoresync",
    "replSet",
    "configsvr",
    "shardsvr",
    "noMoveParanoia"
]


SUPPORTED_MONGO_SHELL_OPTIONS = [
    "shell",
    "norc",
    "quiet",
    "eval",
    "verbose",
    "ipv6",
]


###############################################################################
########################                   ####################################
########################     BOOTSTRAP     ####################################
########################                   ####################################
###############################################################################

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (SystemExit, KeyboardInterrupt) , e:
        if e.code == 0:
            pass
        else:
            raise
    except:
        traceback.print_exc()
