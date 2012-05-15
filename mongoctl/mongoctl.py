#!/usr/bin/env python

###############################################################################
#
#  Copyright (C) 2012 ObjectLabs Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

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

from dargparse import dargparse
from pymongo import Connection
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

DEFAULT_CONF_ROOT = os.path.expanduser("~/.mongoctl")

MONGOCTL_CONF_FILE_NAME = "mongoctl.config"

KEY_FILE_NAME = "keyFile"

DEFAULT_SERVERS_FILE = "servers.config"

DEFAULT_CLUSTERS_FILE = "clusters.config"

DEFAULT_SERVERS_COLLECTION = "servers"

DEFAULT_CLUSTERS_COLLECTION = "clusters"

DEFAULT_ACTIVITY_COLLECTION = "logs.server-activity"

# This is mongodb's default port
DEFAULT_PORT = 27017

# db connection timeout, 10 seconds
CONN_TIMEOUT = 10000

# when requesting OS resource caps governing # of mongod connections,
MAX_DESIRED_FILE_HANDLES = 65536

# VERSION CHECK PREFERENCE CONSTS
VERSION_PREF_EXACT = 0
VERSION_PREF_GREATER = 1
VERSION_PREF_MAJOR_GE = 2
VERSION_PREF_ANY = 3

# Version support stuff
MIN_SUPPORTED_VERSION = "1.8"
REPL_KEY_SUPPORTED_VERSION = '2.0.0'

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
    # Parse options
    parser = get_mongoctl_cmd_parser()

    if len(args) < 1:
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

    # set conf root if specified
    if parsed_args.configRoot is not None:
        _set_config_root(parsed_args.configRoot)
    elif os.getenv(CONF_ROOT_ENV_VAR) is not None:
        _set_config_root(os.getenv(CONF_ROOT_ENV_VAR))

    # get the function to call from the parser framework
    command_function = parsed_args.func

    # parse credentials
    server_id = namespace_get_property(parsed_args,SERVER_ID_PARAM)

    if server_id is not None:
        parse_global_credentials(server_id, args)
        # check if assertLocal was specified
        assert_local = namespace_get_property(parsed_args,"assertLocal")
        if assert_local:
            assert_local_server(server_id)
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
            options_override=options_override)

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

    formatter = "%-15s %-35s %-30s"
    title = formatter % ("_ID", "DESCRIPTION", "ADDRESS")
    print title
    print "="*len(title)

    for server in servers:
        desc = ("" if server.get_description() is None
                else server.get_description())
        pbe = ("" if server.get_address() is None
               else server.get_address())

        print formatter % (server.get_id(), desc, pbe )
    print "\n"


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
    connect_to_server(parsed_options.server)

###############################################################################
# configure cluster command
###############################################################################
def configure_cluster_command(parsed_options):
    if parsed_options.dryRun:
        dry_run_configure_cluster(parsed_options.cluster)
    else:
        configure_cluster(parsed_options.cluster)

###############################################################################
# list clusters command
###############################################################################
def list_clusters_command(parsed_options):
    clusters = lookup_all_clusters()
    if not clusters or len(clusters) < 1:
        log_info("No clusters configured");
        return

    print "_id            description                     members"
    print "-----------------------------------------------------"
    DUH = '???'
    for cluster in clusters:
        desc = (DUH if cluster.get_description() is None
                else cluster.get_description())

        members_info = ", ".join(cluster.get_members_info())
        print "%-14s %-24s [ %s ]" % (cluster.get_id(), desc, members_info)
    print "\n"

###############################################################################
# show cluster command
###############################################################################
def show_cluster_command(parsed_options):
    print lookup_cluster(parsed_options.cluster)

###############################################################################
########################                   ####################################
########################    Mongoctl API   ####################################
########################                   ####################################
###############################################################################


###############################################################################
# start server
###############################################################################

def start_server(server_id,options_override=None):
    do_start_server(lookup_and_validate_server(server_id),
        options_override=options_override)

###############################################################################
def do_start_server(server, options_override=None):
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
    if not is_my_repo:
        log_server_activity(server, "start")

    start_server_process(server,options_override)

    if is_my_repo:
        log_server_activity(server, "start")

    # if the server belongs to a replicaset cluster,
    # then prompt the user to init the replicaset IF not already initialized
    # AND server is NOT an Arbiter

    cluster = lookup_cluster_by_server(server)

    if cluster is not None:
        log_verbose("Server '%s' is a member in the configuration for"
                    " cluster '%s'." % (server.get_id(),cluster.get_id()))

        if not cluster.is_replicaset_initialized():
            log_info("Replica set cluster '%s' has not been initialized yet." % 
                     cluster.get_id())
            if cluster.get_member_for(server).can_become_primary():
                prompt_init_replica_cluster(cluster, server)
            else:
                log_info("Skipping replica set initialization because "
                         "server '%s' cannot be elected primary." %
                         server.get_id())
        else:
            log_verbose("No need to initialize cluster '%s', as it has"
                        " already been initialized." % cluster.get_id())

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
    mongod_process = subprocess.Popen(start_cmd, stdout=child_process_out,
                                      preexec_fn=_set_process_limits)
    return mongod_process


def start_server_process(server,options_override=None):

    mongod_process = _start_server_process_4real(server, options_override)

    log_info("Will now wait for server '%s' to start up."
             " Enjoy mongod's log for now!\n" %
             server.get_id())
    log_info("************ Tailing server '%s' log file '%s' *************\n" %
             (server.get_id(), get_log_file_path(server)))
    log_tailer = tail_server_log(server)
    # wait until the server starts
    is_online = wait_for(
        server_started_predicate(server,mongod_process),
        timeout=300)

    # stop tailing
    stop_tailing(log_tailer)

    if not is_online:
        raise MongoctlException("Unable to start server '%s'" %
                                server.get_id())

    log_info("************ Server '%s' is up!! ***************" %
             server.get_id())

    try:
        prepare_server(server)
    except Exception,e:
        log_error("Unable to fully prepare server '%s'. Cause: %s \n" 
                  "Stop server now if more preparation is desired..." %
                  (server.get_id(), e))
        if shall_we_terminate(mongod_process):
            return

    log_info("Server '%s' started successfully! (pid=%s)" %
             (server.get_id(),get_server_pid(server)))

    if not is_forking(server, options_override):
        mongod_process.communicate()

###############################################################################
def _set_process_limits():
    (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
    log_info("Maximizing OS file descriptor limits for mongod process...\n"
             "\t Current limit values:   soft = %d   hard = %d" %
             (soft, hard))
    new_soft_floor = soft
    new_soft_ceiling = min(hard, MAX_DESIRED_FILE_HANDLES)
    new_soft = new_soft_ceiling # be optimistic for initial attempt
    while new_soft_ceiling - new_soft_floor > 1:
        try:
            log_verbose("Trying setrlimit(resource.RLIMIT_NOFILE, (%d, %d))" %
                        (new_soft, hard))
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            log_verbose("  That worked!  Should I ask for more?")
            new_soft_floor = new_soft
        except:
            log_verbose("  Phooey.  That didn't work.")
            new_soft_ceiling = new_soft - 1
        new_soft = (new_soft_ceiling + new_soft_floor) / 2

    log_info("Resulting OS file descriptor limits for mongod process:  "
             "soft = %d   hard = %d" % 
             resource.getrlimit(resource.RLIMIT_NOFILE))
            
    
###############################################################################
def tail_server_log(server):
    try:
        logpath = get_log_file_path(server)
        # touch log file to make sure it exists
        log_verbose("Touching log file '%s'" % logpath)
        execute_command(["touch", logpath])

        tail_cmd = ["tail", "-f", logpath]
        log_verbose("Executing command: %s" % (" ".join(tail_cmd)))
        return subprocess.Popen(tail_cmd)
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
def shall_we_terminate(mongod_process):
    def killit():
        mongod_process.terminate()
        log_info("Server process terminated at operator behest.")

    (condemned, _) = prompt_execute_task("Kill server now?", killit)
    return condemned

###############################################################################
def dry_run_start_server_cmd(server_id, options_override=None):
    server = lookup_and_validate_server(server_id)
    # ensure that the start was issued locally. Fail otherwise
    validate_local_op(server, "start")

    log_info("************** Dry Run *****************")

    start_cmd = generate_start_command(server, options_override)
    start_cmd_str = " ".join(start_cmd)

    log_info("Command : \n")
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
            log_info("Unable to stop server: Server '%s' is not responding "
                     "(connection time out); "
                     "there might some server running on the"
                     " same port %s" %
                     (server.get_id(), server.get_port()))
            can_stop_mongoly = False
        else:
            log_info("Server '%s' is not running." %
                     server.get_id())
            return

    log_info("Stopping server '%s'..." % server.get_id())
    # log server activity stop
    log_server_activity(server, "stop")
    # TODO: Enable this again when stabilized
    # step_down_if_needed(server, force)

    if can_stop_mongoly:
        shutdown_success = mongo_stop_server(server, force=False)

    if not can_stop_mongoly or not shutdown_success:
        shutdown_success = \
            prompt_or_force_stop_server(server,
                                        force,
                                        try_mongo_force=can_stop_mongoly)

    if shutdown_success:
        log_info("Server '%s' has stopped." % server.get_id())
    else:
        log_error("Unable to stop server '%s'." % server.get_id())

###############################################################################
def step_down_if_needed(server, force):
    ## if server is a primary replica member then step down
    if is_replica_primary(server):
        if force:
            step_server_down(server, force)
        else:
            prompt_step_server_down(server, force)

###############################################################################
def mongo_stop_server(server, force=False):
    server_pid = get_server_pid(server)

    try:
        shutdown_cmd = SON( [('shutdown', 1),('force', force)])
        log_info("\nSending the following command to %s:\n%s\n" %
                 (server.get_connection_address(),
                  document_pretty_string(shutdown_cmd)))
        server.disconnecting_db_command(shutdown_cmd, "admin")

        log_info("Will now wait for server '%s' to stop." % server.get_id())
        # Check that the server has stopped
        wait_for(server_stopped_predicate(server,server_pid),timeout=3)

        if server.is_online():
            log_error("Shutdown command failed...")
            return False
        else:
            return True
    except Exception, e:
        log_error("Failed to gracefully stop server '%s'. Cause: %s" %
                  (server.get_id(), e))
        return False

###############################################################################
def force_stop_server(server, try_mongo_force=True):
    success = False
    if try_mongo_force:
        success = mongo_stop_server(server, force=True)

    if not success or not try_mongo_force:
        success = kill_stop_server(server)

    return success

###############################################################################
def kill_stop_server(server):
    pid = get_server_pid(server)
    if pid is None:
        log_error("Cannot forcefully stop the server because the server's"
                  " pid cannot be determined; pid file %s does not exist. " %
                  get_pid_file_path(server))
        return False

    log_info("Force stopping server '%s'...\n" % server.get_id())
    log_info("Sending kill -1 (HUP) signal to server '%s'... (pid=%s)" %
             (server.get_id(), pid))

    kill_process(pid, force=False)

    log_info("Will now wait for server '%s' pid '%s' to die" %
             (server.get_id(), pid))
    wait_for(pid_dead_predicate(pid), timeout=3)

    if is_pid_alive(pid):
        log_error("Failed to kill server process with -1 (HUP).")
        log_info("Sending kill -9 (SIGKILL) signal to server"
                 "'%s'... (pid=%s)" % (server.get_id(), pid))
        kill_process(pid, force=True)

        log_info("Will now wait for server '%s' pid '%s' to die" %
                 (server.get_id(), pid))
        wait_for(pid_dead_predicate(pid), timeout=3)

    if not is_pid_alive(pid):
        log_info("Forcefully-stopped server '%s'." % server.get_id())
        return True
    else:
        log_error("Forceful stop of server '%s' failed." % server.get_id())
        return False

###############################################################################
def prompt_or_force_stop_server(server, force=False, try_mongo_force=True):
    if force:
        return force_stop_server(server, try_mongo_force=try_mongo_force)

    def stop_func():
        return force_stop_server(server, try_mongo_force=try_mongo_force)

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
    if verbose:
        return server.get_full_status()
    else:
        return server.get_status()

###############################################################################
# Cluster Methods
###############################################################################
def configure_cluster(cluster_id):
    cluster = lookup_and_validate_cluster(cluster_id)
    configure_replica_cluster(cluster)

###############################################################################
def configure_replica_cluster(replica_cluster):
    replica_cluster.configure_all()

###############################################################################
def dry_run_configure_cluster(cluster_id):
    cluster = lookup_and_validate_cluster(cluster_id)
    log_info("************** Dry Run *****************")
    db_command = None

    if cluster.is_replicaset_initialized():
        log_info("Replica set already initialized. "
                 "Make replSetReconfig command...")
        db_command = cluster.get_configure_all_db_command()
    else:
        log_info("Replica set not initialized."
                 " Make replSetInitiate command...")
        db_command = cluster.get_replicaset_init_all_db_command()

    log_info("Command : \n")
    log_info(document_pretty_string(db_command))

###############################################################################
def prompt_init_replica_cluster(replica_cluster,
                                suggested_primary_server):

    prompt = ("Do you want to initialize replica set cluster '%s' using "
              "server '%s'?" % 
              (replica_cluster.get_id(), suggested_primary_server.get_id()))

    def init_repl_func():
        replica_cluster.initialize_replicaset(
            suggested_primary_server=suggested_primary_server,
            only_for_server=suggested_primary_server)
    prompt_execute_task(prompt, init_repl_func)

###############################################################################
# connect_to_server
###############################################################################
def connect_to_server(server_id):
    do_connect_to_server(lookup_and_validate_server(server_id))

###############################################################################
def do_connect_to_server(server):

    if not server.is_online():
        log_info("Server '%s' is not running." % server.get_id())
        return

    log_info("Connecting to server '%s'..." % server.get_id())

    dbname = server.get_default_dbname()

    connect_cmd = [get_mongo_shell_executable(server),
                   "%s/%s" % (server.get_connection_address(),
                              dbname)]

    if server.is_auth() and server.needs_to_auth(dbname):
        db_cred = server.get_db_default_credential(dbname)
        connect_cmd.extend(["-u",
                            db_cred['username'],
                            "-p",
                            db_cred['password']])

    connect_process = subprocess.Popen(connect_cmd)

    connect_process.communicate()

###############################################################################
# HELPER functions
###############################################################################

###############################################################################
def prompt_execute_task(message, task_function):

    if not is_interactive_mode():
        return False

    valid_choices = {"yes":True,
                     "y":True,
                     "ye":True,
                     "no":False,
                     "n":False}

    while True:
        print message + " [y/n] "
        choice = raw_input().lower()
        if not valid_choices.has_key(choice):
            print("Please respond with 'yes' or 'no' "
                  "(or 'y' or 'n').\n")
        elif valid_choices[choice]:
            return (True,task_function())
        else:
            return (False,None)

###############################################################################
def server_stopped_predicate(server, server_pid):
    def server_stopped():
        return (not server.is_online() and
                (server_pid is None or not is_pid_alive(server_pid)))

    return server_stopped

###############################################################################
def server_started_predicate(server, mongod_process):
    def server_started():
        # check if the command failed
        if(mongod_process.poll() is not None):
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
    if server.get_cmd_options() is None:
        errors.append("** cmdOptions not configured")
    if server.get_db_path() is None:
        errors.append("** dbpath not configured" )
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

    # If the server has been asserted to be local then skip validation
    if is_asserted_local_server(server.get_id()):
        log_verbose("Skipping validation of server's '%s' address '%s' to be"
                    " local because --assert-local is on" %
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
    if has_file_repository():
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
    if has_file_repository():
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

    validate_cluster(cluster)

    return cluster

###############################################################################
# Lookup by cluster id
def lookup_cluster(cluster_id, quiet=False):
    validate_repositories()
    cluster = None
    # lookup cluster from the config first
    if has_file_repository():
        cluster = config_lookup_cluster(cluster_id)

    # if cluster is not found then try from db
    if cluster is None and has_db_repository():
        cluster = db_lookup_cluster(cluster_id)

    if cluster is None and not quiet:
        raise MongoctlException("Unknown cluster: %s" % cluster_id)

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

    if has_file_repository():
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
            "** no replKey configured. replKey is required because some "
            "members has 'auth' turned on.")

    if len(errors) > 0:
        raise MongoctlException("Cluster %s configuration is not valid. "
                                "Please fix above below and try again.\n%s" %
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
    if has_file_repository():
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

    # append the mongod executable
    command.append(get_mongod_executable(server))


    # create the command args
    cmd_options = server.get_cmd_options().copy()

    # add pid . if missing of-course :)
    set_document_property_if_missing(cmd_options, "pidfilepath",
        get_pid_file_path(server))

    # set the logpath if forking..

    if is_forking(server, options_override):
        set_document_property_if_missing(
            cmd_options,
            "logpath",
            get_log_file_path(server))

    # Add ReplicaSet args if a cluster is configured

    cluster = lookup_validate_cluster_by_server(server)
    if cluster is not None:

        set_document_property_if_missing(cmd_options,
            "replSet",
            cluster.get_id())

        # Specify the keyFile arg if needed
        if needs_repl_key(server):
            key_file_path = get_key_file_path(server)
            set_document_property_if_missing(cmd_options,
                                             "keyFile",
                                             key_file_path)

    # apply the options override
    if options_override is not None:
        for (option_name,option_val) in options_override.items():
            # TODO: ignoring fork because we never use --fork
            # well fork should not be in options override to begin with
            if option_name != 'fork':
                cmd_options[option_name] = option_val


    command.extend(options_to_command_args(cmd_options))
    return command

###############################################################################
def options_to_command_args(args):

    command_args=[]

    for (arg_name,arg_val) in sorted(args.iteritems()):
        # TODO: better way of ignoring fork( we never use mogod  with --fork
        if arg_name == 'fork':
            continue
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
def get_mongo_executable(server,
                         executable_name,
                         version_check_pref=VERSION_PREF_EXACT):

    server_version = server.get_mongo_version()
    ver_disp = "[Unspecified]" if server_version is None else server_version
    log_verbose("Looking for a compatible %s for server '%s' with "
                "mongoVersion=%s." %
                (executable_name, server.get_id(), ver_disp))
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
        mongo_home_exe = get_mongo_home_exe(mongo_home, executable_name)
        add_to_executables_found(executables_found, mongo_home_exe)
    # Look in $MONGO_VERSIONS if set
    mongo_versions = os.getenv(MONGO_VERSIONS_ENV_VAR)

    if mongo_versions is not None:
        if os.path.exists(mongo_versions):
            for mongo_installation in os.listdir(mongo_versions):
                child_mongo_home = os.path.join(mongo_versions,
                                                mongo_installation)

                child_mongo_exe = get_mongo_home_exe(child_mongo_home,
                                                     executable_name)

                add_to_executables_found(executables_found, child_mongo_exe)


    if len(executables_found) > 0:
        selected_exe = best_executable_match(executable_name,
                                             executables_found,
                                             server_version,
                                             version_check_pref=
                                             version_check_pref)
        if selected_exe is not None:
            log_info("Using %s '%s'" % (executable_name, selected_exe))
            return selected_exe

    ## ok nothing found at all. wtf case
    msg = ("Unable to find a compatible '%s' executable "
           "for version %s "
           "specified in configuration of server '%s'.\n"
           "Here is your enviroment:\n\n"
           "$PATH=%s\n\n"
           "$MONGO_HOME=%s\n\n"
           "$MONGO_VERSIONS=%s" %
           (executable_name, ver_disp, server.get_id(),
            os.getenv("PATH"),
            mongo_home,
            mongo_versions))
    raise MongoctlException(msg)

###############################################################################
def add_to_executables_found(executables_found, executable):
    if is_valid_mongo_exe(executable):
        if executable not in executables_found:
            executables_found.append(executable)
    else:
        log_verbose("Not a valid executable '%s'. Skipping..." % executable)

###############################################################################
def best_executable_match(executable_name,
                          executables,
                          version_str,
                          version_check_pref=VERSION_PREF_EXACT):

    if version_str is None:
        return executables[0]

    version = version_obj(version_str)
    match_func = exact_exe_version_match

    if version_check_pref == VERSION_PREF_MAJOR_GE:
        match_func = major_ge_exe_version_match

    return match_func(executables, version)

###############################################################################
def exact_exe_version_match(executables, version):

    for mongo_exe in executables:
        if mongo_exe_version(mongo_exe) == version:
            return mongo_exe

    return None

###############################################################################
def major_ge_exe_version_match(executables, version):
    # find all compatible exes then return closet match (min version)
    # hold values in a list of (exe,version) tuples
    compatible_exes = []
    for mongo_exe in executables:
        exe_version = mongo_exe_version(mongo_exe)
        if exe_version.parts[0][0] >= version.parts[0][0]:
            compatible_exes.append((mongo_exe, exe_version))

    # Return nothing if nothing compatible
    if len(compatible_exes) == 0:
        return None
    # find the best fit
    compatible_exes.sort(key=lambda t: t[1])
    return compatible_exes[0][0]

###############################################################################
def is_valid_mongo_exe(path):
    return path is not None and is_exe(path)

###############################################################################
def get_mongod_executable(server):
    return get_mongo_executable(server,
                                'mongod',
                                version_check_pref=VERSION_PREF_EXACT)

def get_mongo_shell_executable(server):
    return get_mongo_executable(server,
                                'mongo',
                                version_check_pref=VERSION_PREF_MAJOR_GE)

def get_mongo_home_exe(mongo_home, executable_name):
    return os.path.join(mongo_home, 'bin', executable_name)

def mongo_exe_version(mongo_exe):
    try:
        re_expr = "v?((([0-9]+).([0-9]+).([0-9]+))([^, ]*))"
        vers_spew = execute_command([mongo_exe, "--version"])
        vers_grep = re.search(re_expr, vers_spew)
        full_version = vers_grep.groups()[0]
        return version_obj(full_version)
    except Exception, e:
        log_error("Unable to get mongo version of '%s'."
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
    #clean version string
    try:
        version_str = version_str.replace("-pre-" , "-pre")
        return NormalizedVersion(suggest_normalized_version(version_str))
    except Exception, e:
        return None

###############################################################################
def prepare_server(server):
    log_info("Preparing server '%s' for use as configured..." % server.get_id())
    setup_server_credentials(server)

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
def setup_server_credentials(server):
    """
    FOR NOW: tries to make sure all the specified users exist.
    TODO: see comments
    """
    # TODO: update passwords
    # TODO: remove unwanted users?
    log_info("Setting up credentials for server '%s'..." % server.get_id())
    credentials = server.get_credentials()

    for dbname, db_credentials in credentials.items():
        # create the admin ones last so we won't have an auth issue
        if (dbname == "admin"):
            continue
        setup_server_db_credentials(server, dbname, db_credentials)

    # Note: If server member of a replica then don't setup admin
    # credentials because primary server will do that at replinit

    # Now create admin ones
    if (not server.is_slave() and
        not is_cluster_member(server)):
        setup_server_admin_credentials(server)

###############################################################################
def setup_db_credentials(db, db_credentials):
    existing_users = [d['user'] for d in db['system.users'].find()]
    for credential in db_credentials :
        username = credential['username']
        if username not in existing_users :
            log_verbose("adding user '%s' to db '%s'" % (username, db.name))
            db.add_user(username, credential['password'])
        else:
            log_verbose("user '%s' already present in db '%s'" % (username, db.name))
            #TODO: check password?


def setup_server_db_credentials(server, dbname, db_credentials):
    log_info("Adding credentials for the '%s' database..." % dbname)

    db = server.get_authenticate_db(dbname)

    try:
        setup_db_credentials(db, db_credentials)
        log_info("Credentials for the '%s' database on server '%s' "
                 "have been setup successfully." %
                 (dbname, server.get_id()))
    except Exception,e:
        raise MongoctlException(
            "Error while setting up credentials for '%s'"\
            " database on server '%s'."
            "\n Cause: %s" % (dbname, server.get_id(), e))

###############################################################################
def setup_server_admin_credentials(server):

    admin_credentials = server.get_admin_credentials()
    if(admin_credentials is None or
       len(admin_credentials) < 1):
        log_info("No credentials configured for admin DB...")
        return

    log_info("Setting up admin credentials...")

    try:
        admin_db = server.get_admin_db()

        # potentially create the 1st admin user
        setup_db_credentials(admin_db, admin_credentials[0:1])

        # the 1st-time init case:
        # BEFORE adding 1st admin user, auth. is not possible --
        #       only localhost cxn gets a magic pass.
        # AFTER adding 1st admin user, authentication is required;
        #      so, to be sure we now have authenticated cxn, re-pull admin db:
        admin_db = server.get_admin_db()

        # create the rest of the credentials
        setup_db_credentials(admin_db, admin_credentials[1:])

        log_info("Successfully set up admin credentials on server '%s'." % 
                 server.get_id())
    except Exception,e:
        raise MongoctlException(
            "Error while setting up admin credentials on server '%s'."
            "\n Cause: %s" % (server.get_id(), e))

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
def validate_repositories():
    if ((not has_file_repository()) and
        (not has_db_repository())):
        raise MongoctlException("Invalid 'mongoctl.config': No fileRepository"
                                " or databaseRepository configured. At least"
                                " one repository has to be configured.")

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
    pid_file_path = get_pid_file_path(server)
    if os.path.exists(pid_file_path):
        pid_file = open(pid_file_path, 'r')
        pid = pid_file.readline().strip('\n')
        if pid and pid.isdigit():
            return int(pid)

    return None

###############################################################################
def get_pid_file_path(server):
    return get_server_file_path(server , PID_FILE_NAME)

###############################################################################
def get_log_file_path(server):
    return get_server_file_path(server , LOG_FILE_NAME)

###############################################################################
def get_key_file_path(server):
    return get_server_file_path(server , KEY_FILE_NAME)

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
    key_file_path = get_key_file_path(server)

    # Generate the key file if it does not exist
    if not os.path.exists(key_file_path):
        key_file = open(key_file_path, 'w')
        key_file.write(cluster.get_repl_key())
        key_file.close()
        # set the permissions required by mongod
        os.chmod(key_file_path,stat.S_IRUSR)
    return key_file_path

###############################################################################
def get_server_file_path(server , file_name):
    return server.get_db_path() + os.path.sep + file_name

###############################################################################
# Configuration Functions
###############################################################################

###############################################################################
def get_mongoctl_config_val(key, default=None):
    return get_document_property(get_mongoctl_config(), key, default)

###############################################################################
## Global variable CONFIG: a dictionary of configurations read from config file
__mongo_config__ = None

def get_mongoctl_config():

    global __mongo_config__

    if __mongo_config__ is None:
        conf_file_path = get_config_file_path(MONGOCTL_CONF_FILE_NAME)
        __mongo_config__ = read_config_json("mongoctl",
            conf_file_path)


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
        servers_file_name =  get_document_property(file_repo_conf,
            "servers" , DEFAULT_SERVERS_FILE)

        servers_conf_path = get_config_file_path(servers_file_name)

        if os.path.exists(servers_conf_path):
            server_documents = read_config_json("servers",
                servers_conf_path)
            if not isinstance(server_documents, list):
                raise MongoctlException("Server list in '%s'"
                                        " must be an array" %
                                        (servers_conf_path))
            for document in server_documents:
                server = Server(document)
                __configured_servers__[server.get_id()] = server

        else:
            raise MongoctlException("Error while reading servers from "
                                    "file repository:"
                                    "No such file %s" % servers_conf_path)

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
        clusters_file_name =  get_document_property(file_repo_conf,
            "clusters" , DEFAULT_CLUSTERS_FILE)

        clusters_conf_path = get_config_file_path(clusters_file_name)

        if os.path.exists(clusters_conf_path):
            cluster_documents = read_config_json("clusters",
                clusters_conf_path)
            if not isinstance(cluster_documents, list):
                raise MongoctlException("Cluster list in '%s' "
                                        "must be an array" %
                                        (clusters_conf_path))
            for document in cluster_documents:
                cluster = new_cluster(document)
                __configured_clusters__[cluster.get_id()] = cluster

    return __configured_clusters__

###############################################################################
def read_config_json(name, path):

    # check if the file exists
    if not os.path.exists(path):
        raise MongoctlException("Config file %s does not exist" % path)

    try:
        log_verbose("Reading %s configuration"
                    " from '%s'..." % (name, path))

        json_str = open(path).read()
        # minify the json/remove comments and sh*t
        json_str = minify_json.json_minify(json_str)
        json_val =json.loads(json_str,
            object_hook=json_util.object_hook)

        if not json_val and not isinstance(json_val,list): # b/c [] is not True
            raise MongoctlException("Unable to load %s "
                                    "config file: %s" % (name,path))
        else:
            return json_val
    except MongoctlException,e:
        raise e
    except Exception, e:
        raise MongoctlException("Unable to load %s "
                                "config file: %s: %s" % (name, path, e))

###############################################################################
# Config root / files stuff
###############################################################################
__config_root__ = DEFAULT_CONF_ROOT

def _set_config_root(root_path):
    if not dir_exists(root_path):
        raise MongoctlException("Invalid config-root value: %s does not"
                                " exist or is not a directory" % root_path)
    global __config_root__
    __config_root__ = root_path

###############################################################################
def get_config_file_path(file_name):
    global __config_root__
    return os.path.join(__config_root__, file_name)

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
def execute_command(command):

    # Python 2.7+ : Use the new method because i think its better
    if  hasattr(subprocess, 'check_output'):
        return subprocess.check_output(command)
    else: # Python 2.6 compatible, check_output is not available in 2.6
        return subprocess.Popen(command,
                                stdout=subprocess.PIPE).communicate()[0]

###############################################################################
def get_environment():
    return os.environ

###############################################################################
def is_pid_alive(pid):

    try:
        os.kill(pid,0)
        return True
    except OSError:
        return False

def kill_process(pid, force=False):
    signal = 9 if force else 1
    try:
        os.kill(pid, signal)
        return True
    except OSError:
        return False

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
__asserted_local_servers__ = []

def assert_local_server(server_id):
    global __asserted_local_servers__
    if server_id not in __asserted_local_servers__:
        __asserted_local_servers__.append(server_id)

###############################################################################
def is_asserted_local_server(server_id):
    global __asserted_local_servers__
    return server_id in __asserted_local_servers__

###############################################################################

def get_default_credentials():
    return get_mongoctl_config_val('defaultCredentials', {})

###############################################################################
__global_credentials__ = {}

def get_server_global_credentials(server_id):
    global __global_credentials__
    return get_document_property(__global_credentials__,server_id)

def parse_global_credentials(server_id, args):
    server_glolbal_credentials = {}
    index = 0
    for arg in args:
        if arg == "--credential":
            parsed_crd = parse_credential(args[index+1])
            dbname = parsed_crd["dbname"]
            db_crd = get_document_property(server_glolbal_credentials, dbname)
            if db_crd is None:
                db_crd = []
                server_glolbal_credentials[dbname] = db_crd

            db_crd.append({"username": parsed_crd["username"],
                           "password": parsed_crd["password"]})


        index += 1

    if len(server_glolbal_credentials) > 0:
        __global_credentials__[server_id] = server_glolbal_credentials

def parse_credential(credential_arg):

    if not is_valid_credential_arg(credential_arg):
        raise MongoctlException("Invalid credential argument '%s'."
                                " credential must be in "
                                "dbname:user:pass format" % credential_arg)
    crd_arr = credential_arg.split(":")

    return {"dbname": crd_arr[0],
            "username": crd_arr[1],
            "password": crd_arr[2]}

def is_valid_credential_arg(credential_arg):
    return credential_arg.count(":") == 2

def validate_credentials(credentials):
    """
    Checks credentials document for proper form, and returns filtered version.
    """
    result = {}

    if not isinstance(credentials, dict):
        log_error("Credentials should be a document with db names for keys, "
                  "precisely like this is not: " + str(credentials))
        return result

    for dbname, dbcreds in credentials.items() :
        result[dbname] = []
        for usr_pass in listify(dbcreds) : # be lenient on the singleton.
            if ("username" not in usr_pass or "password" not in usr_pass):
                beef = "credential must have 'username' and 'password' fields"
            elif not (isinstance(usr_pass["username"], basestring) and
                      isinstance(usr_pass["password"], basestring)):
                beef = "'username' and 'password' fields must be strings"
            else:
                result[dbname].append(usr_pass)
                continue
            log_error("Rejecting credential %s for db %s : %s" %
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
        self.__credentials__ = None

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
        return self.get_cmd_option("dbpath")

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
        return "127.0.0.1:%s" % self.get_port()

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
        return self.get_cmd_option("auth")

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

    def set_cmd_options(self, cmd_options):
        return self.set_property('cmdOptions' , cmd_options)

    ###########################################################################
    def get_credentials(self):

        if self.__credentials__ is None:
            credentials = self.get_property('credentials')

            ## TODO: this should be removed later
            if credentials is None or len(credentials) < 1:
                credentials = get_default_credentials()

            server_gloabl_cred = get_server_global_credentials(self.get_id())

            # merge global credentials with configured ones
            if server_gloabl_cred is not None:
                for dbname,glbl_db_crd in server_gloabl_cred.items():
                    db_crd = get_document_property(credentials, dbname)
                    if db_crd is None:
                        db_crd = []
                        credentials[dbname] = db_crd

                    db_crd.extend(glbl_db_crd)

            self.__credentials__ = validate_credentials(credentials)

        return self.__credentials__

    ###########################################################################
    def has_credentials(self):
        credentials = self.get_credentials()
        return credentials is not None and len(credentials) > 0

    ###########################################################################
    def get_admin_credentials(self):
        return self.get_db_credentials("admin")

    ###########################################################################
    def get_db_credentials(self, dbname):
        return get_document_property(self.get_credentials(), dbname)

    ###########################################################################
    def has_db_credentials(self, dbname):
        db_cred = get_document_property(self.get_credentials(), dbname)
        return db_cred is not None and db_cred

    ###########################################################################
    def get_db_default_credential(self, dbname):
        if not self.has_db_credentials(dbname):
            raise MongoctlException("No DB credentials for db %s" % dbname)

        return self.get_db_credentials(dbname)[0]



    ###########################################################################
    # DB Methods
    ###########################################################################

    def disconnecting_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            self.__db_connection__ = None
            return result
        except (errors.AutoReconnect),e:
            log_verbose("This is an expected exception that happens after "
                        "disconnecting db commands: %s" % e)

    ###########################################################################
    def timeout_maybe_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            self.__db_connection__ = None
            return result
        except (Exception),e:
            if "timed out" in str(e):
                log_warning("Command %s is taking a while to complete. "
                            "This is not necessarily bad. " %
                            document_pretty_string(cmd))
            else:
                raise e

    ###########################################################################
    def db_command(self, cmd, dbname):

        if self.is_auth():
            return self.get_authenticate_db(dbname).command(cmd)
        else:
            return self.get_db(dbname).command(cmd)

    ###########################################################################
    def get_authenticate_db(self, dbname):
        auth_success = False
        db = self.get_db(dbname)

        # If the DB doesn't need to be authenticated to (or at least yet)
        # then don't authenticate. this piece of code is important for the case
        # where you are connecting to the DB on local host where --auth is on
        # but there are no admin users yet
        if not self.needs_to_auth(dbname):
            return db
        # If the db has credentials then use them, otherwise use an
        # authenticated admin db to grab required db
        if self.has_db_credentials(dbname):
            auth_success = self.authenticate_db(db, dbname)
        elif self.has_db_credentials("admin") and dbname != "admin":
            # if this passes then we are authed!
            admin_db = self.get_admin_db()
            auth_success = True
            db =  admin_db.connection[dbname]
        else:
            raise MongoctlException("No credentials found for db %s."
                                    " Need to have credentials for %s or"
                                    " admin db" % (dbname,dbname))
        if not auth_success:
            raise MongoctlException("Failed to authenticate"
                                    " to %s db" % dbname)
        return db

    ###########################################################################
    def get_db(self, dbname):
        conn = self.get_db_connection()
        return conn[dbname]

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
        return status['connection'] and 'error' not in status

    ###########################################################################
    def is_online_locally(self):
        return self.is_use_local() and self.is_online()

    ###########################################################################
    def is_use_local(self):
        return (self.get_address() is None or
               is_asserted_local_server(self.get_id())
                or self.is_local())

    ###########################################################################
    def is_local(self):
        try:
            server_host = self.get_host_address();
            if (server_host is None or
                server_host == "localhost" or
                server_host == "127.0.0.1"):
                return True

            localhost = socket.gethostname()
            return (socket.gethostbyname(localhost) ==
                    socket.gethostbyname(server_host))
        except(Exception, RuntimeError), e:
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
            return True

    ###########################################################################
    def get_full_status(self):
        status = self.get_status()
        status['otherDetails'] = "TBD"
        return status

    ###########################################################################
    def get_status(self):
        status = {}
        ## check if the server is online
        try:
            self.new_db_connection()
            status['connection'] = True
            default_dbname = self.get_default_dbname()

            if self.is_auth() and self.needs_to_auth(default_dbname):
                has_auth = self.has_auth_to(default_dbname)
                if not has_auth:
                    status['error'] = "Cannot authenticate"

            # TODO: discuss with Angela
            status["serverStatusSummary"] = {
                "host": "TBD",
                "version": "TBD",
                "repl" : {"ismaster": "TBD"}
                }

            # TODO: discuss with Angela
            status["selfReplicaSetStatusSummary"] = {
                "name": "TBD",
                "stateStr": "TBD"
                }

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
    def get_default_dbname(self):
        if self.is_arbiter_server():
            return "local"
        elif self.has_db_credentials("admin"):
            return "admin"
        elif self.has_credentials():
            return self.get_credentials().keys()[0]
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
        db_cred = self.get_db_credentials(dbname)
        if db_cred is not None:
            for a_cred in db_cred:
                if db.authenticate(a_cred['username'],
                                   a_cred['password']):
                    return True
        return False

    ###########################################################################
    def get_rs_config(self):
        try:
            return self.get_db('local')['system.replset'].find_one()
        except (Exception,RuntimeError), e:
            log_verbose("Cannot get rs config from server '%s'. cause: %s" %
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

        if (server_doc is not None and
            self.__server__ is None):

            if type(server_doc) is bson.DBRef:
                self.__server__ =  lookup_server(server_doc.id)

        return self.__server__

    ###########################################################################
    def get_host(self):
        host = self.get_property("host")
        if host is None:
            host = self.get_server().get_address()

        return host

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
    def get_repl_config(self):

        # create the repl config with host

        repl_config = {"host": self.get_host()}

        # Add the rest of the properties configured in the document
        #  EXCEPT host/server

        ignore = ['host', 'server']

        for key,value in self.__document__.items():
            if key not in ignore :
                repl_config[key] = value

        return repl_config

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
        server = self.get_server()
        if server is None:
            msg = ("Invalid 'server' configuration in member:\n%s \n"
                   "Please make sure 'server' is set or points to a "
                   "valid server." %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)
        validate_server(server)
        if server.get_address() is None:
            raise MongoctlException("Invalid member configuration for server "
                                    "'%s'. address property is not set." %
                                    (server.get_id()))

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
                info.append( member.get_host())

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
    def initialize_replicaset(self,
                              suggested_primary_server=None,
                              only_for_server=None):
        log_info("Initializing replica set cluster '%s'..." % self.get_id())

        ##### Determine primary server
        log_info("Determining which server should be primary...")
        primary_server = suggested_primary_server
        if primary_server is None:
            primary_server = only_for_server
        if primary_server is None:
            primary_member = self.suggest_primary_member()
            if primary_member is not None:
                primary_server = primary_member.get_server()

        if primary_server is None:
            raise MongoctlException("Unable to determine primary server."
                                    " At least one member server has"
                                    " to be online.")
        log_info("Selected server '%s' as primary." % primary_server.get_id())

        init_cmd = self.get_replicaset_init_all_db_command(only_for_server)

        try:

            log_db_command(init_cmd)
            primary_server.timeout_maybe_db_command(init_cmd, "admin")

            # wait for replset to init
            def is_init():
                return self.is_replicaset_initialized()

            log_info("Will now wait for the replicaset to initialize")
            wait_for(is_init,timeout=60, sleep_duration=1)

            if self.is_replicaset_initialized():
                log_info("Successfully initiated replica set cluster '%s'!" %
                         self.get_id())
            else:
                msg = ("Timeout error: Initializing replicaset '%s' took "
                       "longer than expected. This does not necessarily"
                       " mean that it failed but it could have failed. ")
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
            log_info("Attempting to add user to the admin database...")
            setup_server_admin_credentials(primary_server)
            return True
        except Exception,e:
            raise MongoctlException("Unable to initialize "
                                    "replica set cluster '%s'. Cause: %s" %
                                    (self.get_id(),e) )

    ###########################################################################
    def configure_all(self, suggested_primary_server=None):

        # Check if this is an init VS an update
        if not self.is_replicaset_initialized():
            self.initialize_replicaset(
                suggested_primary_server=suggested_primary_server)
            return

        primary_member = self.get_primary_member()

        if primary_member is None:
            raise MongoctlException("Unable to determine primary server"
                                    " for replica set cluster '%s'" %
                                    self.get_id())
        primary_server = primary_member.get_server()
        log_info("Re-configuring replica set cluster '%s'..." % self.get_id())


        rs_reconfig_cmd = self.get_configure_all_db_command()

        try:
            log_info("Executing the following command on the current primary:"
                     "\n%s" % document_pretty_string(rs_reconfig_cmd))

            primary_server.db_command(rs_reconfig_cmd, "admin")
            log_info("Replica set cluster '%s' re-configuration ran"
                     " successfully!" % self.get_id())
            return True
        except Exception,e:
            raise MongoctlException("Unable to reconfigure "
                                    "replica set cluster '%s'. Cause: %s " %
                                    (self.get_id(),e) )

    ###########################################################################
    def get_configure_all_db_command(self):
        current_rs_conf = self.read_rs_config()
        new_config = self.make_replset_config(current_rs_conf=current_rs_conf)
        if current_rs_conf is not None:
            # update the rs config version
            new_config['version'] = current_rs_conf['version'] + 1

        log_info("Current config:\n %s" %
                 document_pretty_string(current_rs_conf))

        log_info("New config:\n %s" % document_pretty_string(new_config))

        return {"replSetReconfig":new_config};

    ###########################################################################
    def get_replicaset_init_all_db_command(self, only_for_server=None):
        replset_config =\
        self.make_replset_config(only_for_server=only_for_server)

        return {"replSetInitiate": replset_config};

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
    def has_any_server_that(self, predicate):
        def server_predicate(member):
            server = member.get_server()
            return predicate(server) if server is not None else False

        return len(filter(server_predicate, self.get_members())) > 0

    ###########################################################################
    def get_all_members_configs(self, validate_members=True):

        member_configs = []
        for member in self.get_members():
            if validate_members:
                member.validate()
            member_configs.append(member.get_repl_config())

        return member_configs

    ###########################################################################
    def make_replset_config(self, only_for_server=None, current_rs_conf=None):

        member_confs = None
        if only_for_server is None:
            member_confs = self.get_all_members_configs()
        else:
            member = self.get_member_for(only_for_server)
            member.validate()
            member_confs = [member.get_repl_config()]

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
                member_id = self.get_member_id_if_exists(mem_conf,
                    current_member_confs)
                if member_id is None:
                    member_id = new_id
                    new_id = new_id + 1

                mem_conf['_id'] = member_id

    ###########################################################################
    def get_member_id_if_exists(self, member_conf, current_member_confs):
        if current_member_confs is None:
            return None

        for curr_mem_conf in current_member_confs:
            if member_conf['host'] == curr_mem_conf['host']:
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
            if member.is_valid():
                server = member.get_server()
                if server.is_administrable():
                    rs_conf = member.get_server().get_rs_config()
                if rs_conf is not None:
                    break

        return rs_conf

    ###########################################################################


    def match_members_to_rs_conf(self):

        current_rs_conf = self.read_rs_config()
        result = {
            "docMembers" : [],
            "rsConfMembers" : current_rs_conf
        }
        current_member_confs = None
        if current_rs_conf is not None:
            current_member_confs = current_rs_conf['members']

        member_confs = self.get_all_members_configs(validate_members=False)
        for mem_conf in member_confs:
            doc_member = {
                "member" : mem_conf,
                "matchedBy": None
            }

            result["docMembers"].append(doc_member)


            if current_rs_conf:
                for rs_mem in current_rs_conf['members']:
                    # try to match by id
                    if ("_id" in mem_conf and
                        mem_conf["_id"] == rs_mem["_id"]):
                        doc_member["matchedBy"] = "_id"
                    elif "host" in mem_conf and mem_conf["host"] == rs_mem["host"]:
                        doc_member["matchedBy"] = "host"

        return result






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
def is_supported_mongod_option(option_name):
    return option_name in SUPPORTED_MONGOD_OPTIONS

###############################################################################
def mongod_config_supports_arg(mongod_option_cfg, option_name):
    mongod_arg = mongod_option_cfg['mongod_arg']
    return option_name in listify(mongod_arg)

###############################################################################
def extract_mongod_options(parsed_args):
    options_override = {}

    # Iterating over parsed options dict
    # Yeah in a hacky way since there is no clean documented way of doing that
    # See http://bugs.python.org/issue11076 for more details
    # this should be changed when argparse provides a cleaner way

    for (option_name,option_val) in parsed_args.__dict__.items():
        if is_supported_mongod_option(option_name) and option_val is not None:
            options_override[option_name] = option_val

    return options_override

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
