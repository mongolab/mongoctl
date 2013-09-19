__author__ = 'abdul'

import os
import stat
import subprocess
import re
import signal
import resource

import mongoctl.repository as repository

from mongoctl.commands.command_utils import (
    options_to_command_args, extract_mongo_exe_options
)

from mongoctl.mongoctl_logging import *
from mongoctl.errors import MongoctlException
from mongoctl import users
from mongoctl.proccess import communicate_to_child_process, create_subprocess
from mongoctl.prompt import prompt_execute_task
from mongoctl.utils import (
    ensure_dir, which, wait_for, dir_exists, is_pid_alive
)
from tail_log import tail_server_log, stop_tailing
from mongoctl.commands.command_utils import (
    get_mongo_executable, VERSION_PREF_EXACT
    )

from mongoctl.prompt import prompt_confirm

from mongoctl.objects.mongod import MongodServer
from mongoctl.objects.mongos import MongosServer

###############################################################################
# CONSTS
###############################################################################
# OS resource limits to impose on the 'mongod' process (see setrlimit(2))
PROCESS_LIMITS = [
    # Many TCP/IP connections to mongod ==> many threads to handle them ==>
    # RAM footprint of many stacks.  Ergo, limit the stack size per thread:
    ('RLIMIT_STACK', "stack size (in bytes)", 1024 * 1024),
    # Speaking of connections, we'd like to be able to have a lot of them:
    ('RLIMIT_NOFILE', "number of file descriptors", 65536)
]

###############################################################################
# start command
###############################################################################

def start_command(parsed_options):
    server_id = parsed_options.server
    server = repository.lookup_and_validate_server(server_id)
    options_override = extract_server_options(server, parsed_options)

    if parsed_options.dryRun:
        dry_run_start_server_cmd(server, options_override)
    else:
        start_server(server,
                     options_override=options_override,
                     rs_add=parsed_options.rsAdd)



###############################################################################
def extract_server_options(server, parsed_args):
    if isinstance(server, MongodServer):
        return extract_mongo_exe_options(parsed_args, SUPPORTED_MONGOD_OPTIONS)
    elif isinstance(server, MongosServer):
        return extract_mongo_exe_options(parsed_args, SUPPORTED_MONGOS_OPTIONS)


###############################################################################
def dry_run_start_server_cmd(server, options_override=None):
    # ensure that the start was issued locally. Fail otherwise
    server.validate_local_op("start")

    log_info("************ Dry Run ************\n")

    start_cmd = generate_start_command(server, options_override)
    start_cmd_str = " ".join(start_cmd)

    log_info("\nCommand:")
    log_info("%s\n" % start_cmd_str)



###############################################################################
# start server
###############################################################################
def start_server(server, options_override=None, rs_add=False):
    do_start_server(server,
                    options_override=options_override,
                    rs_add=rs_add)

###############################################################################
__mongod_pid__ = None
__current_server__ = None

###############################################################################
def do_start_server(server, options_override=None, rs_add=False):
    # ensure that the start was issued locally. Fail otherwise
    server.validate_local_op("start")

    log_info("Checking to see if server '%s' is already running"
             " before starting it..." % server.id)
    status = server.get_status()
    if status['connection']:
        log_info("Server '%s' is already running." %
                 server.id)
        return
    elif "timedOut" in status:
        raise MongoctlException("Unable to start server: Server '%s' seems to"
                                " be already started but is"
                                " not responding (connection timeout)."
                                " Or there might some server running on the"
                                " same port %s" %
                                (server.id, server.get_port()))
    # check if there is another process running on the same port
    elif "error" in status and ("closed" in status["error"] or
                                        "reset" in status["error"] or
                                        "ids don't match" in status["error"]):
        raise MongoctlException("Unable to start server: Either server '%s' is "
                                "started but not responding or port %s is "
                                "already in use." %
                                (server.id, server.get_port()))

    # do necessary work before starting the mongod process
    _pre_server_start(server, options_override=options_override)

    server.log_server_activity("start")

    server_pid = start_server_process(server, options_override)

    _post_server_start(server, server_pid)

    # Note: The following block has to be the last block
    # because server_process.communicate() will not return unless you
    # interrupt the server process which will kill mongoctl, so nothing after
    # this block will be executed. Almost never...

    if not server.is_fork():
        communicate_to_child_process(server_pid)

###############################################################################
def _pre_server_start(server, options_override=None):
    if isinstance(server, MongodServer):
        _pre_mongod_server_start(server, options_override=options_override)

###############################################################################
def _pre_mongod_server_start(server, options_override=None):
    """
    Does necessary work before starting a server

    1- An efficiency step for arbiters running with --no-journal
        * there is a lock file ==>
        * server must not have exited cleanly from last run, and does not know
          how to auto-recover (as a journalled server would)
        * however:  this is an arbiter, therefore
        * there is no need to repair data files in any way ==>
        * i can rm this lockfile and start my server
    """

    lock_file_path = server.get_lock_file_path()

    no_journal = (server.get_cmd_option("nojournal") or
                  (options_override and "nojournal" in options_override))
    if (os.path.exists(lock_file_path) and
            server.is_arbiter_server() and
            no_journal):

        log_warning("WARNING: Detected a lock file ('%s') for your server '%s'"
                    " ; since this server is an arbiter, there is no need for"
                    " repair or other action. Deleting mongod.lock and"
                    " proceeding..." % (lock_file_path, server.id))
        try:
            os.remove(lock_file_path)
        except Exception, e:
            raise MongoctlException("Error while trying to delete '%s'. "
                                    "Cause: %s" % (lock_file_path, e))


###############################################################################
def _post_server_start(server, server_pid, **kwargs):
    if isinstance(server, MongodServer):
        _post_mongod_server_start(server, server_pid, **kwargs)

###############################################################################
def _post_mongod_server_start(server, server_pid, **kwargs):
    try:

        # prepare the server
        prepare_mongod_server(server)
        maybe_config_server_repl_set(server, rs_add=kwargs.get("rs_add"))
    except Exception,e:
        log_error("Unable to fully prepare server '%s'. Cause: %s \n"
                  "Stop server now if more preparation is desired..." %
                  (server.id, e))
        shall_we_terminate(server_pid)
        exit(1)

###############################################################################
def prepare_mongod_server(server):
    """
     Contains post start server operations
    """
    log_info("Preparing server '%s' for use as configured..." %
             server.id)

    # setup the local users
    users.setup_server_local_users(server)

    if not server.is_cluster_member():
        users.setup_server_users(server)

###############################################################################
def shall_we_terminate(mongod_pid):
    def killit():
        utils.kill_process(mongod_pid, force=True)
        log_info("Server process terminated at operator behest.")

    (condemned, _) = prompt_execute_task("Kill server now?", killit)
    return condemned

###############################################################################
def maybe_config_server_repl_set(server, rs_add=False):
    # if the server belongs to a replica set cluster,
    # then prompt the user to init the replica set IF not already initialized
    # AND server is NOT an Arbiter
    # OTHERWISE prompt to add server to replica if server is not added yet

    cluster = server.get_replicaset_cluster()

    if cluster is not None:
        log_verbose("Server '%s' is a member in the configuration for"
                    " cluster '%s'." % (server.id,cluster.id))

        if not cluster.is_replicaset_initialized():
            log_info("Replica set cluster '%s' has not been initialized yet." %
                     cluster.id)
            if cluster.get_member_for(server).can_become_primary():
                if rs_add:
                    cluster.initialize_replicaset(server)
                else:
                    prompt_init_replica_cluster(cluster, server)
            else:
                log_info("Skipping replica set initialization because "
                         "server '%s' cannot be elected primary." %
                         server.id)
        else:
            log_verbose("No need to initialize cluster '%s', as it has"
                        " already been initialized." % cluster.id)
            if not cluster.is_member_configured_for(server):
                if rs_add:
                    cluster.add_member_to_replica(server)
                else:
                    prompt_add_member_to_replica(cluster, server)
            else:
                log_verbose("Server '%s' is already added to the replicaset"
                            " conf of cluster '%s'." %
                            (server.id,cluster.id))


###############################################################################
def prompt_init_replica_cluster(replica_cluster,
                                suggested_primary_server):

    prompt = ("Do you want to initialize replica set cluster '%s' using "
              "server '%s'?" %
              (replica_cluster.id, suggested_primary_server.id))

    def init_repl_func():
        replica_cluster.initialize_replicaset(suggested_primary_server)
    prompt_execute_task(prompt, init_repl_func)


###############################################################################
def prompt_add_member_to_replica(replica_cluster, server):

    prompt = ("Do you want to add server '%s' to replica set cluster '%s'?" %
              (server.id, replica_cluster.id))

    def add_member_func():
        replica_cluster.add_member_to_replica(server)
    prompt_execute_task(prompt, add_member_func)

###############################################################################
def _start_server_process_4real(server, options_override=None):
    server_dir_exists = mk_server_dir(server)
    first_time = not server_dir_exists

    # generate key file if needed
    if server.needs_repl_key():
        get_generate_key_file(server)

    # create the start command line
    start_cmd = generate_start_command(server, options_override)

    start_cmd_str = " ".join(start_cmd)
    first_time_msg = " for the first time" if first_time else ""

    log_info("Starting server '%s'%s..." % (server.id, first_time_msg))
    log_info("\nExecuting command:\n%s\n" % start_cmd_str)

    child_process_out = None
    if server.is_fork():
        child_process_out = subprocess.PIPE

    global __mongod_pid__
    global __current_server__

    parent_mongod = create_subprocess(start_cmd,
                                      stdout=child_process_out,
                                      preexec_fn=server_process_preexec)



    if server.is_fork():
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
             server.id)
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
        raise MongoctlException("Timed out waiting for server '%s' to start. "
                                "Please tail the log file to monitor further "
                                "progress." %
                                server.id)

    log_info("Server '%s' started successfully! (pid=%s)\n" %
             (server.id, mongod_pid))

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

    log_info("Setting OS limit on %s for process (desire up to %d)..."
             "\n\t Current limit values: soft = %d, hard = %d" %
             (description, desired_limit, soft, hard))

    _negotiate_process_limit(set_resource, desired_limit, soft, hard)
    log_info("Resulting OS limit on %s for process: " % description +
             "soft = %d, hard = %d" % resource.getrlimit(which_resource))

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
        command = apply_numactl(command)

    # append the mongod executable
    command.append(get_server_executable(server))

    # create the command args
    cmd_options = server.export_cmd_options(options_override=options_override)

    command.extend(options_to_command_args(cmd_options))
    return command


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
        return [numactl_exe, "--interleave=all"] + command
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
def mk_server_dir(server):
    # ensure the dbpath dir exists
    server_dir = server.get_root_path()
    log_verbose("Ensuring that server's root path '%s' exists..." % server_dir)
    if ensure_dir(server.get_root_path()):
        log_verbose("server root path %s already exists!" % server_dir)
        return True
    else:
        log_verbose("server root path '%s' created successfully" % server_dir)
        return False


###############################################################################
def get_generate_key_file(server):
    cluster = server.get_cluster()
    key_file_path = server.get_key_file() or server.get_default_key_file_path()

    # Generate the key file if it does not exist
    if not os.path.exists(key_file_path):
        key_file = open(key_file_path, 'w')
        key_file.write(cluster.get_repl_key())
        key_file.close()
        # set the permissions required by mongod
        os.chmod(key_file_path,stat.S_IRUSR)
    return key_file_path

###############################################################################
def get_server_executable(server):
    if isinstance(server, MongodServer):
        return get_mongod_executable(server)
    elif isinstance(server, MongosServer):
        return get_mongos_executable(server)

###############################################################################
def get_mongod_executable(server):
    mongod_exe = get_mongo_executable(server.get_mongo_version(),
                                      'mongod',
                                      version_check_pref=VERSION_PREF_EXACT)
    return mongod_exe.path

###############################################################################
def get_mongos_executable(server):
    mongos_exe = get_mongo_executable(server.get_mongo_version(),
                                      'mongos',
                                      version_check_pref=VERSION_PREF_EXACT)
    return mongos_exe.path

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
    "noMoveParanoia",
    "setParameter"
]


###############################################################################
SUPPORTED_MONGOS_OPTIONS = [
    "verbose",
    "quiet",
    "port",
    "bind_ip",
    "maxConns",
    "logpath",
    "logappend",
    "pidfilepath",
    "keyFile",
    "nounixsocket",
    "unixSocketPrefix",
    "ipv6",
    "jsonp",
    "nohttpinterface",
    "upgrade",
    "setParameter",
    "syslog",
    "configdb",
    "localThreshold",
    "test",
    "chunkSize",
    "noscripting"
]
