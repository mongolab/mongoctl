__author__ = 'abdul'

from bson.son import SON
from mongoctl.utils import (
    document_pretty_string, wait_for, kill_process, is_pid_alive
)
from mongoctl.mongoctl_logging import *
from mongoctl.errors import MongoctlException

import mongoctl.repository
import mongoctl.objects.server

from start import server_stopped_predicate
from mongoctl.prompt import prompt_execute_task

###############################################################################
# Constants
###############################################################################

MAX_SHUTDOWN_WAIT = 45

###############################################################################
# stop command
###############################################################################
def stop_command(parsed_options):
    stop_server(parsed_options.server, force=parsed_options.forceStop)



###############################################################################
# stop server
###############################################################################
def stop_server(server_id, force=False):
    do_stop_server(mongoctl.repository.lookup_and_validate_server(server_id),
                   force)

###############################################################################
def do_stop_server(server, force=False):
    # ensure that the stop was issued locally. Fail otherwise
    server.validate_local_op("stop")

    log_info("Checking to see if server '%s' is actually running before"
             " stopping it..." % server.id)

    # init local flags
    can_stop_mongoly = True
    shutdown_success = False

    status = server.get_status()
    if not status['connection']:
        if "timedOut" in status:
            log_info("Unable to issue 'shutdown' command to server '%s'. "
                     "The server is not responding (connection timed out) "
                     "although port %s is open, possibly for mongod." %
                     (server.id, server.get_port()))
            can_stop_mongoly = False
        elif "error" in status and "SSL handshake failed" in status["error"]:
            log_info("Unable to issue 'shutdown' command to server '%s'. "
                     "The server appears to be configured with SSL but is not "
                     "currently running with SSL (SSL handshake failed). "
                     "Try running mongoctl with --ssl-off." % server.id)
            can_stop_mongoly = False
        else:
            log_info("Server '%s' is not running." %
                     server.id)
            return

    pid = server.get_pid()
    pid_disp = pid if pid else "[Cannot be determined]"
    log_info("Stopping server '%s' (pid=%s)..." %
             (server.id, pid_disp))
    # log server activity stop
    server.log_server_activity("stop")
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
        log_info("Server '%s' has stopped." % server.id)
    else:
        raise MongoctlException("Unable to stop server '%s'." %
                                server.id)

###############################################################################
def step_down_if_needed(server, force):
    ## if server is a primary replica member then step down
    if server.is_primary():
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

        log_info("Will now wait for server '%s' to stop." % server.id)
        # Check that the server has stopped
        stop_pred = server_stopped_predicate(server, pid)
        wait_for(stop_pred,timeout=MAX_SHUTDOWN_WAIT)

        if not stop_pred():
            log_error("Shutdown command failed...")
            return False
        else:
            return True
    except Exception, e:
        log_exception(e)
        log_error("Failed to gracefully stop server '%s'. Cause: %s" %
                  (server.id, e))
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

    log_info("Forcibly stopping server '%s'...\n" % server.id)
    log_info("Sending kill -1 (HUP) signal to server '%s' (pid=%s)..." %
             (server.id, pid))

    kill_process(pid, force=False)

    log_info("Will now wait for server '%s' (pid=%s) to die." %
             (server.id, pid))
    wait_for(pid_dead_predicate(pid), timeout=MAX_SHUTDOWN_WAIT)

    if is_pid_alive(pid):
        log_error("Failed to kill server process with -1 (HUP).")
        log_info("Sending kill -9 (SIGKILL) signal to server"
                 "'%s' (pid=%s)..." % (server.id, pid))
        kill_process(pid, force=True)

        log_info("Will now wait for server '%s' (pid=%s) to die." %
                 (server.id, pid))
        wait_for(pid_dead_predicate(pid), timeout=MAX_SHUTDOWN_WAIT)

    if not is_pid_alive(pid):
        log_info("Forcefully-stopped server '%s'." % server.id)
        return True
    else:
        log_error("Forceful stop of server '%s' failed." % server.id)
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
    log_info("Stepping down server '%s'..." % server.id)

    try:
        cmd = SON( [('replSetStepDown', 10),('force', force)])
        server.disconnecting_db_command(cmd, "admin")
        log_info("Server '%s' stepped down successfully!" % server.id)
        return True
    except Exception, e:
        log_exception(e)
        log_error("Failed to step down server '%s'. Cause: %s" %
                  (server.id, e))
        return False

###############################################################################
def prompt_step_server_down(server, force):
    def step_down_func():
        step_server_down(server, force)

    return prompt_execute_task("Server '%s' is a primary server. "
                               "Step it down before proceeding to shutdown?" %
                               server.id,
                               step_down_func)

###############################################################################
def pid_dead_predicate(pid):
    def pid_dead():
        return not is_pid_alive(pid)

    return pid_dead
