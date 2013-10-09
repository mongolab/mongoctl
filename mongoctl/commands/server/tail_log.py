__author__ = 'abdul'

import os

from mongoctl.processes import create_subprocess
from mongoctl.mongoctl_logging import *
from mongoctl import repository
from mongoctl.utils import execute_command

###############################################################################
# tail log command
###############################################################################
def tail_log_command(parsed_options):
    server = repository.lookup_server(parsed_options.server)
    server.validate_local_op("tail-log")
    log_path = server.get_log_file_path()
    # check if log file exists
    if os.path.exists(log_path):
        log_tailer = tail_server_log(server)
        log_tailer.communicate()
    else:
        log_info("Log file '%s' does not exist." % log_path)


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
    except Exception, e:
        log_exception(e)
        log_error("Unable to tail server log file. Cause: %s" % e)
        return None

###############################################################################
def stop_tailing(log_tailer):
    try:
        if log_tailer:
            log_verbose("-- Killing tail log path subprocess")
            log_tailer.terminate()
    except Exception, e:
        log_exception(e)
        log_verbose("Failed to kill tail subprocess. Cause: %s" % e)
