__author__ = 'abdul'

import subprocess
import signal

from mongoctl_logging import *
from prompt import prompt_execute_task
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
        prompt_execute_task("Kill server '%s'?" % __current_server__.id,
                            exit_mongoctl)

###############################################################################
# Register the global mongoctl signal handler
signal.signal(signal.SIGINT, mongoctl_signal_handler)
