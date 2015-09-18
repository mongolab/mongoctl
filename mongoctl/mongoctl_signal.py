__author__ = 'abdul'

import signal

from mongoctl_logging import *
from utils import kill_current_process_child_processes
from processes import get_child_processes

###############################################################################
# SIGNAL HANDLER FUNCTIONS
###############################################################################
#TODO Remove this ugly signal handler and use something more elegant

__registered_signal_handlers__ = []

###############################################################################
def register_mongoctl_signal_handler(handler):
    global __registered_signal_handlers__
    __registered_signal_handlers__.append(handler)

###############################################################################
def mongoctl_global_signal_handler(signal_val, frame):
    print("**** RECEIVED SIGNAL %s *****" % signal_val)
    global __registered_signal_handlers__
    for handler in __registered_signal_handlers__:
        try:
            handler()
        except Exception, ex:
            traceback.format_exc()

    exit_mongoctl()

###############################################################################
def kill_child(child_process):
    try:
        if child_process.poll() is None:
            log_verbose("Killing child process '%s'" % child_process )
            child_process.terminate()
    except Exception, e:
        log_exception(e)
        log_verbose("Unable to kill child process '%s': Cause: %s" %
                    (child_process, e))

###############################################################################
def exit_mongoctl():
    # kill all children then exit
    kill_current_process_child_processes()
    exit(0)

###############################################################################
def init_mongoctl_signal_handler():
    # IMPORTANT: this must be called in order for signals to be handled
    # Register the global mongoctl signal handler
    signal.signal(signal.SIGINT, mongoctl_global_signal_handler)