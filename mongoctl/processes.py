__author__ = 'abdul'

import subprocess
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
def get_child_processes():
    global __child_subprocesses__
    return __child_subprocesses__