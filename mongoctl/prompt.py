__author__ = 'abdul'

import sys
import getpass

from errors import MongoctlException
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
def read_input(message):
    # If we are running in a noninteractive mode then fail
    if not is_interactive_mode():
        msg = ("Error while trying to prompt you for '%s'. Prompting is not "
               "allowed when running with --noninteractive mode. Please pass"
               " enough arguments to bypass prompting or run without "
               "--noninteractive" % message)
        raise MongoctlException(msg)

    print >> sys.stderr, message,
    return raw_input()

###############################################################################
def read_username(dbname):
    # If we are running in a noninteractive mode then fail
    if not is_interactive_mode():
        msg = ("mongoctl needs username in order to proceed. Please pass the"
               " username using the -u option or run without --noninteractive")
        raise MongoctlException(msg)

    return read_input("Enter username for database '%s': " % dbname)

###############################################################################
def read_password(message=''):
    if not is_interactive_mode():
        msg = ("mongoctl needs password in order to proceed. Please pass the"
               " password using the -p option or run without --noninteractive")
        raise MongoctlException(msg)

    print >> sys.stderr, message
    return getpass.getpass()


###############################################################################
def prompt_execute_task(message, task_function):

    yes = prompt_confirm(message)
    if yes:
        return (True,task_function())
    else:
        return (False,None)

###############################################################################
def prompt_confirm(message):

    # return False if noninteractive or --no was specified
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
