__author__ = 'abdul'
import sys
import utils
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
    log_info( "Executing db command %s" % utils.document_pretty_string(cmd))
