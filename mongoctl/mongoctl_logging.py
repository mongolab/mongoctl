__author__ = 'abdul'

import sys
import os
import traceback

import logging

import utils
import mongoctl_globals

from logging.handlers import TimedRotatingFileHandler

###############################################################################
LOG_DIR = "logs"

logger = None

# logger settings
_log_to_stdout = True
_logging_level = logging.INFO

VERBOSE = 15
logging.addLevelName(VERBOSE, "VERBOSE")

###############################################################################
def get_logger():
    global logger, _logging_level

    if logger:
        return logger

    logger = logging.getLogger("MongoctlLogger")

    log_file_name="mongoctl.log"
    conf_dir = mongoctl_globals.DEFAULT_CONF_ROOT
    log_dir = utils.resolve_path(os.path.join(conf_dir, LOG_DIR))
    utils.ensure_dir(log_dir)


    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)8s | %(asctime)s | %(message)s")
    logfile = os.path.join(log_dir, log_file_name)
    fh = TimedRotatingFileHandler(logfile, backupCount=50, when="midnight")

    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    # add the handler to the root logger
    logging.getLogger().addHandler(fh)

    global _log_to_stdout
    if _log_to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        std_formatter = logging.Formatter("%(message)s")
        sh.setFormatter(std_formatter)
        sh.setLevel(_logging_level)
        logging.getLogger().addHandler(sh)

    return logger

###############################################################################
def setup_logging(log_level=logging.INFO, log_to_stdout=True):
    global _log_to_stdout, _logging_level

    _log_to_stdout = log_to_stdout
    _logging_level = log_level

###############################################################################
def turn_logging_verbose_on():
    global _logging_level
    _logging_level = VERBOSE

###############################################################################
def log_info(msg):
    get_logger().info(msg)

###############################################################################
def log_error(msg):
    get_logger().error(msg)

###############################################################################
def log_warning(msg):
    get_logger().warning(msg)

###############################################################################
def log_verbose(msg):
    get_logger().log(VERBOSE, msg)

###############################################################################
def log_debug(msg):
    get_logger().debug(msg)

###############################################################################
def log_exception(exception):
    log_debug("EXCEPTION: %s" % exception)
    log_debug(traceback.format_exc())

###############################################################################
def stdout_log(msg):
    print msg

###############################################################################
def log_db_command(cmd):
    log_info( "Executing db command %s" % utils.document_pretty_string(cmd))
