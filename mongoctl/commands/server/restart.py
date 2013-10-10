__author__ = 'abdul'

import mongoctl.repository as repository

from mongoctl.mongoctl_logging import log_info
from start import extract_server_options, do_start_server
from stop import do_stop_server

###############################################################################
# restart command
###############################################################################
def restart_command(parsed_options):
    server_id = parsed_options.server
    server = repository.lookup_and_validate_server(server_id)

    options_override = extract_server_options(server, parsed_options)

    restart_server(parsed_options.server, options_override)


###############################################################################
# restart server
###############################################################################
def restart_server(server_id, options_override=None):
    server = repository.lookup_and_validate_server(server_id)
    do_restart_server(server, options_override)

###############################################################################
def do_restart_server(server, options_override=None):
    log_info("Restarting server '%s'..." % server.id)

    if server.is_online():
        do_stop_server(server)
    else:
        log_info("Server '%s' is not running." % server.id)

    do_start_server(server, options_override)
