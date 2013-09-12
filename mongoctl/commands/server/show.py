__author__ = 'abdul'

import mongoctl.repository as repository
from mongoctl.mongoctl_logging import log_info

from mongoctl.errors import MongoctlException

###############################################################################
# show server command
###############################################################################
def show_server_command(parsed_options):
    server = repository.lookup_server(parsed_options.server)
    if server is None:
        raise MongoctlException("Could not find server '%s'." %
                                parsed_options.server)
    log_info("Configuration for server '%s':" % parsed_options.server)
    print server

