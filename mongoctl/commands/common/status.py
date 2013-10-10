__author__ = 'abdul'

import mongoctl.repository as repository

from mongoctl.mongoctl_logging import *
from mongoctl.errors import MongoctlException
from mongoctl.utils import document_pretty_string

###############################################################################
# status command TODO: parsed?
###############################################################################
def status_command(parsed_options):
    # we need to print status json to stdout so that its seperate from all
    # other messages that are printed on stderr. This is so scripts can read
    # status json and parse it if it needs

    id = parsed_options.id
    server = repository.lookup_server(id)
    if server:
        log_info("Status for server '%s':" % id)
        status = server.get_status(admin=True)
    else:
        cluster = repository.lookup_cluster(id)
        if cluster:
            log_info("Status for cluster '%s':" % id)
            status = cluster.get_status()
        else:
            raise MongoctlException("Cannot find a server or a cluster with"
                                    " id '%s'" % id)

    status_str = document_pretty_string(status)
    stdout_log(status_str)
    return status
