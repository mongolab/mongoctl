__author__ = 'abdul'

import mongoctl.repository as repository

from mongoctl.errors import MongoctlException
from mongoctl.mongoctl_logging import log_info

###############################################################################
# show cluster command
###############################################################################
def show_cluster_command(parsed_options):
    cluster = repository.lookup_cluster(parsed_options.cluster)
    if cluster is None:
        raise MongoctlException("Could not find cluster '%s'." %
                                parsed_options.cluster)
    log_info("Configuration for cluster '%s':" % parsed_options.cluster)
    print cluster


