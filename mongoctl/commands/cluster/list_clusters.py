__author__ = 'abdul'

import mongoctl.repository as repository

from mongoctl.mongoctl_logging import log_info
from mongoctl.utils import to_string
###############################################################################
# list clusters command
###############################################################################
def list_clusters_command(parsed_options):
    clusters = repository.lookup_all_clusters()
    if not clusters or len(clusters) < 1:
        log_info("No clusters configured")
        return

    # sort clusters by id
    clusters = sorted(clusters, key=lambda c: c.id)
    bar = "-"*80
    print bar
    formatter = "%-25s %-40s %s"
    print formatter % ("_ID", "DESCRIPTION", "MEMBERS")
    print bar

    for cluster in clusters:
        desc = to_string(cluster.get_description())

        members_info = "[ %s ]" % ", ".join(cluster.get_members_info())

        print formatter % (cluster.id, desc, members_info)
    print "\n"

