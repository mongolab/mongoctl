__author__ = 'abdul'

import mongoctl.repository as repository
from mongoctl.mongoctl_logging import log_info
from mongoctl.utils import to_string
###############################################################################
# list servers command
###############################################################################
def list_servers_command(parsed_options):
    servers = repository.lookup_all_servers()
    if not servers or len(servers) < 1:
        log_info("No servers have been configured.")
        return

    servers = sorted(servers, key=lambda s: s.id)
    bar = "-"*80
    print bar
    formatter = "%-25s %-40s %s"
    print formatter % ("_ID", "DESCRIPTION", "CONNECT TO")
    print bar


    for server in servers:
        print formatter % (server.id,
                           to_string(server.get_description()),
                           to_string(server.get_address_display()))
    print "\n"


