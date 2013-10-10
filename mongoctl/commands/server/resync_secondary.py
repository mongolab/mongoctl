__author__ = 'abdul'

import mongoctl.repository as repository
import shutil

from mongoctl.mongoctl_logging import log_info
from mongoctl.errors import MongoctlException
from stop import do_stop_server
from start import do_start_server

###############################################################################
# re-sync secondary command
###############################################################################
def resync_secondary_command(parsed_options):
    resync_secondary(parsed_options.server)

###############################################################################
def resync_secondary(server_id):

    server = repository.lookup_and_validate_server(server_id)
    server.validate_local_op("resync-secondary")

    log_info("Checking if server '%s' is secondary..." % server_id)
    # get the server status
    status = server.get_status(admin=True)
    if not status['connection']:
        msg = ("Server '%s' does not seem to be running. For more details,"
               " run 'mongoctl status %s'" % (server_id, server_id))
        raise MongoctlException(msg)
    elif 'error' in status:
        msg = ("There was an error while connecting to server '%s' (error:%s)."
               " For more details, run 'mongoctl status %s'" %
               (server_id, status['error'], server_id))
        raise MongoctlException(msg)

    rs_state = None
    if 'selfReplicaSetStatusSummary' in status:
        rs_state = status['selfReplicaSetStatusSummary']['stateStr']

    if rs_state not in ['SECONDARY', 'RECOVERING']:
        msg = ("Server '%s' is not a secondary member or cannot be determined"
               " as secondary (stateStr='%s'. For more details, run 'mongoctl"
               " status %s'" % (server_id, rs_state, server_id))
        raise MongoctlException(msg)

    do_stop_server(server)

    log_info("Deleting server's '%s' dbpath '%s'..." %
             (server_id, server.get_db_path()))

    shutil.rmtree(server.get_db_path())

    do_start_server(server)
