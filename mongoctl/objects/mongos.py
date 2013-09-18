__author__ = 'abdul'


import mongoctl.repository as repository
from server import Server

from mongoctl.utils import resolve_path
from mongoctl.mongoctl_logging import log_verbose

from bson.son import SON
from mongoctl.errors import MongoctlException
from cluster import get_member_repl_lag

###############################################################################
# CONSTANTS
###############################################################################


###############################################################################
# MongosServer Class
###############################################################################

class MongosServer(Server):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        super(MongosServer, self).__init__(server_doc)

    ###########################################################################
    def get_cluster(self):
        return repository.lookup_cluster_by_server(self)


    ###########################################################################
    def export_cmd_options(self, options_override=None):
        """
            Override!
        :return:
        """
        cmd_options = super(MongosServer, self).export_cmd_options(
            options_override=options_override)

        # Add configServers arg
        cluster = self.get_cluster()
        config_addresses = ",".join(cluster.get_config_member_addresses())
        cmd_options["configdb"] = config_addresses

        return cmd_options
    ###########################################################################
    # Properties
    ###########################################################################
    def needs_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        return (self.supports_repl_key() and
                self.get_cluster().get_repl_key() is not None)