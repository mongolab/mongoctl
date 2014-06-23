__author__ = 'abdul'

from server import Server

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
    def export_cmd_options(self, options_override=None):
        """
            Override!
        :return:
        """
        cmd_options = super(MongosServer, self).export_cmd_options(
            options_override=options_override)

        # Add configServers arg
        cluster = self.get_validate_cluster()
        config_addresses = ",".join(cluster.get_config_member_addresses())
        cmd_options["configdb"] = config_addresses

        return cmd_options

    ###########################################################################
    def is_cluster_connection_member(self):
        return True