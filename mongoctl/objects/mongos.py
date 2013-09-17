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
    # Properties
    ###########################################################################
