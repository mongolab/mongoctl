__author__ = 'abdul'

from pymongo import uri_parser, errors

###############################################################################
# mongo uri tool. Contains utility functions for dealing with mongo uris
###############################################################################

###############################################################################
# MongoUriWrapper
###############################################################################
class MongoUriWrapper:
    """
     A Mongo URI wrapper that makes it easy to deal with mongo uris:
     - Masks user/password on display (i.e. __str__()
    """
    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, uri_obj):
        self._uri_obj = uri_obj

    ###########################################################################
    @property
    def raw_uri(self):
        return self._get_uri(mask=False)

    ###########################################################################
    @property
    def member_raw_uri_list(self):
        return self._get_member_uri_list(mask=False)

    ###########################################################################
    @property
    def masked_uri(self):
        return self._get_uri(mask=True)

    ###########################################################################
    @property
    def member_masked_uri_list(self):
        return self._get_member_uri_list(mask=True)

    ###########################################################################
    @property
    def database(self):
        return self._uri_obj["database"]


    @database.setter
    def database(self, value):
        self._uri_obj["database"] = value

    ###########################################################################
    @property
    def node_list(self):
        return self._uri_obj["nodelist"]

    ###########################################################################
    @property
    def address(self):
        return self.addresses[0]

    ###########################################################################
    @property
    def addresses(self):
        addresses = []
        for node in self.node_list:
            address = "%s:%s" % (node[0], node[1])
            addresses.append(address)

        return addresses

    ###########################################################################
    @property
    def username(self):
        return self._uri_obj["username"]

    ###########################################################################
    @property
    def password(self):
        return self._uri_obj["password"]

    ###########################################################################
    def is_cluster_uri(self):
        return len(self.node_list) > 1

    ###########################################################################
    def __str__(self):
        return self.masked_uri

    ###########################################################################
    def _get_uri(self, mask=False):
        # build db string
        db_str = "/%s" % self.database if self.database else ""

        # build credentials string
        if self.username:
            if mask:
                creds = "*****:*****@"
            else:
                creds = "%s:%s@" % (self.username, self.password)
        else:
            creds = ""

        # build hosts string
        address_str = ",".join(self.addresses)
        return "mongodb://%s%s%s" % (creds, address_str, db_str)

    ###########################################################################
    def _get_member_uri_list(self, mask=False):
        # build db string
        db_str = "%s" % self.database if self.database else ""
        username = self.username
        password = "****" if mask else self.password

        # build credentials string
        if username:
            creds = "%s:%s@" % (username, password)
        else:
            creds = ""

        # build hosts string
        member_uris = []
        for node in self.node_list:
            address = "%s:%s" % (node[0], node[1])
            mem_uri = "mongodb://%s%s/%s" % (creds, address, db_str)
            member_uris.append(mem_uri)

        return member_uris

###############################################################################
def parse_mongo_uri(uri):
    try:
        uri_obj = uri_parser.parse_uri(uri)
        # validate uri
        nodes = uri_obj["nodelist"]
        for node in nodes:
            host = node[0]
            if not host:
                raise Exception("URI '%s' is missing a host." % uri)

        return MongoUriWrapper(uri_obj)
    except errors.ConfigurationError, e:
        raise Exception("Malformed URI '%s'. %s" % (uri, e))

    except Exception, e:
        raise Exception("Unable to parse mongo uri '%s'."
                        " Cause: %s" % (e, uri))

###############################################################################
def mask_mongo_uri(uri):
    uri_wrapper = parse_mongo_uri(uri)
    return uri_wrapper.masked_uri

###############################################################################
def is_mongo_uri(value):
    try:
        parse_mongo_uri(value)
        return True
    except Exception,e:
        return False

###############################################################################
def is_cluster_mongo_uri(mongo_uri):
    """
        Returns true if the specified mongo uri is a cluster connection
    """
    return len(parse_mongo_uri(mongo_uri).node_list) > 1


