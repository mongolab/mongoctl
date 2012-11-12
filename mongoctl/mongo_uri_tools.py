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
        return self._get_uri()
    ###########################################################################
    @property
    def masked_uri(self):
        return self._get_uri(mask=True)

    ###########################################################################
    @property
    def database(self):
        return self._uri_obj["database"]

    ###########################################################################
    @property
    def node_list(self):
        return self._uri_obj["nodelist"]

    ###########################################################################
    @property
    def username(self):
        return self._uri_obj["username"]

    ###########################################################################
    @property
    def password(self):
        return self._uri_obj["password"]

    ###########################################################################
    def __str__(self):
        return self.masked_uri

    ###########################################################################
    def _get_uri(self, mask=False):
        # build db string
        db = self.database or ""
        username = self.username
        password = "****" if mask else self.password

        # build credentials string
        if username:
            creds = "%s:%s@" % (username, password)
        else:
            creds = ""

        # build hosts string
        addresses = []
        for node in self.node_list:
            address = "%s:%s" % (node[0], node[1])
            addresses.append(address)

        address_str = ",".join(addresses)
        return "mongodb://%s%s/%s" % (creds, address_str, db)

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