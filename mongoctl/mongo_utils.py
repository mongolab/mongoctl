__author__ = 'abdul'

import pymongo
import pymongo.uri_parser
import pymongo.errors
import socket
import mongoctl_logging
from datetime import datetime
import utils
import traceback
###############################################################################
# db connection timeout, 10 seconds
CONN_TIMEOUT_MS = 10000

###############################################################################
def mongo_client(*args, **kwargs):
    """
    wrapper around mongo client
    :param args:
    :param kwargs:
    :return:
    """

    kwargs = kwargs or {}
    connection_timeout_ms = kwargs.get("connectTimeoutMS") or CONN_TIMEOUT_MS

    kwargs.update({
        "socketTimeoutMS": connection_timeout_ms,
        "connectTimeoutMS": connection_timeout_ms,
        "maxPoolSize": 1
    })
    if pymongo.get_version_string().startswith("3.2"):
        fail_fast_if_connection_refused(*args, **kwargs)
        if kwargs and kwargs.get("serverSelectionTimeoutMS") is None:
            kwargs["connect"] = True
            kwargs["serverSelectionTimeoutMS"] = connection_timeout_ms

    mongoctl_logging.log_debug("(BEGIN) create MongoClient %s" % args[0])

    client = pymongo.MongoClient(*args, **kwargs)

    mongoctl_logging.log_debug("(END) create MongoClient %s" % args[0])

    ping(client)

    return client

###############################################################################
def ping(mongo_client):

    mongoctl_logging.log_debug("(BEGIN) ping %s:%s" % (mongo_client.address[0], mongo_client.address[1]))
    start_date = datetime.now()
    result = mongo_client.get_database("admin").command({"ping": 1})
    duration = utils.timedelta_total_seconds(datetime.now() - start_date)
    mongoctl_logging.log_debug("(END) ping %s:%s (finished in %s seconds)" % (mongo_client.address[0],
                                                                              mongo_client.address[1], duration))

    # DEBUGGING
    if duration > 1:
        mongoctl_logging.log_debug("**** Ping took more than 1 second. STACK:\n %s\n " % "\n".join(traceback.format_stack()))
    return result

###############################################################################
def fail_fast_if_connection_refused(*args, **kwargs):
    try:
        # parse uri
        uri = args[0]
        if uri.startswith("mongodb://"):
            address, port = pymongo.uri_parser.parse_uri(uri)["nodelist"][0]
        else: # assume its address:port
            address, port = uri.split(":")
            port = int(port)

        mongoctl_logging.log_debug("fail_fast_if_connection_refused for %s:%s" % (address, port))
        s = socket.create_connection((address, port), CONN_TIMEOUT_MS/1000)
        s.close()
        mongoctl_logging.log_debug("PASSED fail_fast_if_connection_refused !")
    except Exception, ex:
        mongoctl_logging.log_debug("FINISHED fail_fast_if_connection_refused: %s" % ex)
        if "refused" in str(ex):
            raise pymongo.errors.ConnectionFailure(str(ex))
        else:
            pass


