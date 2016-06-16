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

    host, port = parse_client_host_port(*args)
    if pymongo.get_version_string().startswith("3.2"):
        fail_fast_if_connection_refused(host, port)
        if kwargs and kwargs.get("serverSelectionTimeoutMS") is None:
            kwargs["connect"] = True
            kwargs["serverSelectionTimeoutMS"] = connection_timeout_ms

    mongoctl_logging.log_debug("(BEGIN) create MongoClient %s" % args[0])

    client = pymongo.MongoClient(*args, **kwargs)

    mongoctl_logging.log_debug("(END) create MongoClient %s" % args[0])

    test_client(client, host, port)

    return client

###############################################################################
def test_client(mongo_client, host, port):
    start_date = datetime.now()
    try:
        mongoctl_logging.log_debug("(BEGIN) test_client ping %s:%s" % (host, port))

        result = mongo_client.get_database("admin").command({"ping": 1})

        return result
    except Exception, ex:
        mongoctl_logging.log_debug("(ERROR) test_client ping %s:%s: %s" % (host, port, ex))
        mongoctl_logging.log_exception(ex)
    finally:
        duration = utils.timedelta_total_seconds(datetime.now() - start_date)

        mongoctl_logging.log_debug("(END) test_client ping %s:%s (finished in %s seconds)" % (host, port, duration))

        # DEBUGGING
        if duration > 1:
            mongoctl_logging.log_debug("**** Ping took more than 1 second. "
                                       "STACK:\n %s\n " % "\n".join(traceback.format_stack()))


###############################################################################
def fail_fast_if_connection_refused(host, port):
    try:

        mongoctl_logging.log_debug("fail_fast_if_connection_refused for %s:%s" % (host, port))
        s = socket.create_connection((host, port), CONN_TIMEOUT_MS/1000)
        s.close()
        mongoctl_logging.log_debug("PASSED fail_fast_if_connection_refused !")
    except Exception, ex:
        mongoctl_logging.log_debug("FINISHED fail_fast_if_connection_refused: %s" % ex)
        if "refused" in str(ex):
            raise pymongo.errors.ConnectionFailure(str(ex))
        else:
            pass

###############################################################################

def parse_client_host_port(*args):
    uri = args[0]
    if uri.startswith("mongodb://"):
        host, port = pymongo.uri_parser.parse_uri(uri)["nodelist"][0]
    else: # assume its address:port
        host, port = uri.split(":")
        port = int(port)

    return host, port
