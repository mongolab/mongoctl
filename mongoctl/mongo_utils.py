__author__ = 'abdul'

import pymongo
import pymongo.uri_parser
import socket

###############################################################################
# db connection timeout, 10 seconds
CONN_TIMEOUT = 10000

###############################################################################
def mongo_client(*args, **kwargs):
    """
    wrapper around mongo client
    :param args:
    :param kwargs:
    :return:
    """
    kwargs = kwargs or {}
    kwargs.update({
        "socketTimeoutMS": CONN_TIMEOUT,
        "connectTimeoutMS": CONN_TIMEOUT,
        "maxPoolSize": 1
    })
    if pymongo.get_version_string().startswith("3.2"):
        # parse uri
        uri = args[0]
        address, port = pymongo.uri_parser.parse_uri(uri)["nodelist"][0]
        fail_fast_if_connection_refused(address, port)
        if kwargs and kwargs.get("serverSelectionTimeoutMS") is None:
            kwargs["connect"] = True
            kwargs["serverSelectionTimeoutMS"] = CONN_TIMEOUT

    client = pymongo.MongoClient(*args, **kwargs)
    ping(client)
    return client

###############################################################################
def ping(mongo_client):
    return mongo_client.get_database("admin").command({"ping": 1})

###############################################################################
def fail_fast_if_connection_refused(address, port):
    try:

        socket.create_connection((address, port), CONN_TIMEOUT)
    except Exception, ex:
        if "refused" in str(ex):
            raise


