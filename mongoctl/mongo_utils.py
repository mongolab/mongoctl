__author__ = 'abdul'

import pymongo

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
        fail_fast_if_connection_refused(*args)
        if kwargs and kwargs.get("serverSelectionTimeoutMS") is None:
            kwargs["connect"] = True
            kwargs["serverSelectionTimeoutMS"] = 1

    client = pymongo.MongoClient(*args, **kwargs)
    ping(client)
    return client

###############################################################################
def ping(mongo_client):
    return mongo_client.get_database("admin").command({"ping": 1})

###############################################################################
def fail_fast_if_connection_refused(*args):
    try:
        c = pymongo.MongoClient(*args, connect=True, connectTimeoutMS=1,
                                maxPoolSize=1, socketTimeoutMS=1,
                                serverSelectionTimeoutMS=1)
        ping(c)
    except Exception, ex:
        if "refused" in str(ex):
            raise


