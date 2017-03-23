__author__ = 'abdul'

import pymongo
import pymongo.uri_parser
import pymongo.errors

import mongoctl_logging

from pymo import mongo_client as _mongo_client
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
        if kwargs and kwargs.get("serverSelectionTimeoutMS") is None:
            kwargs["connect"] = True
            kwargs["serverSelectionTimeoutMS"] = connection_timeout_ms

    return _mongo_client(*args, **kwargs)




