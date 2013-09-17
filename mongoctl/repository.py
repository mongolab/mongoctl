

__author__ = 'abdul'

import pymongo
import config

import objects.cluster

from errors import MongoctlException
from mongoctl_logging import log_warning, log_verbose, log_info
from mongo_uri_tools import parse_mongo_uri
from utils import (
    resolve_class, document_pretty_string, is_valid_member_address
    )

from mongo_version import is_supported_mongo_version, is_valid_version
from mongo_uri_tools import is_cluster_mongo_uri, mask_mongo_uri

DEFAULT_SERVERS_FILE = "servers.config"

DEFAULT_CLUSTERS_FILE = "clusters.config"

DEFAULT_SERVERS_COLLECTION = "servers"

DEFAULT_CLUSTERS_COLLECTION = "clusters"

DEFAULT_ACTIVITY_COLLECTION = "logs.server-activity"

###############################################################################
# Global variable: mongoctl's mongodb object
__mongoctl_db__ = None

###############################################################################
def get_mongoctl_database():

    # if not using db then return
    if not has_db_repository():
        return

    global __mongoctl_db__

    if __mongoctl_db__ is not None:
        return __mongoctl_db__

    log_verbose("Connecting to mongoctl db...")
    try:

        (conn, dbname) = _db_repo_connect()

        __mongoctl_db__ = conn[dbname]
        return __mongoctl_db__
    except Exception, e:
        __mongoctl_db__ = "OFFLINE"
        log_warning("\n*************\n"
                    "Will not be using database repository for configurations"
                    " at this time!"
                    "\nREASON: Could not establish a database"
                    " connection to mongoctl's database repository."
                    "\nCAUSE: %s."
                    "\n*************" % e)

###############################################################################
def has_db_repository():
    return config.get_database_repository_conf() is not None

###############################################################################
def has_file_repository():
    return config.get_file_repository_conf() is not None

###############################################################################
def consulting_db_repository():
    return has_db_repository() and is_db_repository_online()

###############################################################################
def is_db_repository_online():
    mongoctl_db = get_mongoctl_database()
    return mongoctl_db and mongoctl_db != "OFFLINE"

###############################################################################
def _db_repo_connect():
    db_conf = config.get_database_repository_conf()
    uri = db_conf["databaseURI"]
    conn = pymongo.Connection(uri)
    dbname = parse_mongo_uri(uri).database
    return conn, dbname


###############################################################################
def validate_repositories():
    if ((not has_file_repository()) and
            (not has_db_repository())):
        raise MongoctlException("Invalid 'mongoctl.config': No fileRepository"
                                " or databaseRepository configured. At least"
                                " one repository has to be configured.")

###############################################################################
# Server lookup functions
###############################################################################
def lookup_server(server_id):
    validate_repositories()

    server = None
    # lookup server from the db repo first
    if consulting_db_repository():
        server = db_lookup_server(server_id)

    # if server is not found then try from file repo
    if server is None and has_file_repository():
        server = config_lookup_server(server_id)


    return server

###############################################################################
def lookup_and_validate_server(server_id):
    server = lookup_server(server_id)
    if server is None:
        raise MongoctlException("Cannot find configuration for a server "
                                "with _id of '%s'." % server_id)

    validation_errors = validate_server(server)
    if len(validation_errors) > 0:
        raise MongoctlException(
            "Server '%s' configuration is not valid. Please fix errors below"
            " and try again.\n%s" % (server_id,"\n".join(validation_errors)))

    return server

###############################################################################
def db_lookup_server(server_id):
    server_collection = get_mongoctl_server_db_collection()
    server_doc = server_collection.find_one({"_id": server_id})

    if server_doc:
        return new_server(server_doc)
    else:
        return None

###############################################################################
## Looks up the server from config file
def config_lookup_server(server_id):
    servers = get_configured_servers()
    return servers.get(server_id)

###############################################################################
# returns all servers configured in both DB and config file
def lookup_all_servers():
    validate_repositories()

    all_servers = {}

    if consulting_db_repository():
        all_servers = db_lookup_all_servers()

    if has_file_repository():
        file_repo_servers = get_configured_servers()
        all_servers = dict(file_repo_servers.items() + all_servers.items())

    return all_servers.values()

###############################################################################
# returns servers saved in the db collection of servers
def db_lookup_all_servers():
    servers = get_mongoctl_server_db_collection()
    return new_servers_dict(servers.find())

###############################################################################
# Cluster lookup functions
###############################################################################
def lookup_and_validate_cluster(cluster_id):
    cluster = lookup_cluster(cluster_id)

    if cluster is None:
        raise MongoctlException("Unknown cluster: %s" % cluster_id)

    validate_cluster(cluster)

    return cluster

###############################################################################
# Lookup by cluster id
def lookup_cluster(cluster_id):
    validate_repositories()
    cluster = None
    # lookup cluster from the db repo first

    if consulting_db_repository():
        cluster = db_lookup_cluster(cluster_id)

    # if cluster is not found then try from file repo
    if cluster is None and has_file_repository():
        cluster = config_lookup_cluster(cluster_id)

    return cluster

###############################################################################
# Looks up the server from config file
def config_lookup_cluster(cluster_id):
    clusters = get_configured_clusters()
    return clusters.get(cluster_id)

###############################################################################
def db_lookup_cluster(cluster_id):
    cluster_collection = get_mongoctl_cluster_db_collection()
    cluster_doc = cluster_collection.find_one({"_id": cluster_id})

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None

###############################################################################
# returns all clusters configured in both DB and config file
def lookup_all_clusters():
    validate_repositories()
    all_clusters = {}

    if consulting_db_repository():
        all_clusters = db_lookup_all_clusters()

    if has_file_repository():
        all_clusters = dict(get_configured_clusters().items() +
                            all_clusters.items())

    return all_clusters.values()

###############################################################################
# returns a dictionary of (cluster_id, cluster) looked up from DB
def db_lookup_all_clusters():
    clusters = get_mongoctl_cluster_db_collection()
    return new_replicaset_clusters_dict(clusters.find())

###############################################################################
# Lookup by server id
def db_lookup_cluster_by_server(server):
    cluster_collection = get_mongoctl_cluster_db_collection()
    cluster_doc = cluster_collection.find_one({"members.server.$id":
                                                   server.id})

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None


###############################################################################
def config_lookup_cluster_by_server(server):
    clusters = get_configured_clusters()
    result = filter(lambda c: c.has_member_server(server), clusters.values())
    if result:
        return result[0]


###############################################################################
# Global variable: lazy loaded map that holds servers read from config file
__configured_servers__ = None

###############################################################################
def get_configured_servers():

    global __configured_servers__

    if __configured_servers__ is None:
        __configured_servers__ = {}

        file_repo_conf = config.get_file_repository_conf()
        servers_path_or_url = file_repo_conf.get("servers",
                                                 DEFAULT_SERVERS_FILE)

        server_documents = config.read_config_json("servers",
                                                   servers_path_or_url)
        if not isinstance(server_documents, list):
            raise MongoctlException("Server list in '%s' must be an array" %
                                    servers_path_or_url)
        for document in server_documents:
            server = new_server(document)
            __configured_servers__[server.id] = server

    return __configured_servers__


###############################################################################
# Global variable: lazy loaded map that holds clusters read from config file
__configured_clusters__ = None

###############################################################################
def get_configured_clusters():

    global __configured_clusters__

    if __configured_clusters__ is None:
        __configured_clusters__ = {}

        file_repo_conf = config.get_file_repository_conf()
        clusters_path_or_url = file_repo_conf.get("clusters",
                                                  DEFAULT_CLUSTERS_FILE)



        cluster_documents = config.read_config_json("clusters",
                                                    clusters_path_or_url)
        if not isinstance(cluster_documents, list):
            raise MongoctlException("Cluster list in '%s' must be an array" %
                                    clusters_path_or_url)
        for document in cluster_documents:
            cluster = new_cluster(document)
            __configured_clusters__[cluster.id] = cluster

    return __configured_clusters__

###############################################################################
def validate_cluster(cluster):
    log_info("Validating cluster '%s'..." % cluster.id )

    errors = []

    ## validate repl key if needed
    def server_needs_repl_key(server):
        return server.needs_repl_key()

    if (cluster.has_any_server_that(server_needs_repl_key) and
                cluster.get_repl_key() is None):
        errors.append(
            "** no replKey configured. replKey is required because at least "
            "one member has 'auth' enabled.")

    if len(errors) > 0:
        raise MongoctlException("Cluster %s configuration is not valid. "
                                "Please fix errors below and try again.\n%s" %
                                (cluster.id , "\n".join(errors)))

    return cluster

###############################################################################
def lookup_validate_cluster_by_server(server):
    cluster = lookup_cluster_by_server(server)

    if cluster is not None:
        validate_cluster(cluster)

    return cluster

###############################################################################
def lookup_cluster_by_server(server):
    validate_repositories()
    cluster = None

    ## Look for the cluster in db repo
    if consulting_db_repository():
        cluster = db_lookup_cluster_by_server(server)

    ## If nothing is found then look in file repo
    if cluster is None and has_file_repository():
        cluster = config_lookup_cluster_by_server(server)


    return cluster

###############################################################################
def validate_server(server):
    errors = []

    version = server.get_mongo_version()
    # None versions are ok
    if version is not None:
        if not is_valid_version(version):
            errors.append("** Invalid mongoVersion value '%s'" % version)
        elif not is_supported_mongo_version(version):
            errors.append("** mongoVersion '%s' is not supported. Please refer"
                          " to mongoctl documentation for supported"
                          " versions." % version)
    return errors

###############################################################################
def get_mongoctl_server_db_collection():

    mongoctl_db = get_mongoctl_database()
    conf = config.get_database_repository_conf()

    server_collection_name = conf.get("servers", DEFAULT_SERVERS_COLLECTION)

    return mongoctl_db[server_collection_name]

###############################################################################
def get_mongoctl_cluster_db_collection():

    mongoctl_db = get_mongoctl_database()
    conf = config.get_database_repository_conf()
    cluster_collection_name = conf.get("clusters", DEFAULT_CLUSTERS_COLLECTION)

    return mongoctl_db[cluster_collection_name]

###############################################################################
def get_activity_collection():

    mongoctl_db = get_mongoctl_database()

    activity_coll_name = config.get_mongoctl_config_val(
        'activityCollectionName', DEFAULT_ACTIVITY_COLLECTION)

    return mongoctl_db[activity_coll_name]


###############################################################################
# Factory Functions
###############################################################################
def new_server(server_doc):
    _type = server_doc.get("_type")

    if _type is None or _type == "mongod":
        server_type = "mongoctl.objects.mongod.MongodServer"
    elif _type == "mongos":
        server_type = "mongoctl.objects.mongos.MongosServer"
    else:
        raise MongoctlException("Unknown server _type '%s' for server:\n%s" %
                                (_type, document_pretty_string(server_doc)))

    clazz = resolve_class(server_type)
    return clazz(server_doc)

###############################################################################
def build_server_from_address(address):
    if not is_valid_member_address(address):
        return None

    port = int(address.split(":")[1])
    server_doc = {"_id": address,
                  "address": address,
                  "cmdOptions":{
                      "port": port
                  }}
    return new_server(server_doc)

###############################################################################
def build_server_from_uri(uri):
    uri_wrapper = parse_mongo_uri(uri)
    node = uri_wrapper.node_list[0]
    host = node[0]
    port = node[1]

    database = uri_wrapper.database or "admin"
    username = uri_wrapper.username
    password = uri_wrapper.password

    address = "%s:%s" % (host, port)
    server = build_server_from_address(address)

    # set login user if specified
    if username:
        server.set_login_user(database, username, password)

    return server

###############################################################################
def build_cluster_from_uri(uri):
    uri_wrapper = parse_mongo_uri(uri)
    database = uri_wrapper.database or "admin"
    username = uri_wrapper.username
    password = uri_wrapper.password

    nodes = uri_wrapper.node_list
    cluster_doc = {
        "_id": mask_mongo_uri(uri)
    }
    member_doc_list = []

    for node in nodes:
        host = node[0]
        port = node[1]
        member_doc = {
            "host": "%s:%s" % (host, port)
        }
        member_doc_list.append(member_doc)

    cluster_doc["members"] = member_doc_list

    cluster = new_cluster(cluster_doc)

    # set login user if specified
    if username:
        for member in cluster.get_members():
            member.get_server().set_login_user(database, username, password)

    return cluster

###############################################################################
def build_server_or_cluster_from_uri(uri):
    if is_cluster_mongo_uri(uri):
        return build_cluster_from_uri(uri)
    else:
        return build_server_from_uri(uri)

###############################################################################
def new_servers_dict(docs):
    dict = {}
    map(lambda doc: dict.update({doc['_id']: new_server(doc)}), docs)
    return dict

###############################################################################
def new_cluster(cluster_doc):
    return objects.cluster.ReplicaSetCluster(cluster_doc)

###############################################################################
def new_replicaset_clusters_dict(docs):
    dict = {}
    map(lambda doc: dict.update({doc['_id']: new_cluster(doc)}), docs)
    return dict

###############################################################################
def new_replicaset_cluster_member(cluster_mem_doc):
    return objects.cluster.ReplicaSetClusterMember(cluster_mem_doc)

###############################################################################
def new_replicaset_cluster_member_list(docs_iteratable):
    return map(new_replicaset_cluster_member, docs_iteratable)
