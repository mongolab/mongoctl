

__author__ = 'abdul'

import pymongo
import pymongo.read_preferences
import config

from bson import DBRef

from errors import MongoctlException
from mongoctl_logging import log_warning, log_verbose, log_info, log_exception
from mongo_uri_tools import parse_mongo_uri
from utils import (
    resolve_class, document_pretty_string, is_valid_member_address, listify
    )

from mongodb_version import is_supported_mongo_version, is_valid_version
from mongo_uri_tools import is_cluster_mongo_uri, mask_mongo_uri

DEFAULT_SERVERS_FILE = "servers.config"

DEFAULT_CLUSTERS_FILE = "clusters.config"

DEFAULT_SERVERS_COLLECTION = "servers"

DEFAULT_CLUSTERS_COLLECTION = "clusters"

DEFAULT_ACTIVITY_COLLECTION = "logs.server-activity"


LOOKUP_TYPE_MEMBER = "members"
LOOKUP_TYPE_CONFIG_SVR = "configServers"
LOOKUP_TYPE_SHARDS = "shards"
LOOKUP_TYPE_ANY = [LOOKUP_TYPE_CONFIG_SVR, LOOKUP_TYPE_MEMBER,
                   LOOKUP_TYPE_SHARDS]

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

        __mongoctl_db__ = _db_repo_connect()
        return __mongoctl_db__
    except Exception, e:
        log_exception(e)
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
    client = pymongo.MongoClient(uri, read_preference=
    pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED)
    return client.get_default_database()



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
def db_lookup_cluster_by_server(server, lookup_type=LOOKUP_TYPE_ANY):
    cluster_collection = get_mongoctl_cluster_db_collection()
    lookup_type = listify(lookup_type)
    type_query =[]
    for t in lookup_type:
        prop_query = {"%s.server.$id" % t: server.id}
        type_query.append(prop_query)

    query = {
        "$or": type_query
    }

    cluster_doc = cluster_collection.find_one(query)

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None


###############################################################################
# Lookup by server id
def db_lookup_cluster_by_shard(shard):
    cluster_collection = get_mongoctl_cluster_db_collection()

    query = {
        "shards.cluster.$id": shard.id
    }

    cluster_doc = cluster_collection.find_one(query)

    if cluster_doc is not None:
        return new_cluster(cluster_doc)
    else:
        return None



###############################################################################
def config_lookup_cluster_by_server(server, lookup_type=LOOKUP_TYPE_ANY):
    clusters = get_configured_clusters()
    lookup_type = listify(lookup_type)

    for t in lookup_type:
        result = None
        if t == LOOKUP_TYPE_MEMBER:
            result = filter(lambda c: c.has_member_server(server),
                            clusters.values())
        elif t == LOOKUP_TYPE_CONFIG_SVR:
            result = filter(lambda c: cluster_has_config_server(c, server),
                            clusters.values())
        elif t == LOOKUP_TYPE_SHARDS:
            result = filter(lambda c: cluster_has_shard(c, server),
                            clusters.values())
        if result:
            return result[0]

###############################################################################
def config_lookup_cluster_by_shard(shard):
    clusters = get_configured_clusters()

    result = filter(lambda c: cluster_has_shard(c, shard), clusters.values())
    if result:
        return result[0]

###############################################################################
def cluster_has_config_server(cluster, server):
    config_servers = cluster.get_property("configServers")
    if config_servers:
        for server_doc in config_servers:
            server_ref = server_doc["server"]
            if isinstance(server_ref, DBRef) and server_ref.id == server.id:
                return cluster

###############################################################################
def cluster_has_shard(cluster, shard):
    from objects.server import Server
    shards = cluster.get_property("shards")
    if shards:
        for shard_doc in shards:
            if isinstance(shard, Server):
                ref = shard_doc.get("server")
            else:
                ref = shard_doc.get("cluster")
            if isinstance(ref, DBRef) and ref.id == shard.id:
                return cluster

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

    if isinstance(cluster, replicaset_cluster_type()):
        errors.extend(validate_replicaset_cluster(cluster))
    elif isinstance(cluster, sharded_cluster_type()):
        errors.extend(validate_sharded_cluster(cluster))

    if len(errors) > 0:
        raise MongoctlException("Cluster %s configuration is not valid. "
                                "Please fix errors below and try again.\n%s" %
                                (cluster.id , "\n".join(errors)))

    return cluster

###############################################################################
def validate_replicaset_cluster(cluster):
    errors = []
    return errors

###############################################################################
def validate_sharded_cluster(cluster):
    errors = []
    if not cluster.config_members or len(cluster.config_members) not in [1,3]:
        errors.append("Need 1 or 3 configServers configured in your cluster")

    return errors

###############################################################################
def lookup_validate_cluster_by_server(server):
    cluster = lookup_cluster_by_server(server)

    if cluster is not None:
        validate_cluster(cluster)

    return cluster

###############################################################################
def lookup_cluster_by_server(server, lookup_type=LOOKUP_TYPE_ANY):
    validate_repositories()
    cluster = None

    ## Look for the cluster in db repo
    if consulting_db_repository():
        cluster = db_lookup_cluster_by_server(server, lookup_type=lookup_type)

    ## If nothing is found then look in file repo
    if cluster is None and has_file_repository():
        cluster = config_lookup_cluster_by_server(server,
                                                  lookup_type=lookup_type)


    return cluster


###############################################################################
def lookup_cluster_by_shard(shard):
    validate_repositories()
    cluster = None

    ## Look for the cluster in db repo
    if consulting_db_repository():
        cluster = db_lookup_cluster_by_shard(shard)

    ## If nothing is found then look in file repo
    if cluster is None and has_file_repository():
        cluster = config_lookup_cluster_by_shard(shard)

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

    if _type is None or _type in SERVER_TYPE_MAP:
        clazz = resolve_class(SERVER_TYPE_MAP.get(_type,
                                                  MONGOD_SERVER_CLASS_NAME))
    else:
        raise MongoctlException("Unknown server _type '%s' for server:\n%s" %
                                (_type, document_pretty_string(server_doc)))

    return clazz(server_doc)

MONGOD_SERVER_CLASS_NAME = "mongoctl.objects.mongod.MongodServer"
MONGOS_ROUTER_CLASS_NAME = "mongoctl.objects.mongos.MongosServer"
SERVER_TYPE_MAP = {
    "Mongod": MONGOD_SERVER_CLASS_NAME,
    "ConfigMongod": MONGOD_SERVER_CLASS_NAME,
    "Mongos": MONGOS_ROUTER_CLASS_NAME,
    # === XXX deprecated XXX ===
    "mongod": MONGOD_SERVER_CLASS_NAME,  # XXX deprecated
    "mongos": MONGOS_ROUTER_CLASS_NAME,  # XXX deprecated
}


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
    d = {}
    map(lambda doc: d.update({doc['_id']: new_server(doc)}), docs)
    return d

###############################################################################
def new_cluster(cluster_doc):
    _type = cluster_doc.get("_type")

    if _type is None or _type == "ReplicaSetCluster":
        clazz = replicaset_cluster_type()
    elif _type == "ShardedCluster":
        clazz = sharded_cluster_type()
    else:
        raise MongoctlException("Unknown cluster _type '%s' for server:\n%s" %
                                (_type, document_pretty_string(cluster_doc)))
    return clazz(cluster_doc)

###############################################################################
def new_replicaset_clusters_dict(docs):
    d = {}
    map(lambda doc: d.update({doc['_id']: new_cluster(doc)}), docs)
    return d

###############################################################################
def new_replicaset_cluster_member(cluster_mem_doc):
    mem_type = "mongoctl.objects.replicaset_cluster.ReplicaSetClusterMember"
    clazz = resolve_class(mem_type)
    return clazz(cluster_mem_doc)

###############################################################################
def new_replicaset_cluster_member_list(docs_iteratable):
    return map(new_replicaset_cluster_member, docs_iteratable)

def replicaset_cluster_type():
    return resolve_class("mongoctl.objects.replicaset_cluster."
                         "ReplicaSetCluster")

def sharded_cluster_type():
    return resolve_class("mongoctl.objects.sharded_cluster.ShardedCluster")
