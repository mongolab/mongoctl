__author__ = 'abdul'


import mongoctl.repository as repository

from mongoctl.mongo_uri_tools import is_mongo_uri, parse_mongo_uri

from mongoctl.utils import resolve_path
from mongoctl.mongoctl_logging import log_info , log_warning

from mongoctl.commands.command_utils import (
    is_db_address, is_dbpath, extract_mongo_exe_options, get_mongo_executable,
    options_to_command_args,
    VERSION_PREF_EXACT_OR_MINOR
    )
from mongoctl.errors import MongoctlException

from mongoctl.utils import call_command
from mongoctl.objects.server import Server
from mongoctl.mongodb_version import MongoDBVersionInfo


###############################################################################
# CONSTS
###############################################################################

SUPPORTED_MONGO_DUMP_OPTIONS = [
    "directoryperdb",
    "journal",
    "collection",
    "out",
    "query",
    "oplog",
    "repair",
    "forceTableScan",
    "ipv6",
    "verbose",
    "authenticationDatabase",
    "dumpDbUsersAndRoles"
]


###############################################################################
# dump command
###############################################################################
def dump_command(parsed_options):

    # get and validate dump target
    target = parsed_options.target
    use_best_secondary = parsed_options.useBestSecondary
    #max_repl_lag = parsed_options.maxReplLag
    is_addr = is_db_address(target)
    is_path = is_dbpath(target)

    if is_addr and is_path:
        msg = ("Ambiguous target value '%s'. Your target matches both a dbpath"
               " and a db address. Use prefix 'file://', 'cluster://' or"
               " 'server://' to make it more specific" % target)

        raise MongoctlException(msg)

    elif not (is_addr or is_path):
        raise MongoctlException("Invalid target value '%s'. Target has to be"
                                " a valid db address or dbpath." % target)
    dump_options = extract_mongo_dump_options(parsed_options)

    if is_addr:
        mongo_dump_db_address(target,
                              username=parsed_options.username,
                              password=parsed_options.password,
                              use_best_secondary=use_best_secondary,
                              max_repl_lag=None,
                              dump_options=dump_options)
    else:
        dbpath = resolve_path(target)
        mongo_dump_db_path(dbpath, dump_options=dump_options)

###############################################################################
# mongo_dump
###############################################################################
def mongo_dump_db_address(db_address,
                          username=None,
                          password=None,
                          use_best_secondary=False,
                          max_repl_lag=None,
                          dump_options=None):

    if is_mongo_uri(db_address):
        mongo_dump_uri(uri=db_address, username=username, password=password,
                       use_best_secondary=use_best_secondary,
                       dump_options=dump_options)
        return

    # db_address is an id string
    id_path = db_address.split("/")
    id = id_path[0]
    database = id_path[1] if len(id_path) == 2 else None

    server = repository.lookup_server(id)
    if server:
        mongo_dump_server(server, database=database, username=username,
                          password=password, dump_options=dump_options)
        return
    else:
        cluster = repository.lookup_cluster(id)
        if cluster:
            mongo_dump_cluster(cluster, database=database, username=username,
                               password=password,
                               use_best_secondary=use_best_secondary,
                               max_repl_lag=max_repl_lag,
                               dump_options=dump_options)
            return

            # Unknown destination
    raise MongoctlException("Unknown db address '%s'" % db_address)

###############################################################################
def mongo_dump_db_path(dbpath, dump_options=None):

    do_mongo_dump(dbpath=dbpath,
                  dump_options=dump_options)

###############################################################################
def mongo_dump_uri(uri,
                   username=None,
                   password=None,
                   use_best_secondary=False,
                   dump_options=None):

    uri_wrapper = parse_mongo_uri(uri)
    database = uri_wrapper.database
    username = username if username else uri_wrapper.username
    password = password if password else uri_wrapper.password

    server_or_cluster = repository.build_server_or_cluster_from_uri(uri)

    if isinstance(server_or_cluster, Server):
        mongo_dump_server(server_or_cluster,
                          database=database,
                          username=username,
                          password=password,
                          dump_options=dump_options)
    else:
        mongo_dump_cluster(server_or_cluster,
                           database=database,
                           username=username,
                           password=password,
                           use_best_secondary=use_best_secondary,
                           dump_options=dump_options)

###############################################################################
def mongo_dump_server(server,
                      database=None,
                      username=None,
                      password=None,
                      dump_options=None):
    repository.validate_server(server)

    auth_db = database or "admin"
    # auto complete password if possible
    if username:
        if not password and database:
            password = server.lookup_password(database, username)
        if not password:
            password = server.lookup_password("admin", username)


    do_mongo_dump(host=server.get_connection_host_address(),
                  port=server.get_port(),
                  database=database,
                  username=username,
                  password=password,
                  version_info=server.get_mongo_version_info(),
                  dump_options=dump_options,
                  ssl=server.use_ssl_client())

###############################################################################
def mongo_dump_cluster(cluster,
                       database=None,
                       username=None,
                       password=None,
                       use_best_secondary=False,
                       max_repl_lag=False,
                       dump_options=None):
    repository.validate_cluster(cluster)

    if use_best_secondary:
        mongo_dump_cluster_best_secondary(cluster=cluster,
                                          max_repl_lag=max_repl_lag,
                                          database=database,
                                          username=username,
                                          password=password,
                                          dump_options=dump_options)
    else:
        mongo_dump_cluster_primary(cluster=cluster,
                                   database=database,
                                   username=username,
                                   password=password,
                                   dump_options=dump_options)
###############################################################################
def mongo_dump_cluster_primary(cluster,
                               database=None,
                               username=None,
                               password=None,
                               dump_options=None):
    log_info("Locating default server for cluster '%s'..." % cluster.id)
    default_server = cluster.get_default_server()
    if default_server:
        log_info("Dumping default server '%s'..." % default_server.id)
        mongo_dump_server(default_server,
                          database=database,
                          username=username,
                          password=password,
                          dump_options=dump_options)
    else:
        raise MongoctlException("No default server found for cluster '%s'" %
                                cluster.id)


###############################################################################
def mongo_dump_cluster_best_secondary(cluster,
                                      max_repl_lag=None,
                                      database=None,
                                      username=None,
                                      password=None,
                                      dump_options=None):

    #max_repl_lag = max_repl_lag or 3600
    log_info("Finding best secondary server for cluster '%s' with replication"
             " lag less than max (%s seconds)..." %
             (cluster.id, max_repl_lag))
    best_secondary = cluster.get_dump_best_secondary(max_repl_lag=max_repl_lag)
    if best_secondary:
        server = best_secondary.get_server()

        log_info("Found secondary server '%s'. Dumping..." % server.id)
        mongo_dump_server(server, database=database, username=username,
                          password=password, dump_options=dump_options)
    else:
        raise MongoctlException("No secondary server found for cluster '%s'" %
                                cluster.id)

###############################################################################
def do_mongo_dump(host=None,
                  port=None,
                  dbpath=None,
                  database=None,
                  username=None,
                  password=None,
                  version_info=None,
                  dump_options=None,
                  ssl=False):


    # create dump command with host and port
    dump_cmd = [get_mongo_dump_executable(version_info)]

    # ssl options
    if ssl:
        dump_cmd.append("--ssl")

    if host:
        dump_cmd.extend(["--host", host])
    if port:
        dump_cmd.extend(["--port", str(port)])

    # dbpath
    if dbpath:
        dump_cmd.extend(["--dbpath", dbpath])

    # database
    if database:
        dump_cmd.extend(["-d", database])

    # username and password
    if username:
        dump_cmd.extend(["-u", username, "-p"])
        if password:
            dump_cmd.append(password)

    # ignore authenticationDatabase option is version_info is less than 2.4.0
    if (dump_options and "authenticationDatabase" in dump_options and
            version_info and version_info < MongoDBVersionInfo("2.4.0")):
        dump_options.pop("authenticationDatabase", None)

    # ignore dumpDbUsersAndRoles option is version_info is less than 2.6.0
    if (dump_options and "dumpDbUsersAndRoles" in dump_options and
            version_info and version_info < MongoDBVersionInfo("2.6.0")):
        dump_options.pop("dumpDbUsersAndRoles", None)

    # append shell options
    if dump_options:
        dump_cmd.extend(options_to_command_args(dump_options))


    cmd_display =  dump_cmd[:]
    # mask user/password
    if username:
        cmd_display[cmd_display.index("-u") + 1] = "****"
        if password:
            cmd_display[cmd_display.index("-p") + 1] =  "****"



    log_info("Executing command: \n%s" % " ".join(cmd_display))
    call_command(dump_cmd, bubble_exit_code=True)


###############################################################################
def extract_mongo_dump_options(parsed_args):
    return extract_mongo_exe_options(parsed_args,
                                     SUPPORTED_MONGO_DUMP_OPTIONS)

###############################################################################
def get_mongo_dump_executable(version_info):
    dump_exe = get_mongo_executable(version_info,
                                    'mongodump',
                                    version_check_pref=
                                    VERSION_PREF_EXACT_OR_MINOR)
    # Warn the user if it is not an exact match (minor match)
    if version_info and version_info != dump_exe.version:
        log_warning("Using mongodump '%s' that does not exactly match "
                    "server version '%s'" % (dump_exe.version, version_info))

    return dump_exe.path
