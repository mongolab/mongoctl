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
from mongoctl.mongodb_version import make_version_info

###############################################################################
# CONSTS
###############################################################################
SUPPORTED_MONGO_RESTORE_OPTIONS = [
    "directoryperdb",
    "journal",
    "collection",
    "ipv6",
    "filter",
    "objcheck",
    "drop",
    "oplogReplay",
    "keepIndexVersion",
    "verbose",
    "authenticationDatabase",
    "restoreDbUsersAndRoles",
    "noIndexRestore"
]


###############################################################################
# restore command
###############################################################################
def restore_command(parsed_options):

    # get and validate source/destination
    source = parsed_options.source
    destination = parsed_options.destination

    is_addr = is_db_address(destination)
    is_path = is_dbpath(destination)

    if is_addr and is_path:
        msg = ("Ambiguous destination value '%s'. Your destination matches"
               " both a dbpath and a db address. Use prefix 'file://',"
               " 'cluster://' or 'server://' to make it more specific" %
               destination)

        raise MongoctlException(msg)

    elif not (is_addr or is_path):
        raise MongoctlException("Invalid destination value '%s'. Destination has to be"
                                " a valid db address or dbpath." % destination)
    restore_options = extract_mongo_restore_options(parsed_options)

    if is_addr:
        mongo_restore_db_address(destination,
                                 source,
                                 username=parsed_options.username,
                                 password=parsed_options.password,
                                 restore_options=restore_options)
    else:
        dbpath = resolve_path(destination)
        mongo_restore_db_path(dbpath, source, restore_options=restore_options)


###############################################################################
# mongo_restore
###############################################################################
def mongo_restore_db_address(db_address,
                             source,
                             username=None,
                             password=None,
                             restore_options=None):

    if is_mongo_uri(db_address):
        mongo_restore_uri(db_address, source, username, password,
                          restore_options)
        return

    # db_address is an id string
    id_path = db_address.split("/")
    id = id_path[0]
    database = id_path[1] if len(id_path) == 2 else None

    server = repository.lookup_server(id)
    if server:
        mongo_restore_server(server, source, database=database,
                             username=username, password=password,
                             restore_options=restore_options)
        return
    else:
        cluster = repository.lookup_cluster(id)
        if cluster:
            mongo_restore_cluster(cluster, source, database=database,
                                  username=username, password=password,
                                  restore_options=restore_options)
            return

    raise MongoctlException("Unknown db address '%s'" % db_address)

###############################################################################
def mongo_restore_db_path(dbpath, source, restore_options=None):
    do_mongo_restore(source, dbpath=dbpath, restore_options=restore_options)

###############################################################################
def mongo_restore_uri(uri, source,
                      username=None,
                      password=None,
                      restore_options=None):

    uri_wrapper = parse_mongo_uri(uri)
    database = uri_wrapper.database
    username = username if username else uri_wrapper.username
    password = password if password else uri_wrapper.password

    server_or_cluster = repository.build_server_or_cluster_from_uri(uri)

    if isinstance(server_or_cluster, Server):
        mongo_restore_server(server_or_cluster, source, database=database,
                             username=username, password=password,
                             restore_options=restore_options)
    else:
        mongo_restore_cluster(server_or_cluster, source, database=database,
                              username=username, password=password,
                              restore_options=restore_options)

###############################################################################
def mongo_restore_server(server, source,
                         database=None,
                         username=None,
                         password=None,
                         restore_options=None):
    repository.validate_server(server)

    # auto complete password if possible
    if username:
        if not password and database:
            password = server.lookup_password(database, username)
        if not password:
            password = server.lookup_password("admin", username)

    do_mongo_restore(source,
                     host=server.get_connection_host_address(),
                     port=server.get_port(),
                     database=database,
                     username=username,
                     password=password,
                     version_info=server.get_mongo_version_info(),
                     restore_options=restore_options,
                     ssl=server.use_ssl_client())


###############################################################################
def mongo_restore_cluster(cluster, source,
                          database=None,
                          username=None,
                          password=None,
                          restore_options=None):
    repository.validate_cluster(cluster)
    log_info("Locating default server for cluster '%s'..." % cluster.id)
    default_server = cluster.get_default_server()
    if default_server:
        log_info("Restoring default server '%s'" % default_server.id)
        mongo_restore_server(default_server, source,
                             database=database,
                             username=username,
                             password=password,
                             restore_options=restore_options)
    else:
        raise MongoctlException("No default server found for cluster '%s'" %
                                cluster.id)

###############################################################################
def do_mongo_restore(source,
                     host=None,
                     port=None,
                     dbpath=None,
                     database=None,
                     username=None,
                     password=None,
                     version_info=None,
                     restore_options=None,
                     ssl=False):

    # create restore command with host and port
    restore_cmd = [get_mongo_restore_executable(version_info)]

    # ssl options
    if ssl:
        restore_cmd.append("--ssl")

    if host:
        restore_cmd.extend(["--host", host])
    if port:
        restore_cmd.extend(["--port", str(port)])

    # dbpath
    if dbpath:
        restore_cmd.extend(["--dbpath", dbpath])

    # database
    if database:
        restore_cmd.extend(["-d", database])

    # username and password
    if username:
        restore_cmd.extend(["-u", username, "-p"])
        if password:
            restore_cmd.append(password)

    # ignore authenticationDatabase option is version_info is less than 2.4.0
    if (restore_options and "authenticationDatabase" in restore_options and
            version_info and version_info < make_version_info("2.4.0")):
        restore_options.pop("authenticationDatabase", None)

    # ignore restoreDbUsersAndRoles option is version_info is less than 2.6.0
    if (restore_options and "restoreDbUsersAndRoles" in restore_options and
            version_info and version_info < make_version_info("2.6.0")):
        restore_options.pop("restoreDbUsersAndRoles", None)

    # append shell options
    if restore_options:
        restore_cmd.extend(options_to_command_args(restore_options))

    # pass source arg
    restore_cmd.append(source)

    cmd_display =  restore_cmd[:]
    # mask user/password
    if username:
        cmd_display[cmd_display.index("-u") + 1] =  "****"
        if password:
            cmd_display[cmd_display.index("-p") + 1] =  "****"

    # execute!
    log_info("Executing command: \n%s" % " ".join(cmd_display))
    call_command(restore_cmd, bubble_exit_code=True)


###############################################################################
def get_mongo_restore_executable(version_info):
    restore_exe = get_mongo_executable(version_info,
                                       'mongorestore',
                                       version_check_pref=
                                       VERSION_PREF_EXACT_OR_MINOR)
    # Warn the user if it is not an exact match (minor match)
    if version_info and version_info != restore_exe.version:
        log_warning("Using mongorestore '%s' that does not exactly match"
                    "server version '%s'" % (restore_exe.version,
                                             version_info))

    return restore_exe.path

###############################################################################
def extract_mongo_restore_options(parsed_args):
    return extract_mongo_exe_options(parsed_args,
                                     SUPPORTED_MONGO_RESTORE_OPTIONS)
