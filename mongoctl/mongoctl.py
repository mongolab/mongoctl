#!/usr/bin/env python

# The MIT License

# Copyright (c) 2012 ObjectLabs Corporation

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

__author__ = 'abdul'

###############################################################################
# Imports
###############################################################################

import sys
import traceback

import os


import shutil
import platform
import urllib

import repository
import config
import objects.server

from dargparse import dargparse

from bson.son import SON
from mongoctl_command_config import MONGOCTL_PARSER_DEF

from mongo_uri_tools import *
from utils import *
from mongoctl_logging import *
from prompt import *

from mongo_version import *

from errors import MongoctlException

###############################################################################
# Constants
###############################################################################



CONF_ROOT_ENV_VAR = "MONGOCTL_CONF"

SERVER_ID_PARAM = "server"

CLUSTER_ID_PARAM = "cluster"

LATEST_VERSION_FILE_URL = "https://raw.github.com/mongolab/mongoctl/master/" \
                          "mongo_latest_stable_version.txt"
###############################################################################
# MAIN
###############################################################################
def main(args):
    try:
        do_main(args)
    except MongoctlException,e:
        log_error(e)
        exit(1)

###############################################################################
def do_main(args):
    header = """
-------------------------------------------------------------------------------------------
  __ _  ___  ___  ___ ____  ____/ /_/ /
 /  ' \/ _ \/ _ \/ _ `/ _ \/ __/ __/ / 
/_/_/_/\___/_//_/\_, /\___/\__/\__/_/  
                /___/ 
-------------------------------------------------------------------------------------------
   """

    # Parse options
    parser = get_mongoctl_cmd_parser()

    if len(args) < 1:
        print(header)
        parser.print_help()
        return

    # Parse the arguments and call the function of the selected cmd
    parsed_args = parser.parse_args(args)

    # turn on verbose if specified
    if namespace_get_property(parsed_args,"mongoctlVerbose"):
        turn_logging_verbose_on()

    # set interactive mode
    non_interactive = namespace_get_property(parsed_args,'noninteractive')
    non_interactive = False if non_interactive is None else non_interactive

    set_interactive_mode(not non_interactive)

    if not is_interactive_mode():
        log_verbose("Running with noninteractive mode")

    # set global prompt value
    yes_all = parsed_args.yesToEverything
    no_all = parsed_args.noToEverything

    if yes_all and no_all:
        raise MongoctlException("Cannot have --yes and --no at the same time. "
                                "Please choose either --yes or --no")
    elif yes_all:
        say_yes_to_everything()
    elif no_all:
        say_no_to_everything()

    # set conf root if specified
    if parsed_args.configRoot is not None:
        config._set_config_root(parsed_args.configRoot)
    elif os.getenv(CONF_ROOT_ENV_VAR) is not None:
        config._set_config_root(os.getenv(CONF_ROOT_ENV_VAR))

    # get the function to call from the parser framework
    command_function = parsed_args.func

    # parse global login if present
    parse_global_login_user_arg(parsed_args)
    server_id = namespace_get_property(parsed_args,SERVER_ID_PARAM)

    if server_id is not None:
        # check if assumeLocal was specified
        assume_local = namespace_get_property(parsed_args,"assumeLocal")
        if assume_local:
            objects.server.assume_local_server(server_id)
    # execute command
    log_info("")
    return command_function(parsed_args)

###############################################################################
########################                       ################################
######################## Commandline functions ################################
########################                       ################################
###############################################################################












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
# print uri command
###############################################################################
def print_uri_command(parsed_options):
    id = parsed_options.id
    db = parsed_options.db
    # check if the id is a server id

    server = lookup_server(id)
    if server:
        print server.get_mongo_uri_template(db=db)
    else:
        cluster = lookup_cluster(id)
        if cluster:
            print cluster.get_replica_mongo_uri_template(db=db)
        else:
            raise MongoctlException("Cannot find a server or a cluster with"
                                    " id '%s'" % id)



###############################################################################
# configure cluster command
###############################################################################
def configure_cluster_command(parsed_options):
    cluster_id = parsed_options.cluster
    force_primary_server_id = parsed_options.forcePrimaryServer

    if parsed_options.dryRun:
        dry_run_configure_cluster(cluster_id=cluster_id,
                                  force_primary_server_id=
                                    force_primary_server_id)
    else:
        configure_cluster(cluster_id=cluster_id,
                          force_primary_server_id=
                            force_primary_server_id)

###############################################################################
# list clusters command
###############################################################################
def list_clusters_command(parsed_options):
    clusters = lookup_all_clusters()
    if not clusters or len(clusters) < 1:
        log_info("No clusters configured");
        return

    # sort clusters by id
    clusters = sorted(clusters, key=lambda c: c.get_id())
    bar = "-"*80
    print bar
    formatter = "%-25s %-40s %s"
    print formatter % ("_ID", "DESCRIPTION", "MEMBERS")
    print bar

    for cluster in clusters:
        desc = _lista(cluster.get_description())

        members_info = "[ %s ]" % ", ".join(cluster.get_members_info())

        print formatter % (cluster.get_id(), desc, members_info)
    print "\n"

###############################################################################
# show cluster command
###############################################################################
def show_cluster_command(parsed_options):
    cluster = lookup_cluster(parsed_options.cluster)
    if cluster is None:
        raise MongoctlException("Could not find cluster '%s'." %
                            parsed_options.cluster)
    log_info("Configuration for cluster '%s':" % parsed_options.cluster)
    print cluster

###############################################################################
# install command
###############################################################################
def install_command(parsed_options):
    install_mongodb(version=parsed_options.version)

###############################################################################
# uninstall command
###############################################################################
def uninstall_command(parsed_options):
    uninstall_mongodb(version=parsed_options.version)

###############################################################################
# list-versions command
###############################################################################
def list_versions_command(parsed_options):
    mongo_installations = find__all_mongo_installations()

    bar = "-" * 80
    print bar
    formatter = "%-20s %s"
    print formatter % ("VERSION", "LOCATION")
    print bar

    for install_dir,version in mongo_installations:
        print formatter % (version, install_dir)
    print "\n"


###############################################################################
########################                   ####################################
########################    Mongoctl API   ####################################
########################                   ####################################
###############################################################################






###############################################################################
# Cluster Methods
###############################################################################
def configure_cluster(cluster_id, force_primary_server_id=None):
    cluster = lookup_and_validate_cluster(cluster_id)
    force_primary_server = None
    # validate force primary
    if force_primary_server_id:
        force_primary_server = \
            lookup_and_validate_server(force_primary_server_id)

    configure_replica_cluster(cluster,force_primary_server=
                                       force_primary_server)

###############################################################################
def configure_replica_cluster(replica_cluster, force_primary_server=None):
    replica_cluster.configure_replicaset(force_primary_server=
                                           force_primary_server)

###############################################################################
def dry_run_configure_cluster(cluster_id, force_primary_server_id=None):
    cluster = lookup_and_validate_cluster(cluster_id)
    log_info("\n************ Dry Run ************\n")
    db_command = None
    force = force_primary_server_id is not None
    if cluster.is_replicaset_initialized():
        log_info("Replica set already initialized. "
                 "Making the replSetReconfig command...")
        db_command = cluster.get_replicaset_reconfig_db_command(force=force)
    else:
        log_info("Replica set has not yet been initialized."
                 " Making the replSetInitiate command...")
        db_command = cluster.get_replicaset_init_all_db_command()

    log_info("Executing the following command on the current primary:")
    log_info(document_pretty_string(db_command))






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

    server = lookup_server(id)
    if server:
        mongo_dump_server(server, database=database, username=username,
                          password=password, dump_options=dump_options)
        return
    else:
        cluster = lookup_cluster(id)
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

    server_or_cluster = build_server_or_cluster_from_uri(uri)

    if type(server_or_cluster) == Server:
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
    validate_server(server)

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
                  server_version=server.get_mongo_version(),
                  dump_options=dump_options)

###############################################################################
def mongo_dump_cluster(cluster,
                       database=None,
                       username=None,
                       password=None,
                       use_best_secondary=False,
                       max_repl_lag=False,
                       dump_options=None):
    validate_cluster(cluster)

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
    log_info("Locating primary server for cluster '%s'..." % cluster.get_id())
    primary_member = cluster.get_primary_member()
    if primary_member:
        primary_server = primary_member.get_server()
        log_info("Dumping current primary '%s'..." % primary_server.get_id())
        mongo_dump_server(primary_server,
            database=database,
            username=username,
            password=password,
            dump_options=dump_options)
    else:
        raise MongoctlException("No primary found for cluster '%s'" %
                                cluster.get_id())


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
             (cluster.get_id(), max_repl_lag))
    best_secondary = cluster.get_dump_best_secondary(max_repl_lag=max_repl_lag)
    if best_secondary:
        server = best_secondary.get_server()

        log_info("Found secondary server '%s'. Dumping..." % server.get_id())
        mongo_dump_server(server, database=database, username=username,
                          password=password, dump_options=dump_options)
    else:
        raise MongoctlException("No secondary server found for cluster '%s'" %
                                cluster.get_id())

###############################################################################
def do_mongo_dump(host=None,
                  port=None,
                  dbpath=None,
                  database=None,
                  username=None,
                  password=None,
                  server_version=None,
                  dump_options=None):


    # create dump command with host and port
    dump_cmd = [get_mongo_dump_executable(server_version)]

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

    # ignore authenticationDatabase option is server_version is less than 2.4.0
    if (dump_options and "authenticationDatabase" in dump_options and
        server_version and
        version_obj(server_version) < MongoctlNormalizedVersion("2.4.0")):
        dump_options.pop("authenticationDatabase", None)

    # append shell options
    if dump_options:
        dump_cmd.extend(options_to_command_args(dump_options))


    cmd_display =  dump_cmd[:]
    # mask user/password
    if username:
        cmd_display[cmd_display.index("-u") + 1] =  "****"
        if password:
            cmd_display[cmd_display.index("-p") + 1] =  "****"



    log_info("Executing command: \n%s" % " ".join(cmd_display))
    call_command(dump_cmd, bubble_exit_code=True)

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

    server = lookup_server(id)
    if server:
        mongo_restore_server(server, source, database=database,
                             username=username, password=password,
                             restore_options=restore_options)
        return
    else:
        cluster = lookup_cluster(id)
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

    server_or_cluster = build_server_or_cluster_from_uri(uri)

    if type(server_or_cluster) == Server:
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
    validate_server(server)

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
                     server_version=server.get_mongo_version(),
                     restore_options=restore_options)

###############################################################################
def mongo_restore_cluster(cluster, source,
                          database=None,
                          username=None,
                          password=None,
                          restore_options=None):
    validate_cluster(cluster)
    log_info("Locating primary server for cluster '%s'..." % cluster.get_id())
    primary_member = cluster.get_primary_member()
    if primary_member:
        primary_server = primary_member.get_server()
        log_info("Restoring primary server '%s'" % primary_server.get_id())
        mongo_restore_server(primary_server, source,
                          database=database,
                          username=username,
                          password=password,
                          restore_options=restore_options)
    else:
        raise MongoctlException("No primary server found for cluster '%s'" %
                                cluster.get_id())

###############################################################################
def do_mongo_restore(source,
                     host=None,
                     port=None,
                     dbpath=None,
                     database=None,
                     username=None,
                     password=None,
                     server_version=None,
                     restore_options=None):


    # create restore command with host and port
    restore_cmd = [get_mongo_restore_executable(server_version)]

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

    # ignore authenticationDatabase option is server_version is less than 2.4.0
    if (restore_options and "authenticationDatabase" in restore_options and
        server_version and
        version_obj(server_version) < MongoctlNormalizedVersion("2.4.0")):
        restore_options.pop("authenticationDatabase", None)

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
# install_mongodb
###############################################################################
def install_mongodb(version):

    bits = platform.architecture()[0].replace("bit", "")
    os_name = platform.system().lower()

    if os_name == 'darwin' and platform.mac_ver():
        os_name = "osx"

    return do_install_mongodb(os_name, bits, version)

###############################################################################
def do_install_mongodb(os_name, bits, version):

    if version is None:
        version = fetch_latest_stable_version()
        log_info("Installing latest stable MongoDB version '%s'..." % version)
    # validate version string
    elif not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongodb_installs_dir = get_mongodb_installs_dir()
    if not mongodb_installs_dir:
        raise MongoctlException("No mongoDBInstallationsDirectory configured"
                                " in mongoctl.config")

    platform_spec = get_validate_platform_spec(os_name, bits)

    log_info("Running install for %s %sbit to "
             "mongoDBInstallationsDirectory (%s)..." % (os_name, bits,
                                                  mongodb_installs_dir))


    mongo_installation = get_mongo_installation(version)

    if mongo_installation is not None: # no-op
        log_info("You already have MongoDB %s installed ('%s'). "
                 "Nothing to do." % (version, mongo_installation))
        return mongo_installation

    archive_name = "mongodb-%s-%s.tgz" % (platform_spec, version)
    url = "http://fastdl.mongodb.org/%s/%s" % (os_name, archive_name)

    # Validate if the version exists
    response = urllib.urlopen(url)

    if response.getcode() != 200:
        msg = ("Unable to download from url '%s' (response code '%s'). "
               "It could be that version '%s' you specified does not exist."
               " Please double check the version you provide" %
               (url, response.getcode(), version))
        raise MongoctlException(msg)

    mongo_dir_name = "mongodb-%s-%s" % (platform_spec, version)
    install_dir = os.path.join(mongodb_installs_dir, mongo_dir_name)

    ensure_dir(mongodb_installs_dir)

    # XXX LOOK OUT! Two processes installing same version simultaneously => BAD.
    # TODO: mutex to protect the following

    if not dir_exists(install_dir):
        try:
            ## download the url
            download(url)
            extract_archive(archive_name)

            log_info("Moving extracted folder to %s" % mongodb_installs_dir)
            shutil.move(mongo_dir_name, mongodb_installs_dir)

            os.remove(archive_name)
            log_info("Deleting archive %s" % archive_name)

            log_info("MongoDB %s installed successfully!" % version)
            return install_dir
        except Exception, e:
            log_error("Failed to install MongoDB '%s'. Cause: %s" %
                      (version, e))

###############################################################################
# uninstall_mongodb
###############################################################################
def uninstall_mongodb(version):

    # validate version string
    if not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongo_installation = get_mongo_installation(version)

    if mongo_installation is None: # no-op
        msg = ("Cannot find a MongoDB installation for version '%s'. Please"
               " use list-versions to see all possible versions " % version)
        log_info(msg)
        return

    log_info("Found MongoDB '%s' in '%s'" % (version, mongo_installation))

    def rm_mongodb():
        log_info("Deleting '%s'" % mongo_installation)
        shutil.rmtree(mongo_installation)
        log_info("MongoDB '%s' Uninstalled successfully!" % version);

    prompt_execute_task("Proceed uninstall?" , rm_mongodb)

###############################################################################
def fetch_latest_stable_version():
    response = urllib.urlopen(LATEST_VERSION_FILE_URL)
    if response.getcode() == 200:
        return response.read().strip()
    else:
        raise MongoctlException("Unable to fetch MongoDB latest stable version"
                                " from '%s' (Response code %)" %
                                (LATEST_VERSION_FILE_URL, response.getcode()))
###############################################################################
def get_mongo_installation(version_str):
    # get all mongod installation dirs and return the one
    # whose version == specified version. If any...
    version = version_obj(version_str)
    for install_dir, install_version in find__all_mongo_installations():
        if install_version == version:
            return install_dir

    return None

###############################################################################
def find__all_mongo_installations():
    all_installs = []
    all_mongod_exes = find_all_executables('mongod')
    for exe_path, exe_version in all_mongod_exes:
        # install dir is exe parent's (bin) parent
        install_dir = os.path.dirname(os.path.dirname(exe_path))
        all_installs.append((install_dir,exe_version))

    return all_installs

###############################################################################
def get_validate_platform_spec(os_name, bits):

    if os_name not in ["linux", "osx", "win32", "sunos5"]:
        raise MongoctlException("Unsupported OS %s" % os_name)

    if bits == "64":
        return "%s-x86_64" % os_name
    else:
        if os_name == "linux":
            return "linux-i686"
        elif os_name in ["osx" , "win32"]:
            return "%s-i386" % os_name
        elif os_name == "sunos5":
            return "i86pc"

###############################################################################
def download(url):
    log_info("Downloading %s..." % url)

    if which("curl"):
        download_cmd = ['curl', '-O']
        if not is_interactive_mode():
            download_cmd.append('-Ss')
    elif which("wget"):
        download_cmd = ['wget']
    else:
        msg = ("Cannot download file.You need to have 'curl' or 'wget"
               "' command in your path in order to proceed.")
        raise MongoctlException(msg)

    download_cmd.append(url)
    call_command(download_cmd)

###############################################################################
def extract_archive(archive_name):
    log_info("Extracting %s..." % archive_name)
    if not which("tar"):
        msg = ("Cannot extract archive.You need to have 'tar' command in your"
               " path in order to proceed.")
        raise MongoctlException(msg)

    tar_cmd = ['tar', 'xvf', archive_name]
    call_command(tar_cmd)


###############################################################################
def is_forking(server, options_override):
    fork = server.is_fork()
    if options_override is not None:
        fork = options_override.get('fork', fork)

    if fork is None:
        fork = True

    return fork

###############################################################################
def is_server_or_cluster_db_address(value):
    """
    checks if the specified value is in the form of
    [server or cluster id][/database]
    """
    # check if value is an id string
    id_path = value.split("/")
    id = id_path[0]
    return len(id_path) <= 2 and (lookup_server(id) or lookup_cluster(id))

###############################################################################
def is_db_address(value):
    """
    Checks if the specified value is a valid mongoctl database address
    """
    return value and (is_mongo_uri(value) or
                      is_server_or_cluster_db_address(value))


###############################################################################
def is_dbpath(value):
    """
    Checks if the specified value is a dbpath. dbpath could be an absolute
    file path, relative path or a file uri
    """

    value = resolve_path(value)
    return os.path.exists(value)


###############################################################################
def get_mongo_dump_executable(server_version):
    dump_exe = get_mongo_executable(server_version,
                                    'mongodump',
                                    version_check_pref=
                                    VERSION_PREF_EXACT_OR_MINOR)
    # Warn the user if it is not an exact match (minor match)
    if server_version and version_obj(server_version) != dump_exe.version:
        log_warning("Using mongodump '%s' that does not exactly match "
                    "server version '%s'" % (dump_exe.version, server_version))

    return dump_exe.path

###############################################################################
def get_mongo_restore_executable(server_version):
    restore_exe = get_mongo_executable(server_version,
                                       'mongorestore',
                                       version_check_pref=
                                        VERSION_PREF_EXACT_OR_MINOR)
    # Warn the user if it is not an exact match (minor match)
    if server_version and version_obj(server_version) != restore_exe.version:
        log_warning("Using mongorestore '%s' that does not exactly match"
                    "server version '%s'" % (restore_exe.version,
                                             server_version))

    return restore_exe.path


###############################################################################
def prepare_server(server):
    """
     Contains post start server operations
    """
    log_info("Preparing server '%s' for use as configured..." %
             server.get_id())

    # setup the local users
    setup_server_local_users(server)

    if not server.is_cluster_member():
        setup_server_users(server)












###############################################################################
# Utility Methods
###############################################################################

###############################################################################
def namespace_get_property(namespace, name):
    if hasattr(namespace, name):
        return getattr(namespace,name)

    return None

###############################################################################
def parse_global_login_user_arg(parsed_args):
    username = namespace_get_property(parsed_args, "username")

    # if -u or --username  was not specified then nothing to do
    if not username:
        return
    password = namespace_get_property(parsed_args, "password")
    server_id = namespace_get_property(parsed_args,SERVER_ID_PARAM)

    global __global_login_user__
    __global_login_user__['serverId'] = server_id
    __global_login_user__['username'] = username
    __global_login_user__['password'] = password


###############################################################################
########################                      #################################
########################  Commandline parsing #################################
########################                      #################################
###############################################################################


###############################################################################
# Mongoctl main parser
###############################################################################


###############################################################################
def get_mongoctl_cmd_parser():
    parser = dargparse.build_parser(MONGOCTL_PARSER_DEF)
    return parser

###############################################################################
def extract_mongod_options(parsed_args):
    return extract_mongo_exe_options(parsed_args, SUPPORTED_MONGOD_OPTIONS)



###############################################################################
def extract_mongo_dump_options(parsed_args):
    return extract_mongo_exe_options(parsed_args,
                                     SUPPORTED_MONGO_DUMP_OPTIONS)

###############################################################################
def extract_mongo_restore_options(parsed_args):
    return extract_mongo_exe_options(parsed_args,
        SUPPORTED_MONGO_RESTORE_OPTIONS)

###############################################################################
def extract_mongo_exe_options(parsed_args, supported_options):
    options_extract = {}

    # Iterating over parsed options dict
    # Yeah in a hacky way since there is no clean documented way of doing that
    # See http://bugs.python.org/issue11076 for more details
    # this should be changed when argparse provides a cleaner way

    for (option_name,option_val) in parsed_args.__dict__.items():
        if option_name in supported_options and option_val is not None:
            options_extract[option_name] = option_val

    return options_extract

###############################################################################
# Supported mongod command-line options/flags
###############################################################################

SUPPORTED_MONGOD_OPTIONS = [
    "verbose",
    "quiet",
    "port",
    "bind_ip",
    "maxConns",
    "objcheck",
    "logpath",
    "logappend",
    "pidfilepath",
    "keyFile",
    "nounixsocket",
    "unixSocketPrefix",
    "fork",
    "auth",
    "cpu",
    "dbpath",
    "diaglog",
    "directoryperdb",
    "journal",
    "journalOptions",
    "journalCommitInterval",
    "ipv6",
    "jsonp",
    "noauth",
    "nohttpinterface",
    "nojournal",
    "noprealloc",
    "notablescan",
    "nssize",
    "profile",
    "quota",
    "quotaFiles",
    "rest",
    "repair",
    "repairpath",
    "slowms",
    "smallfiles",
    "syncdelay",
    "sysinfo",
    "upgrade",
    "fastsync",
    "oplogSize",
    "master",
    "slave",
    "source",
    "only",
    "slavedelay",
    "autoresync",
    "replSet",
    "configsvr",
    "shardsvr",
    "noMoveParanoia",
    "setParameter"
]




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
    "authenticationDatabase"
]

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
    "authenticationDatabase"
]

###############################################################################
########################                   ####################################
########################     BOOTSTRAP     ####################################
########################                   ####################################
###############################################################################

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (SystemExit, KeyboardInterrupt) , e:
        if e.code == 0:
            pass
        else:
            raise
    except:
        traceback.print_exc()
