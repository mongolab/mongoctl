__author__ = 'abdul'

import os

import mongoctl.repository as repository

from base import DocumentWrapper
from mongoctl.utils import resolve_path, document_pretty_string, is_host_local
from pymongo.errors import AutoReconnect, ConnectionFailure
from mongoctl.mongoctl_logging import (
    log_verbose, log_error, log_warning, log_exception, log_debug
    )
from mongoctl.mongodb_version import make_version_info

from mongoctl.config import get_default_users
from mongoctl.errors import MongoctlException
from mongoctl.prompt import read_username, read_password

from bson.son import SON

from pymongo.connection import Connection
from pymongo.errors import OperationFailure

import datetime

from mongoctl import config
from mongoctl import users
from mongoctl.mongodb_version import MongoDBEdition

###############################################################################
# CONSTANTS
###############################################################################

# default pid file name
PID_FILE_NAME = "pid.txt"

LOG_FILE_NAME = "mongodb.log"

KEY_FILE_NAME = "keyFile"

# This is mongodb's default port
DEFAULT_PORT = 27017

# db connection timeout, 10 seconds
CONN_TIMEOUT = 10000


REPL_KEY_SUPPORTED_VERSION = '2.0.0'

# CLIENT_SSL_MODE global flag to for turning off ssl
# TODO this is temporary and should be deleted
CLIENT_SSL_MODE = None

# A global config that is set through --use-alt-address option that will use
# a different "address" property of when making connections to servers
USE_ALT_ADDRESS = None

###############################################################################
class ClientSslMode(object):
    DISABLED = "disabled"
    ALLOW = "allow"
    REQUIRE = "require"
    PREFER = "prefer"

###############################################################################
# Server Class
###############################################################################

class Server(DocumentWrapper):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        DocumentWrapper.__init__(self, server_doc)
        self._db_connection = None
        self._seed_users = None
        self._mongo_version = None
        self._mongodb_edition = None
        self._cluster = None
        self._connection_address = None

    ###########################################################################
    # Properties
    ###########################################################################

    ###########################################################################
    def get_description(self):
        return self.get_ignore_str_property("description")

    ###########################################################################
    def set_description(self, desc):
        return self.set_property("description", desc)

    ###########################################################################
    def get_server_home(self):
        home_dir = self.get_property("serverHome")
        if home_dir:
            return resolve_path(home_dir)
        else:
            return None

    ###########################################################################
    def set_server_home(self, val):
        self.set_property("serverHome", val)

    ###########################################################################
    def get_pid_file_path(self):
        return self.get_server_file_path("pidfilepath", PID_FILE_NAME)

    ###########################################################################
    def get_log_file_path(self):
        return self.get_server_file_path("logpath", LOG_FILE_NAME)

    ###########################################################################
    def get_key_file(self):
        kf = self.get_cmd_option("keyFile")
        if kf:
            return resolve_path(kf)

    ###########################################################################
    def get_client_ssl_mode(self):
        mode = CLIENT_SSL_MODE
        ssl_option = self.get_cmd_option("sslMode")

        if not mode and ssl_option:
            if ssl_option == "requireSSL":
                mode = ClientSslMode.REQUIRE
            elif ssl_option in ["preferSSL", "allowSSL"]:
                mode = ClientSslMode.PREFER
            elif ssl_option == "disabled":
                mode = ClientSslMode.DISABLED

        return mode

    ###########################################################################
    def use_ssl_client(self):
        return (self.get_client_ssl_mode() == ClientSslMode.REQUIRE or
                self.prefer_use_ssl())

    ###########################################################################
    def prefer_use_ssl(self):
        if self.get_client_ssl_mode() != ClientSslMode.PREFER:
            return False

        log_debug("prefer_use_ssl() Checking if we prefer ssl for '%s'" %
                  self.id)
        try:
            self.make_ssl_db_connection(self.get_connection_address())
            return True
        except Exception, e:
            if not "SSL handshake failed" in str(e):
                log_exception(e)
            return None





    ###########################################################################
    def ssl_key_file(self):
        return self.get_cmd_option("sslKeyFile") is not None

    ###########################################################################
    def ssl_cert_file(self):
        return self.get_cmd_option("sslCertFile") is not None

    ###########################################################################
    def get_default_key_file_path(self):
        return self.get_server_file_path("keyFile", KEY_FILE_NAME)


    ###########################################################################
    def get_server_file_path(self, cmd_prop, default_file_name):
        file_path = self.get_cmd_option(cmd_prop)
        if file_path is not None:
            return resolve_path(file_path)
        else:
            return self.get_default_file_path(default_file_name)

    ###########################################################################
    def get_default_file_path(self, file_name):
        return self.get_server_home() + os.path.sep + file_name

    ###########################################################################
    def get_address(self):
        address = self.get_property("address")

        if USE_ALT_ADDRESS:
            address = self.get_property(USE_ALT_ADDRESS)
            if not address:
                raise MongoctlException(
                    "No alternative address '%s' found in server '%s'" %
                    (USE_ALT_ADDRESS, self.id))

        if address is not None:
            if address.find(":") > 0:
                return address
            else:
                return "%s:%s" % (address, self.get_port())
        else:
            return None

    ###########################################################################
    def get_address_display(self):
        display = self.get_address()
        if display is None:
            display = self.get_local_address()
        return display

    ###########################################################################
    def get_host_address(self):
        if self.get_address() is not None:
            return self.get_address().split(":")[0]
        else:
            return None

    ###########################################################################
    def get_connection_host_address(self):
        return self.get_connection_address().split(":")[0]

    ###########################################################################
    def set_address(self, address):
        self.set_property("address", address)

    ###########################################################################
    def get_local_address(self):
        return "localhost:%s" % self.get_port()

    ###########################################################################
    def get_port(self):
        port = self.get_cmd_option("port")
        if port is None:
            port = DEFAULT_PORT
        return port

    ###########################################################################
    def set_port(self, port):
        self.set_cmd_option("port", port)

    ###########################################################################
    def is_fork(self):
        fork = self.get_cmd_option("fork")
        return fork or fork is None


    ###########################################################################
    def is_auth(self):
        if self.get_cmd_option("auth") or self.get_cmd_option("keyFile"):
            return True
        else:
            cluster = self.get_cluster()
            if cluster:
                return cluster.get_repl_key() is not None

    ###########################################################################
    def get_mongo_version(self):
        """
        Gets mongo version of the server if it is running. Otherwise return
         version configured in mongoVersion property
        """
        if self._mongo_version:
            return self._mongo_version

        if self.is_online():
            mongo_version = self.get_db_connection().server_info()['version']
        else:
            mongo_version = self.get_property("mongoVersion")

        self._mongo_version = mongo_version
        return self._mongo_version


    ###########################################################################
    def get_mongodb_edition(self):

        if self._mongodb_edition:
            return self._mongodb_edition

        if self.is_online():
            server_info = self.get_db_connection().server_info()
            if ("gitVersion" in server_info and
                    ("subscription" in server_info["gitVersion"] or
                     "enterprise" in server_info["gitVersion"])):
                edition = MongoDBEdition.ENTERPRISE
            elif ("OpenSSLVersion" in server_info and
                    server_info["OpenSSLVersion"]):
                edition = MongoDBEdition.COMMUNITY_SSL
            else:
                edition = MongoDBEdition.COMMUNITY
        else:
            edition = self.get_property("mongoEdition")

        self._mongodb_edition = edition

        return self._mongodb_edition

    ###########################################################################
    def get_mongo_version_info(self):
        version_number = self.get_mongo_version()
        if version_number is not None:
            return make_version_info(version_number,
                                     edition=self.get_mongodb_edition())
        else:
            return None

    ###########################################################################
    def get_cmd_option(self, option_name):
        cmd_options = self.get_cmd_options()

        if cmd_options and cmd_options.has_key(option_name):
            return cmd_options[option_name]
        else:
            return None

    ###########################################################################
    def set_cmd_option(self, option_name, option_value):
        cmd_options = self.get_cmd_options()

        if cmd_options:
            cmd_options[option_name] = option_value

    ###########################################################################
    def get_cmd_options(self):
        return self.get_property('cmdOptions')

    ###########################################################################
    def set_cmd_options(self, cmd_options):
        return self.set_property('cmdOptions' , cmd_options)

    ###########################################################################
    def export_cmd_options(self, options_override=None):
        cmd_options =  self.get_cmd_options().copy()
        # reset some props to exporting vals
        cmd_options['pidfilepath'] = self.get_pid_file_path()

            # apply the options override
        if options_override is not None:
            for (option_name, option_val) in options_override.items():
                cmd_options[option_name] = option_val

        # set the logpath if forking..

        if (self.is_fork() or (options_override is not None and
                               options_override.get("fork"))):
            cmd_options['fork'] = True
            if "logpath" not in cmd_options:
                cmd_options["logpath"] = self.get_log_file_path()

        # Specify the keyFile arg if needed
        if self.needs_repl_key() and "keyFile" not in cmd_options:
            key_file_path = (self.get_key_file() or
                             self.get_default_key_file_path())
            cmd_options["keyFile"] = key_file_path
        return cmd_options

    ###########################################################################
    def get_seed_users(self):

        if self._seed_users is None:
            seed_users = self.get_property('seedUsers')

            ## This hidden for internal user and should not be documented
            if not seed_users:
                seed_users = get_default_users()

            self._seed_users = seed_users

        return self._seed_users

    ###########################################################################
    def get_login_user(self, dbname):
        login_user =  users.get_server_login_user(self, dbname)
        # if no login user found then check global login

        if not login_user:
            login_user = users.get_global_login_user(self, dbname)

        # if dbname is local and we cant find anything yet
        # THEN assume that local credentials == admin credentials
        if not login_user and dbname == "local":
            login_user = self.get_login_user("admin")

        return login_user

    ###########################################################################
    def lookup_password(self, dbname, username):
        # look in seed users
        db_seed_users = self.get_db_seed_users(dbname)
        if db_seed_users:
            user = filter(lambda user: user['username'] == username,
                        db_seed_users)
            if user and "password" in user[0]:
                return user[0]["password"]

    ###########################################################################
    def set_login_user(self, dbname, username, password):
        users.set_server_login_user(self, dbname, username, password)

    ###########################################################################
    def get_admin_users(self):
        return self.get_db_seed_users("admin")

    ###########################################################################
    def get_db_seed_users(self, dbname):
        return self.get_seed_users().get(dbname)

    ###########################################################################
    def get_cluster(self):
        if self._cluster is None:
            self._cluster = repository.lookup_cluster_by_server(self)
        return self._cluster

    ###########################################################################
    def get_validate_cluster(self):
        cluster = self.get_cluster()
        if not cluster:
            raise MongoctlException("No cluster found for server '%s'" %
                                    self.id)
        repository.validate_cluster(cluster)
        return cluster

    ###########################################################################
    def is_cluster_member(self):
        return self.get_cluster() is not None

    ###########################################################################
    def is_cluster_connection_member(self):
        """
        Override!
        :return: true if the server should be included in a cluster connection
        """
        pass

    ###########################################################################
    # DB Methods
    ###########################################################################

    def disconnecting_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except AutoReconnect,e:
            log_verbose("This is an expected exception that happens after "
                        "disconnecting db commands: %s" % e)
        finally:
            self._db_connection = None

    ###########################################################################
    def timeout_maybe_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except Exception, e:
            log_exception(e)
            if "timed out" in str(e):
                log_warning("Command %s is taking a while to complete. "
                            "This is not necessarily bad. " %
                            document_pretty_string(cmd))
            else:
                raise
        finally:
            self._db_connection = None

    ###########################################################################
    def db_command(self, cmd, dbname):

        need_auth = self.command_needs_auth(dbname, cmd)
        db = self.get_db(dbname, no_auth=not need_auth)
        return db.command(cmd)

    ###########################################################################
    def command_needs_auth(self, dbname, cmd):
        return self.needs_to_auth(dbname)

    ###########################################################################
    def get_db(self, dbname, no_auth=False, username=None, password=None,
               retry=True, never_auth_with_admin=False):

        conn = self.get_db_connection()
        db = conn[dbname]

        # If the DB doesn't need to be authenticated to (or at least yet)
        # then don't authenticate. this piece of code is important for the case
        # where you are connecting to the DB on local host where --auth is on
        # but there are no admin users yet
        if no_auth:
            return db

        if (not username and
                (not self.needs_to_auth(dbname))):
            return db

        if username:
            self.set_login_user(dbname, username, password)

        login_user = self.get_login_user(dbname)

        is_system_user = (login_user and
                          login_user.get("username") == "__system")
        # if there is no login user for this database then use admin db unless
        # it was specified not to
        # ALSO use admin if this is 'local' db for mongodb >= 2.6.0
        if ((not never_auth_with_admin and
             not login_user and
             dbname != "admin")
            or
            (dbname == "local" and
             not is_system_user and
             not users.server_supports_local_users(self))):
            # if this passes then we are authed!
            admin_db = self.get_db("admin", retry=retry)
            return admin_db.connection[dbname]

        # no retries on local db, so if we fail to auth to local we always
        # attempt to use admin
        retry = retry and dbname != "local"
        auth_success = self.authenticate_db(db, dbname, retry=retry)

        # If auth failed then give it a try by auth into admin db unless it
        # was specified not to
        if (not never_auth_with_admin and
                not auth_success
            and dbname != "admin"):
            admin_db = self.get_db("admin", retry=retry)
            return admin_db.connection[dbname]

        if auth_success:
            return db
        else:
            raise MongoctlException("Failed to authenticate to %s db" % dbname)

    ###########################################################################
    def authenticate_db(self, db, dbname, retry=True):
        """
        Returns True if we manage to auth to the given db, else False.
        """
        login_user = self.get_login_user(dbname)
        username = None
        password = None


        auth_success = False

        if login_user:
            username = login_user["username"]
            if "password" in login_user:
                password = login_user["password"]

        # have three attempts to authenticate
        no_tries = 0

        while not auth_success and no_tries < 3:
            if not username:
                username = read_username(dbname)
            if not password:
                password = self.lookup_password(dbname, username)
                if not password:
                    password = read_password("Enter password for user '%s\%s'"%
                                             (dbname, username))

            # if auth success then exit loop and memoize login
            try:
                auth_success = db.authenticate(username, password)
            except OperationFailure, ofe:
                if "auth fails" in str(ofe):
                    auth_success = False

            if auth_success or not retry:
                break
            else:
                log_error("Invalid login!")
                username = None
                password = None

            no_tries += 1

        if auth_success:
            self.set_login_user(dbname, username, password)

        return auth_success

    ###########################################################################
    def get_working_login(self, database, username=None, password=None):
        """
            authenticate to the specified database starting with specified
            username/password (if present), try to return a successful login
            within 3 attempts
        """
        login_user = None


        #  this will authenticate and update login user
        self.get_db(database, username=username, password=password,
                    never_auth_with_admin=True)

        login_user = self.get_login_user(database)

        if login_user:
            username = login_user["username"]
            password = (login_user["password"] if "password" in login_user
                        else None)
        return username, password

    ###########################################################################
    def is_online(self):
        try:
            self.new_db_connection()
            return True
        except Exception, e:
            log_exception(e)
            return False

    ###########################################################################
    def can_function(self):
        status = self.get_status()
        if status['connection']:
            if 'error' not in status:
                return True
            else:
                log_verbose("Error while connecting to server '%s': %s " %
                            (self.id, status['error']))

    ###########################################################################
    def is_online_locally(self):
        return self.is_use_local() and self.is_online()

    ###########################################################################
    def is_use_local(self):
        return (self.get_address() is None or
                is_assumed_local_server(self.id)
                or self.is_local())

    ###########################################################################
    def is_local(self):
        try:
            server_host = self.get_host_address()
            return server_host is None or is_host_local(server_host)
        except Exception, e:
            log_exception(e)
            log_error("Unable to resolve address '%s' for server '%s'."
                      " Cause: %s" %
                      (self.get_host_address(), self.id, e))
        return False

    ###########################################################################
    def needs_to_auth(self, dbname):
        """
        Determines if the server needs to authenticate to the database.
        NOTE: we stopped depending on is_auth() since its only a configuration
        and may not be accurate
        """
        log_debug("Checking if server '%s' needs to auth on  db '%s'...." %
                  (self.id, dbname))
        try:
            conn = self.new_db_connection()
            db = conn[dbname]
            db.collection_names()
            result = False
        except (RuntimeError,Exception), e:
            log_exception(e)
            result = "authorized" in str(e)

        log_debug("needs_to_auth check for server '%s'  on db '%s' : %s" %
                  (self.id, dbname, result))
        return result

    ###########################################################################
    def get_status(self, admin=False):
        status = {}
        ## check if the server is online
        try:
            self.get_db_connection()
            status['connection'] = True

            # grab status summary if it was specified + if i am not an arbiter
            if admin:
                server_summary = self.get_server_status_summary()
                status["serverStatusSummary"] = server_summary

        except (RuntimeError, Exception), e:
            log_exception(e)
            self.sever_db_connection()   # better luck next time!
            status['connection'] = False
            status['error'] = "%s" % e
            if "timed out" in status['error']:
                status['timedOut'] = True
        return status

    ###########################################################################
    def get_server_status_summary(self):
        server_status = self.db_command(SON([('serverStatus', 1)]), "admin")
        connections = server_status['connections']
        # remove totalCreated if it exists
        if "totalCreated" in connections:
            del(connections["totalCreated"])

        server_summary = {
            "host": server_status['host'],
            "connections": connections,
            "version": server_status['version']
        }
        return server_summary


    ###########################################################################
    def get_uptime(self):
        server_status = self._server_status_command()
        if server_status:
            return server_status.get("uptime")

    ###########################################################################
    def _server_status_command(self):
        if self.is_online():
            return self.db_command(SON([('serverStatus', 1)]), "admin")

    ###########################################################################
    def get_db_connection(self):
        if self._db_connection is None:
            self._db_connection  = self.new_db_connection()
        return self._db_connection

    ###########################################################################
    def sever_db_connection(self):
        if self._db_connection is not None:
            self._db_connection.close()
        self._db_connection = None

    ###########################################################################
    def new_db_connection(self):
        return self.make_db_connection(self.get_connection_address())

    ###########################################################################
    def get_connection_address(self):

        if self._connection_address:
            return self._connection_address

        # try to get the first working connection address
        if (self.is_use_local() and
                self.has_connectivity_on(self.get_local_address())):
            self._connection_address = self.get_local_address()
        elif self.has_connectivity_on(self.get_address()):
            self._connection_address = self.get_address()

        # use old logic
        if not self._connection_address:
            if self.is_use_local():
                self._connection_address = self.get_local_address()
            else:
                self._connection_address = self.get_address()

        return self._connection_address

    ###########################################################################
    def make_db_connection(self, address):

        try:

            client_ssl_mode = self.get_client_ssl_mode()
            if client_ssl_mode in [None, ClientSslMode.DISABLED]:
                return self.make_plain_db_connection(address)
            elif client_ssl_mode == ClientSslMode.REQUIRE:
                return self.make_ssl_db_connection(address)
            elif client_ssl_mode == ClientSslMode.ALLOW:
                try:
                    # attempt an ssl connection
                    return self.make_plain_db_connection(address)
                except Exception, e:
                    return self.make_ssl_db_connection(address)

            else:
                ## PREFER
                try:
                    # attempt an ssl connection
                    return self.make_ssl_db_connection(address)
                except Exception, e:
                    return self.make_plain_db_connection(address)
        except Exception, e:
            log_exception(e)
            error_msg = "Cannot connect to '%s'. Cause: %s" % \
                        (address, e)
            raise MongoctlException(error_msg,cause=e)


    ###########################################################################
    def make_ssl_db_connection(self, address):

        kwargs = {
            "ssl": True
        }

        if self.ssl_key_file():
            kwargs["ssl_keyfile"] = self.ssl_key_file()
            kwargs["ssl_certfile"] = self.ssl_cert_file()

        return self._do_make_db_connection(address, **kwargs)

    ###########################################################################
    def make_plain_db_connection(self, address):
        return self._do_make_db_connection(address)

    ###########################################################################
    def _do_make_db_connection(self, address, **kwargs):
        kwargs = kwargs or {}
        kwargs.update({
            "socketTimeoutMS": CONN_TIMEOUT,
            "connectTimeoutMS": CONN_TIMEOUT
        })

        return Connection(address, **kwargs)

    ###########################################################################
    def has_connectivity_on(self, address):

        try:
            log_verbose("Checking if server '%s' is accessible on "
                        "address '%s'" % (self.id, address))
            self.make_db_connection(address)
            return True
        except Exception, e:
            log_exception(e)
            log_verbose("Check failed for server '%s' is accessible on "
                        "address '%s': %s" % (self.id, address, e))
            return False

    ###########################################################################
    def get_rs_config(self):
        try:
            return self.get_db('local')['system.replset'].find_one()
        except (Exception,RuntimeError), e:
            log_exception(e)
            if type(e) == MongoctlException:
                raise e
            else:
                log_verbose("Cannot get rs config from server '%s'. "
                            "cause: %s" % (self.id, e))
                return None

    ###########################################################################
    def validate_local_op(self, op):

        # If the server has been assumed to be local then skip validation
        if is_assumed_local_server(self.id):
            log_verbose("Skipping validation of server's '%s' address '%s' to be"
                        " local because --assume-local is on" %
                        (self.id, self.get_host_address()))
            return

        log_verbose("Validating server address: "
                    "Ensuring that server '%s' address '%s' is local on this "
                    "machine" % (self.id, self.get_host_address()))
        if not self.is_local():
            log_verbose("Server address validation failed.")
            raise MongoctlException("Cannot %s server '%s' on this machine "
                                    "because server's address '%s' does not appear "
                                    "to be local to this machine. Pass the "
                                    "--assume-local option if you are sure that "
                                    "this server should be running on this "
                                    "machine." % (op,
                                                  self.id,
                                                  self.get_host_address()))
        else:
            log_verbose("Server address validation passed. "
                        "Server '%s' address '%s' is local on this "
                        "machine !" % (self.id, self.get_host_address()))


    ###########################################################################
    def log_server_activity(self, activity):

        if is_logging_activity():
            log_record = {"op": activity,
                          "ts": datetime.datetime.utcnow(),
                          "serverDoc": self.get_document(),
                          "server": self.id,
                          "serverDisplayName": self.get_description()}
            log_verbose("Logging server activity \n%s" %
                        document_pretty_string(log_record))

            repository.get_activity_collection().insert(log_record)

    ###########################################################################
    def needs_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        cluster = self.get_cluster()
        return (self.supports_repl_key() and
                cluster is not None and cluster.get_repl_key() is not None)

    ###########################################################################
    def supports_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        version = self.get_mongo_version_info()
        return (version is None or
                version >= make_version_info(REPL_KEY_SUPPORTED_VERSION))

    ###########################################################################
    def get_pid(self):
        pid_file_path = self.get_pid_file_path()
        if os.path.exists(pid_file_path):
            pid_file = open(pid_file_path, 'r')
            pid = pid_file.readline().strip('\n')
            if pid and pid.isdigit():
                return int(pid)
            else:
                log_warning("Unable to determine pid for server '%s'. "
                            "Not a valid number in '%s"'' %
                            (self.id, pid_file_path))
        else:
            log_warning("Unable to determine pid for server '%s'. "
                        "pid file '%s' does not exist" %
                        (self.id, pid_file_path))

        return None

###############################################################################
def is_logging_activity():
    return (repository.consulting_db_repository() and
            config.get_mongoctl_config_val("logServerActivity" , False))

###############################################################################
__assumed_local_servers__ = []

def assume_local_server(server_id):
    global __assumed_local_servers__
    if server_id not in __assumed_local_servers__:
        __assumed_local_servers__.append(server_id)

###############################################################################
def is_assumed_local_server(server_id):
    global __assumed_local_servers__
    return server_id in __assumed_local_servers__

###############################################################################
def set_client_ssl_mode(mode):
    allowed_modes = [ClientSslMode.DISABLED,
                     ClientSslMode.ALLOW,
                     ClientSslMode.REQUIRE,
                     ClientSslMode.PREFER]
    if mode not in allowed_modes:
        raise MongoctlException("Invalid ssl mode '%s'. Mush choose from %s" %
                                (mode, allowed_modes))

    global CLIENT_SSL_MODE
    CLIENT_SSL_MODE = mode
