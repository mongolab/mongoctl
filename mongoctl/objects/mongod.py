__author__ = 'abdul'


import mongoctl.repository as repository
import server

from mongoctl.utils import resolve_path
from mongoctl.mongoctl_logging import log_verbose

from bson.son import SON
from mongoctl.errors import MongoctlException
from cluster import get_member_repl_lag

###############################################################################
# CONSTANTS
###############################################################################

# This is mongodb's default dbpath
DEFAULT_DBPATH='/data/db'

LOCK_FILE_NAME = "mongod.lock"

###############################################################################
# MongodServer Class
###############################################################################

class MongodServer(server.Server):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        super(MongodServer, self).__init__(server_doc)

    ###########################################################################
    # Properties
    ###########################################################################

    def get_db_path(self):
        dbpath = self.get_cmd_option("dbpath")
        if dbpath is None:
            dbpath = DEFAULT_DBPATH

        return resolve_path(dbpath)

    ###########################################################################
    def get_root_dir(self):
        """
            Override!
        :return:
        """
        return self.get_db_path()

    ###########################################################################
    def export_cmd_options(self, options_override=None):
        """
            Override!
        :return:
        """
        cmd_options = super(MongodServer, self).export_cmd_options(
            options_override=options_override)

        # reset some props to exporting vals
        cmd_options['dbpath'] = self.get_db_path()

        if 'repairpath' in cmd_options:
            cmd_options['repairpath'] = resolve_path(cmd_options['repairpath'])

        # Add ReplicaSet args if a cluster is configured

        cluster = self.get_cluster()
        if cluster is not None:
            if "replSet" not in cmd_options:
                cmd_options["replSet"] = cluster.id

        # add configSvr
        if self.is_config_server():
            cmd_options["configsvr"] = True

        return cmd_options

    ###########################################################################
    def get_seed_users(self):
        """
            Override!
        :return:
        """
        seed_users = super(MongodServer, self).get_seed_users()
        # exempt database users for config servers
        if seed_users and self.is_config_server():
            for dbname in seed_users.keys():
                if dbname not in ["admin", "local", "config"]:
                    del seed_users[dbname]

        return seed_users

    ###########################################################################
    def get_lock_file_path(self):
        return self.get_default_file_path(LOCK_FILE_NAME)


    ###########################################################################
    def is_master(self):
        return self.get_cmd_option("master")

    ###########################################################################
    def is_slave(self):
        return self.get_cmd_option("slave")

    ###########################################################################
    def is_auth(self):
        if self.get_cmd_option("auth") or self.get_cmd_option("keyFile"):
            return True
        else:
            cluster = self.get_cluster()
            if cluster:
                return cluster.get_repl_key() is not None

    ###########################################################################
    def set_auth(self,auth):
        self.set_cmd_option("auth", auth)


    ###########################################################################
    def get_cluster(self):
        return repository.lookup_cluster_by_server(self)

    ###########################################################################
    def is_cluster_member(self):
        return self.get_cluster() is not None

    ###########################################################################
    def is_config_server(self):
        return self.get_cmd_option("configsvr")

    ###########################################################################
    def is_administrable(self):
        return not self.is_arbiter_server() and self.can_function()

    ###########################################################################
    def get_status(self, admin=False):

        # get status for super
        status = super(MongodServer, self).get_status(admin=admin)

        if "error" not in status and admin and not self.is_arbiter_server():
            rs_summary = self.get_rs_status_summary()
            if rs_summary:
                status["selfReplicaSetStatusSummary"] = rs_summary

        return status

    ###########################################################################
    def get_server_status_summary(self):
        server_status = self.db_command(SON([('serverStatus', 1)]), "admin")
        server_summary = {
            "host": server_status['host'],
            "connections": server_status['connections'],
            "version": server_status['version']
        }
        return server_summary

    ###########################################################################
    def get_rs_status_summary(self):
        if self.is_cluster_member():
            member_rs_status = self.get_member_rs_status()
            if member_rs_status:
                return {
                    "name": member_rs_status['name'],
                    "stateStr": member_rs_status['stateStr']
                }

    ###########################################################################
    def is_arbiter_server(self):
        cluster = repository.lookup_cluster_by_server(self)
        if cluster is not None:
            return cluster.get_member_for(self).is_arbiter()
        else:
            return False

    ###########################################################################
    def is_config_server(self):
        lookup_type = repository.LOOKUP_TYPE_CONFIG_SVR
        cluster = repository.lookup_cluster_by_server(self,
                                                      lookup_type=lookup_type)
        return cluster is not None

    ###########################################################################
    def is_shard_server(self):
        lookup_type = repository.LOOKUP_TYPE_SHARDS
        cluster = repository.lookup_cluster_by_server(self,
                                                      lookup_type=lookup_type)
        return cluster is not None

    ###########################################################################
    def command_needs_auth(self, dbname, cmd):
        # isMaster command does not need auth
        if "isMaster" in cmd or "ismaster" in cmd:
            return False
        if 'shutdown' in cmd and self.is_arbiter_server():
            return False

        # otherwise use default behavior
        return super(MongodServer, self).command_needs_auth(dbname, cmd)

    ###########################################################################
    def get_mongo_uri_template(self, db=None):
        if not db:
            if self.is_auth():
                db = "/[dbname]"
            else:
                db = ""
        else:
            db = "/" + db

        creds = "[dbuser]:[dbpass]@" if self.is_auth() else ""
        return "mongodb://%s%s%s" % (creds, self.get_address_display(), db)

    ###########################################################################
    def get_rs_config(self):
        try:
            return self.get_db('local')['system.replset'].find_one()
        except (Exception,RuntimeError), e:
            if type(e) == MongoctlException:
                raise e
            else:
                log_verbose("Cannot get rs config from server '%s'. "
                            "cause: %s" % (self.id, e))
                return None

    ###########################################################################
    def get_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status =  self.db_command(rs_status_cmd, 'admin')
            return rs_status
        except (Exception,RuntimeError), e:
            log_verbose("Cannot get rs status from server '%s'. cause: %s" %
                        (self.id, e))
            return None

    ###########################################################################
    def get_member_rs_status(self):
        rs_status =  self.get_rs_status()
        if rs_status:
            try:
                for member in rs_status['members']:
                    if 'self' in member and member['self']:
                        return member
            except (Exception,RuntimeError), e:
                log_verbose("Cannot get member rs status from server '%s'. cause: %s" %
                            (self.id, e))
                return None

    ###########################################################################
    def is_primary(self):
        master_result = self.is_master_command()

        if master_result:
            return master_result.get("ismaster")

    ###########################################################################
    def is_secondary(self):
        master_result = self.is_master_command()

        if master_result:
            return master_result.get("secondary")

    ###########################################################################
    def is_master_command(self):
        try:
            if self.is_online():
                result = self.db_command({"isMaster" : 1}, "admin")
                return result

        except(Exception, RuntimeError),e:
            log_verbose("isMaster command failed on server '%s'. Cause %s" %
                        (self.id, e))

    ###########################################################################
    def read_replicaset_name(self):
        master_result = self.is_master_command()
        if master_result:
            return "setName" in master_result and master_result["setName"]

    ###########################################################################
    def get_repl_lag(self, master_status):
        """
            Given two 'members' elements from rs.status(),
            return lag between their optimes (in secs).
        """
        member_status = self.get_member_rs_status()

        if not member_status:
            raise MongoctlException("Unable to determine replicaset status for"
                                    " member '%s'" %
                                    self.id)

        return get_member_repl_lag(member_status, master_status)


    ###########################################################################
    def needs_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        return (self.supports_repl_key() and
                self.is_auth() and self.is_cluster_member())


