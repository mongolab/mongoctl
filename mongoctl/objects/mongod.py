__author__ = 'abdul'



import server

from mongoctl.utils import resolve_path
from mongoctl.mongoctl_logging import log_verbose, log_debug, log_exception, \
    log_warning

from bson.son import SON
from mongoctl.errors import MongoctlException
from replicaset_cluster import ReplicaSetCluster, get_member_repl_lag
from sharded_cluster import ShardedCluster
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
        if not dbpath:
            dbpath = super(MongodServer, self).get_server_home()
        if not dbpath:
            dbpath = DEFAULT_DBPATH

        return resolve_path(dbpath)

    ###########################################################################
    def get_server_home(self):
        """
            Override!
        :return:
        """
        home_dir = super(MongodServer, self).get_server_home()
        if not home_dir:
            home_dir = self.get_db_path()

        return home_dir

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

        repl_cluster = self.get_replicaset_cluster()
        if repl_cluster is not None:
            if "replSet" not in cmd_options:
                cmd_options["replSet"] = repl_cluster.id

        # add configsvr as needed
        if self.is_config_server():
            cmd_options["configsvr"] = True

        # add shardsvr as needed
        if self.is_shard_server():
            cmd_options["shardsvr"] = True

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
    def set_auth(self,auth):
        self.set_cmd_option("auth", auth)


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
    def get_rs_status_summary(self):
        if self.is_replicaset_member():
            member_rs_status = self.get_member_rs_status()
            if member_rs_status:
                return {
                    "name": member_rs_status['name'],
                    "stateStr": member_rs_status['stateStr']
                }

    ###########################################################################
    def is_cluster_connection_member(self):
        return not self.is_arbiter_server()

    ###########################################################################
    def is_arbiter_server(self):
        cluster = self.get_cluster()
        return (isinstance(cluster, ReplicaSetCluster) and
                cluster.get_member_for(self).is_arbiter())

    ###########################################################################
    def is_replicaset_member(self):
        cluster = self.get_cluster()
        return isinstance(cluster, ReplicaSetCluster)

    ###########################################################################
    def get_replicaset_cluster(self):
        cluster = self.get_cluster()
        if isinstance(cluster, ReplicaSetCluster):
            return cluster

    ###########################################################################
    def is_config_server(self):
        cluster = self.get_cluster()

        return ((isinstance(cluster, ShardedCluster) and
                 cluster.has_config_server(self)) or
                self.get_cmd_option("configsvr"))

    ###########################################################################
    def is_shard_server(self):
        cluster = self.get_cluster()
        if isinstance(cluster, ShardedCluster):
            return cluster.has_shard(self)
        elif isinstance(cluster, ReplicaSetCluster):
            return cluster.is_shard_member()

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
            log_debug("Error whille trying to read rs config from "
                      "server '%s': %s" % (self.id, e))
            log_exception(e)
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
            log_debug("Cannot get rs status from server '%s'. cause: %s" %
                        (self.id, e))
            log_exception(e)
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
                log_debug("Cannot get member rs status from server '%s'."
                          " cause: %s" % (self.id, e))
                log_exception(e)

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
    def has_joined_replica(self):
        master_result = self.is_master_command()
        if master_result:
            return (master_result.get("setName") or
                    master_result.get("ismaster") or
                    master_result.get("arbiterOnly") or
                    master_result.get("secondary"))

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


