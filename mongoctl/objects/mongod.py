__author__ = 'abdul'



import server

from mongoctl.utils import resolve_path
from mongoctl.mongoctl_logging import log_verbose, log_debug, log_exception, \
    log_warning, log_info

from bson.son import SON
from mongoctl.errors import MongoctlException
from replicaset_cluster import ReplicaSetCluster, get_member_repl_lag
from sharded_cluster import ShardedCluster
from mongoctl.mongodb_version import make_version_info
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
    def export_cmd_options(self, options_override=None, standalone=False):
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

        # apply standalone if specified
        if standalone:
            if "replSet" in cmd_options:
                del cmd_options["replSet"]
            if "keyFile" in cmd_options:
                del cmd_options["keyFile"]

        # add configsvr as needed
        if self.is_config_server():
            cmd_options["configsvr"] = True

        # add shardsvr as needed
        if self.is_shard_server():
            cmd_options["shardsvr"] = True

        # remove wiredTigerCacheSizeGB if its not an int since we set it in runtime parameter
        #  wiredTigerEngineRuntimeConfig in this case
        if "wiredTigerCacheSizeGB" in cmd_options and not isinstance(self.get_cmd_option("wiredTigerCacheSizeGB"), int):
            del cmd_options["wiredTigerCacheSizeGB"]

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

        if "error" not in status and admin:
            rs_summary = self.get_rs_status_summary()
            if rs_summary:
                status["selfReplicaSetStatusSummary"] = rs_summary
                status["oplogLength"] = self.get_log_length_summary()
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

        if self.get_cmd_option("configsvr"):
            return True
        elif isinstance(cluster, ShardedCluster):
            return cluster.has_config_server(self)
        elif isinstance(cluster, ReplicaSetCluster):
            return cluster.is_config_replica()


    ###########################################################################
    def is_standalone_config_server(self):
        if self.is_config_server():
            cluster = self.get_cluster()
            return isinstance(cluster, ShardedCluster)

    ###########################################################################
    def is_shard_server(self):
        cluster = self.get_cluster()
        if isinstance(cluster, ShardedCluster):
            return cluster.has_shard(self)
        elif isinstance(cluster, ReplicaSetCluster):
            return cluster.is_shard()

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
            return (master_result.get("ismaster") or
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

    ###########################################################################
    def get_environment_variables(self):
        env_vars = super(MongodServer, self).get_environment_variables() or {}

        # default TCMALLOC_AGGRESSIVE_DECOMMIT for wiredTiger if not set
        if self.is_wired_tiger() and "TCMALLOC_AGGRESSIVE_DECOMMIT" not in env_vars:
            log_info("Server is wiredTiger. Defaulting TCMALLOC_AGGRESSIVE_DECOMMIT=y")
            env_vars["TCMALLOC_AGGRESSIVE_DECOMMIT"] = "y"

        return env_vars

    ###########################################################################
    def get_allowed_environment_variables(self):
        return ["TCMALLOC_AGGRESSIVE_DECOMMIT"]

    ###########################################################################
    def is_wired_tiger(self):
        version = self.get_mongo_version_info()
        storage_engine = self.get_cmd_option("storageEngine")

        if version >= make_version_info("3.2.0"):
            return not storage_engine or storage_engine == "wiredTiger"
        elif version >= make_version_info("3.0.0"):
            return storage_engine == "wiredTiger"

    ###########################################################################
    def set_runtime_parameters(self):
        params = self.generate_runtime_parameters()

        if params:
            for name, val in params.items():
                self.set_runtime_parameter_cmd(name, val)

            # verify that params were applied properly
            self.verify_runtime_parameters()

    ###########################################################################
    def verify_runtime_parameters(self):
        log_info("Verifying runtime params...")
        wtcs_gb = self.get_cmd_option("wiredTigerCacheSizeGB")
        if wtcs_gb is not None and isinstance(wtcs_gb, float):
            wtcs_bytes = int(wtcs_gb * 1024 * 1024 * 1024)
            server_status = self.server_status()
            if not(server_status and "wiredTiger" in server_status and "cache" in server_status["wiredTiger"] and
                           "maximum bytes configured" in server_status["wiredTiger"]["cache"] and
                           server_status["wiredTiger"]["cache"]["maximum bytes configured"] == wtcs_bytes):
                raise MongoctlException("CONFIG ERROR: wiredTigerCacheSizeGB was not applied")

        log_info("Runtime params verification passed!")

    ###########################################################################
    def generate_runtime_parameters(self):
        parameters = {}
        return parameters
        """ TODO Enable later
        wtcs_gb = self.get_cmd_option("wiredTigerCacheSizeGB")
        if wtcs_gb is not None and wtcs_gb < 1:
            wtcs_mb = int(wtcs_gb * 1024)
            parameters["wiredTigerEngineRuntimeConfig"] = "cache_size=%sM" % wtcs_mb

        return parameters
        """

    ###########################################################################
    def set_runtime_parameter_cmd(self, name, value):
        log_info("Setting runtime parameter '%s' to '%s', for server '%s'..." %
                 (name, value, self.id))
        cmd = {
            "setParameter": 1,
            name: value
        }
        self.db_command(cmd, "admin")


    ###########################################################################
    def get_replication_info(self):
        try:
            ol = self.get_db("local")["oplog.rs"]
            first_op = ol.find(sort=[("$natural", 1)], limit=1).next()
            last_op = ol.find(sort=[("$natural", -1)], limit=1).next()
            first_ts = first_op["ts"]
            last_ts = last_op["ts"]
            return {
                "timeDiff": last_ts.time - first_ts.time,
                "timeDiffHours": (last_ts.time - first_ts.time) / 3600
            }
        except Exception, ex:
            log_exception("Error during get_replication_info()")

    ###########################################################################
    def get_log_length_summary(self):
        repl_info = self.get_replication_info()
        if repl_info:
            return "%s secs (%s hours)" % (repl_info["timeDiff"], repl_info["timeDiffHours"])
        else:
            return ""



