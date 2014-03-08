__author__ = 'abdul'

import mongoctl.repository as repository

from cluster import Cluster
from server import Server

from base import DocumentWrapper
from bson import DBRef

from mongoctl.mongoctl_logging import log_info
from mongoctl.utils import document_pretty_string

import time
###############################################################################
# ShardSet Cluster Class
###############################################################################
class ShardedCluster(Cluster):

    ###########################################################################
    # Constructor and other init methods
    ###########################################################################
    def __init__(self, cluster_document):
        Cluster.__init__(self, cluster_document)
        self._config_members = self._resolve_members("configServers")
        self._shards = self._resolve_shard_members()

    ###########################################################################
    def _resolve_shard_members(self):
        member_documents = self.get_property("shards")
        members = []

        # if members are not set then return
        if member_documents:
            for mem_doc in member_documents:
                member = ShardMember(mem_doc)
                members.append(member)

        return members

    ###########################################################################
    @property
    def config_members(self):
        return self._config_members

    ###########################################################################
    def has_config_server(self, server):
        for member in self.config_members:
            if member.get_server().id == server.id:
                return True

    ###########################################################################
    @property
    def shards(self):
        return self._shards

    ###########################################################################
    def has_shard(self, shard):
        return self.get_shard_member(shard) is not None

    ###########################################################################
    def get_shard_member(self, shard):
        for shard_member in self.shards:
            if ((isinstance(shard, Server) and
                 shard_member.get_server() and
                 shard_member.get_server().id == shard.id)
                or
                (isinstance(shard, Cluster) and
                 shard_member.get_cluster() and
                 shard_member.get_cluster().id == shard.id)):
                return shard_member

    ###########################################################################
    def get_shard_member_by_shard_id(self, shard_id):
        for shard_member in self.shards:
            if ((shard_member.get_server() and
                         shard_member.get_server().id == shard_id)
                or (shard_member.get_cluster() and
                            shard_member.get_cluster().id == shard_id)):
                return shard_member
    ###########################################################################
    def get_config_member_addresses(self):
        addresses = []
        for member in self.config_members:
            addresses.append(member.get_server().get_address())

        return addresses

    ###########################################################################
    def get_member_addresses(self):
        addresses = []
        for member in self.config_members:
            addresses.append(member.get_server().get_address())

        return addresses

    ###########################################################################
    def get_shard_member_address(self, shard_member):

        if shard_member.get_server():
            return shard_member.get_server().get_address()
        elif shard_member.get_cluster():
            cluster_member_addresses = []
            for cluster_member in shard_member.get_cluster().get_members():
                cluster_member_addresses.append(
                    cluster_member.get_server().get_address())
            return "%s/%s" % (shard_member.get_cluster().id,
                              ",".join(cluster_member_addresses))


    ###########################################################################
    def configure_sharded_cluster(self):
        sh_list = self.list_shards()
        if sh_list and sh_list.get("shards"):
            log_info("Shard cluster already configured. Will only be adding"
                     " new shards as needed...")

        for shard_member in self.shards:
            self.add_shard(shard_member.get_shard())

    ###########################################################################
    def add_shard(self, shard):
        log_info("Adding shard '%s' to ShardedCluster '%s' " % (shard.id, self.id))

        if self.is_shard_configured(shard):
            log_info("Shard '%s' already added! Nothing to do..." % shard.id)
            return

        mongos = self.get_any_online_mongos()
        shard_member = self.get_shard_member(shard)
        cmd = self.get_add_shard_command(shard_member)

        configured_shards = self.list_shards()
        log_info("Current configured shards: \n%s" %
                 document_pretty_string(configured_shards))


        log_info("Executing command \n%s\non mongos '%s'" %
                 (document_pretty_string(cmd), mongos.id))
        mongos.db_command(cmd, "admin")

        log_info("Shard '%s' added successfully!" % self.id)

    ###########################################################################
    def get_add_shard_command(self, shard_member):
        return {
            "addShard": self.get_shard_member_address(shard_member)
        }

    ###########################################################################
    def remove_shard(self, shard, unsharded_data_dest_id=None,
                     synchronized=False):
        log_info("Removing shard '%s' from ShardedCluster '%s' " %
                 (shard.id, self.id))

        configured_shards = self.list_shards()
        log_info("Current configured shards: \n%s" %
                 document_pretty_string(configured_shards))

        completed = False
        while not completed:
            result = self._do_remove_shard(shard, unsharded_data_dest_id)
            completed = synchronized and (result["state"] == "completed" or
                                          not self.is_shard_configured(shard))
            if not completed:
                time.sleep(2)

    ###########################################################################

    def _do_remove_shard(self, shard, unsharded_data_dest_id=None):
        cmd = self.get_validate_remove_shard_command(shard)
        mongos = self.get_any_online_mongos()

        log_info("Executing command \n%s\non mongos '%s'" %
                 (document_pretty_string(cmd), mongos.id))

        result = mongos.db_command(cmd, "admin")

        log_info("Command result: \n%s" % result)

        if "dbsToMove" in result and unsharded_data_dest_id:
            dest_shard_member = self.get_shard_member_by_shard_id(
                unsharded_data_dest_id)

            if not dest_shard_member:
                raise Exception("No such shard '%s' in ShardedCluster '%s' " %
                                (unsharded_data_dest_id, self.id))

            dest_shard = dest_shard_member.get_shard()
            self.move_dbs_primary(result["dbsToMove"], dest_shard)


        if result.get('state') == "completed":
            log_info("Shard '%s' removed successfully!" % self.id)

        return result

    ###########################################################################
    def get_validate_remove_shard_command(self, shard):
        if not self.is_shard_configured(shard):
            raise Exception("Bad remove shard attempt. Shard '%s' has not"
                            " been added yet" % shard.id)

        # TODO: re-enable this when  is_last_shard works properly
        # check if its last shard and raise error if so
        ##if self.is_last_shard(shard):
          ##  raise Exception("Bad remove shard attempt. Shard '%s' is the last"
            ##                " shard" % shard.id)
        shard_member = self.get_shard_member(shard)

        return {
            "removeShard": shard_member.get_shard_id()
        }

    ###########################################################################
    def get_remove_shard_command(self, shard):
        return {
            "removeShard": shard.id
        }

    ###########################################################################
    def list_shards(self):
        mongos = self.get_any_online_mongos()
        return mongos.db_command({"listShards": 1}, "admin")

    ###########################################################################
    def is_shard_configured(self, shard):
        shard_list = self.list_shards()
        if shard_list and shard_list.get("shards"):
            for sh in shard_list["shards"]:
                if shard.id == sh["_id"]:
                    return True

    ###########################################################################
    def is_last_shard(self, shard):
        # TODO: implement
        pass

    ###########################################################################
    def get_default_server(self):
        return self.get_any_online_mongos()

    ###########################################################################
    def get_any_online_mongos(self):
        for member in self.get_members():
            if member.get_server().is_online():
                return member.get_server()

        raise Exception("Unable to connect to a mongos")


    ###########################################################################
    def move_dbs_primary(self, db_names, dest_shard):
        log_info("Moving databases %s primary to shard '%s'" %
                 (db_names, dest_shard.id))
        mongos = self.get_any_online_mongos()

        for db_name in db_names:
            move_cmd = {
                "movePrimary": db_name,
                "to": dest_shard.id
            }
            log_info("Executing movePrimary command:\n%s\non mongos '%s'" %
                     (document_pretty_string(move_cmd), mongos.id))

            result = mongos.db_command(move_cmd, "admin")

            log_info("Move result: %s" % document_pretty_string(result))

    ###########################################################################
    def get_member_type(self):
        return ShardMember

###############################################################################
# ShardMember Class
###############################################################################
class ShardMember(DocumentWrapper):
    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, member_doc):
        DocumentWrapper.__init__(self, member_doc)
        self._server = None
        self._cluster = None

    ###########################################################################
    def get_server(self):
        server_doc = self.get_property("server")
        if not server_doc:
            return

        if self._server is None:
            if server_doc is not None:
                if type(server_doc) is DBRef:
                    self._server = repository.lookup_server(server_doc.id)

        return self._server

    ###########################################################################
    def get_cluster(self):
        cluster_doc = self.get_property("cluster")
        if not cluster_doc:
            return

        if self._cluster is None:
            if cluster_doc is not None:
                if type(cluster_doc) is DBRef:
                    self._cluster = repository.lookup_cluster(cluster_doc.id)

        return self._cluster

    ###########################################################################
    def get_shard_id(self):
        return self.get_shard().id

    ###########################################################################
    def get_shard(self):

        if self.get_server():
            return self.get_server()
        elif self.get_cluster():
            return self.get_cluster()

