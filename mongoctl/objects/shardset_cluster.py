__author__ = 'abdul'

import mongoctl.repository as repository

from cluster import Cluster
from server import Server

from base import DocumentWrapper
from bson import DBRef

from mongoctl.mongoctl_logging import log_info
from mongoctl.utils import document_pretty_string
###############################################################################
# ShardSet Cluster Class
###############################################################################
class ShardSetCluster(Cluster):

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
    def configure_shardset(self):
        raise Exception("No implemented")

    ###########################################################################
    def add_shard(self, shard):
        log_info("Adding shard '%s' to shardset '%s' " % (shard.id, self.id))

        if self.is_shard_configured(shard):
            log_info("Shard '%s' already added! Nothing to do..." % shard.id)
            return

        mongos = self.get_any_online_mongos()
        shard_member = self.get_shard_member(shard)
        cmd = self.get_add_shard_command(shard_member)



        configured_shards = self.read_configured_shards()
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
    def read_configured_shards(self):
        mongos = self.get_any_online_mongos()
        return list(mongos.get_db("config")["shards"].find())

    ###########################################################################
    def is_shard_configured(self, shard):
        mongos = self.get_any_online_mongos()
        return mongos.get_db("config")["shards"].find_one(shard.id) is not None

    ###########################################################################
    def get_any_online_mongos(self):
        for member in self.get_members():
            if member.get_server().is_online():
                return member.get_server()

        raise Exception("Unable to connect to a mongos")

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






