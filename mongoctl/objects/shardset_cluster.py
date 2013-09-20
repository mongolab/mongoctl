__author__ = 'abdul'

import mongoctl.repository as repository

from cluster import Cluster
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
    def has_shard_server(self, server):
        for shard in self.shards:
            if shard.get_server() and shard.get_server().id == server.id:
                return True

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
    def get_shard_addresses(self):
        addresses = []
        for member in self.shards:
            if member.get_server():
                addresses.append(member.get_server().get_address())
            elif member.get_cluster():
                cluster_member_addresses = []
                for cluster_member in member.get_cluster().get_members():
                    cluster_member_addresses.append(
                        cluster_member.get_server().get_address())
                cluster_shard_address = "%s/%s" % (member.get_cluster().id,
                                                   ",".join(cluster_member_addresses))
                addresses.append(cluster_shard_address)

        return addresses

    ###########################################################################
    def configure_shardset(self):
        mongos = self.get_any_online_member().get_server()
        cmd = self.get_shardset_configure_command()
        log_info("Executing command \n%s\non mongos '%s'" %
                 (document_pretty_string(cmd), mongos.id))
        mongos.db_command(cmd, "admin")

        log_info("Shardset '%s' configured successfully!" % self.id)

    ###########################################################################
    def get_shardset_configure_command(self):
        return {
            "addShard": self.get_shard_url()
        }

    ###########################################################################
    def get_shard_url(self):
        shard_addresses = self.get_shard_addresses()
        return ",".join(shard_addresses)

    ###########################################################################
    def get_any_online_member(self):
        for member in self.get_members():
            if member.get_server().is_online():
                return member

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






