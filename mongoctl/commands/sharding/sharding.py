__author__ = 'abdul'

import mongoctl.repository as repository
from mongoctl.utils import document_pretty_string
from mongoctl.mongoctl_logging import log_info

from mongoctl.objects.shardset_cluster import ShardSetCluster
from mongoctl.objects.replicaset_cluster import ReplicaSetCluster

from mongoctl.errors import MongoctlException



###############################################################################
# configure shard cluster command
###############################################################################
def configure_shard_cluster_command(parsed_options):
    cluster_id = parsed_options.cluster
    cluster = repository.lookup_and_validate_cluster(cluster_id)

    if not isinstance(cluster, ShardSetCluster):
        raise MongoctlException("Cluster '%s' is not a shardset cluster" %
                                cluster.id)

    if parsed_options.dryRun:
        dry_run_configure_shard_cluster(cluster)
    else:
        configure_shard_cluster(cluster)

###############################################################################
# ShardSetCluster Methods
###############################################################################

def configure_shard_cluster(cluster):
    cluster.configure_shardset()

###############################################################################
def dry_run_configure_shard_cluster(cluster):

    log_info("\n************ Dry Run ************\n")

    db_command = cluster.get_shardset_configure_command()

    log_info("Executing the following command")
    log_info(document_pretty_string(db_command))

###############################################################################
# Add shard command
###############################################################################
def add_shard_command(parsed_options):
    shard_id = parsed_options.shardId

    # determine if the shard is a replicaset cluster or a server
    shard = repository.lookup_cluster(shard_id)

    if not shard:
        shard = repository.lookup_server(shard_id)

    if not shard:
        raise MongoctlException("Unknown shard '%s'" % shard_id)

    shardset_cluster = repository.config_lookup_cluster_by_shard(shard)

    if not shardset_cluster:
        raise MongoctlException("'%s' is not a shard" % shard_id)


    if parsed_options.dryRun:
        dry_run_add_shard(shard, shardset_cluster)
    else:
        add_shard(shard, shardset_cluster)


###############################################################################
def add_shard(shard, shardset_cluster):
    shardset_cluster.add_shard(shard)

###############################################################################
def dry_run_add_shard(shard, shardset_cluster):
    log_info("\n************ Dry Run ************\n")

    shard_member = shardset_cluster.get_shard_member(shard)
    db_command = shardset_cluster.get_add_shard_command(shard_member)

    log_info("Executing the following command")
    log_info(document_pretty_string(db_command))
