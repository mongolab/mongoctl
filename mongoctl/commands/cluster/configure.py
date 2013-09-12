__author__ = 'abdul'


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

