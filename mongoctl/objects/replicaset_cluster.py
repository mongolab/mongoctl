__author__ = 'abdul'

import mongoctl.repository as repository

from cluster import Cluster
from mongoctl import users
from base import DocumentWrapper
from mongoctl.utils import *
from bson import DBRef

from mongoctl.config import get_cluster_member_alt_address_mapping
from mongoctl.mongoctl_logging import log_verbose, log_error, log_db_command

from mongoctl.prompt import prompt_confirm

###############################################################################
# ReplicaSet Cluster Member Class
###############################################################################

class ReplicaSetClusterMember(DocumentWrapper):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, member_doc):
        DocumentWrapper.__init__(self, member_doc)
        self._server = None

    ###########################################################################
    # Properties
    ###########################################################################

    def get_server(self):

        server_doc = self.get_property("server")
        host = self.get_property("host")

        if self._server is None:
            if server_doc is not None:
                if type(server_doc) is DBRef:
                    self._server = repository.lookup_server(server_doc.id)
            elif host is not None:
                self._server = repository.build_server_from_address(host)

        return self._server

    ###########################################################################
    def get_host(self):
        server = self.get_server()
        if server:
            return server.get_address()

    ###########################################################################
    def is_arbiter(self):
        return self.get_property("arbiterOnly") == True

    ###########################################################################
    def is_passive(self):
        return self.get_priority() == 0

    ###########################################################################
    def get_priority(self):
        return self.get_property("priority")

    ###########################################################################
    # Interface Methods
    ###########################################################################
    def get_member_type(self):
        return ReplicaSetClusterMember

    ###########################################################################
    def can_become_primary(self):
        return not self.is_arbiter() and self.get_priority() != 0

    ###########################################################################
    def get_member_repl_config(self):

        # create the member repl config with host

        member_conf = {"host": self.get_host()}

        # Add the rest of the properties configured in the document
        #  EXCEPT host/server

        ignore = ['host', 'server']

        for key,value in self.__document__.items():
            if key not in ignore :
                member_conf[key] = value

        self._apply_alt_address_mapping(member_conf)

        return member_conf

    ###########################################################################
    def _apply_alt_address_mapping(self, member_conf):

        # Not applicable to arbiters
        if self.is_arbiter():
            return

        tag_mapping = get_cluster_member_alt_address_mapping()
        if not tag_mapping:
            return

        tags = member_conf.get("tags", {})
        for tag_name, alt_address_prop in tag_mapping.items():
            alt_address = self.get_server().get_property(alt_address_prop)

            # set the alt address if it is different than host
            if alt_address and alt_address != member_conf['host']:
                tags[tag_name] = alt_address
            else:
                log_verbose("No alt address tag value created for alt address"
                            " mapping '%s=%s' for member \n%s" %
                            (tag_name, alt_address_prop, self))

        # set the tags property of the member config if there are any
        if tags:
            log_verbose("Member '%s' tags : %s" % (member_conf['host'], tags))
            member_conf['tags'] = tags

    ###########################################################################
    def read_rs_config(self):
        if self.is_valid():
            server = self.get_server()
            if server.can_function():
                return server.get_rs_config()
        return None

    ###########################################################################
    def is_valid(self):
        try:
            self.validate()
            return True
        except Exception, e:
            log_error("%s" % e)
            log_exception(e)
            return False

    ###########################################################################
    def validate(self):
        host_conf = self.get_property("host")
        server_conf = self.get_property("server")

        # ensure that 'server' or 'host' are configured

        if server_conf is None and host_conf is None:
            msg = ("Invalid member configuration:\n%s \n"
                   "Please set 'server' or 'host'." %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)

        # validate host if set
        if host_conf and not is_valid_member_address(host_conf):
            msg = ("Invalid 'host' value in member:\n%s \n"
                   "Please make sure 'host' is in the 'address:port' form" %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)

        # validate server if set
        server = self.get_server()
        if server is None:
            msg = ("Invalid 'server' value in member:\n%s \n"
                   "Please make sure 'server' is set or points to a "
                   "valid server." %
                   document_pretty_string(self.get_document()))
            raise MongoctlException(msg)
        repository.validate_server(server)
        if server.get_address() is None:
            raise MongoctlException("Invalid member configuration for server "
                                    "'%s'. address property is not set." %
                                    (server.id))

    ###########################################################################
    def validate_against_current_config(self, current_rs_conf):
        """
        Validates the member document against current rs conf
            1- If there is a member in current config with _id equals to my id
                then ensure hosts addresses resolve to the same host

            2- If there is a member in current config with host resolving to my
               host then ensure that if my id is et then it
               must equal member._id

        """

        # if rs is not configured yet then there is nothing to validate
        if not current_rs_conf:
            return

        my_host = self.get_host()
        current_member_confs = current_rs_conf['members']
        err = None
        for curr_mem_conf in current_member_confs:
            if (self.id and
                        self.id == curr_mem_conf['_id'] and
                    not is_same_address(my_host, curr_mem_conf['host'])):
                err = ("Member config is not consistent with current rs "
                       "config. \n%s\n. Both have the sam _id but addresses"
                       " '%s' and '%s' do not resolve to the same host." %
                       (document_pretty_string(curr_mem_conf),
                        my_host, curr_mem_conf['host'] ))

            elif (is_same_address(my_host, curr_mem_conf['host']) and
                      self.id and
                          self.id != curr_mem_conf['_id']):
                err = ("Member config is not consistent with current rs "
                       "config. \n%s\n. Both addresses"
                       " '%s' and '%s' resolve to the same host but _ids '%s'"
                       " and '%s' are not equal." %
                       (document_pretty_string(curr_mem_conf),
                        my_host, curr_mem_conf['host'],
                        self.id, curr_mem_conf['_id']))

        if err:
            raise MongoctlException("Invalid member configuration:\n%s \n%s" %
                                    (self, err))


    ###########################################################################
    def validate_against_other(self, other_member):
        err = None
        # validate _id uniqueness
        if self.id and self.id == other_member.id:
            err = ("Duplicate '_id' ('%s') found in a different member." %
                   self.id)

        # validate server uniqueness
        elif (self.get_property('server') and
                      self.get_server().id == other_member.get_server().id):
            err = ("Duplicate 'server' ('%s') found in a different member." %
                   self.get_server().id)
        else:

            # validate host uniqueness
            h1 = self.get_host()
            h2 = other_member.get_host()

            try:

                if is_same_address(h1, h2):
                    err = ("Duplicate 'host' found. Host in '%s' and "
                           "'%s' map to the same host." % (h1, h2))

            except Exception, e:
                log_exception(e)
                err = "%s" % e

        if err:
            raise MongoctlException("Invalid member configuration:\n%s \n%s" %
                                    (self, err))

###############################################################################
# ReplicaSet Cluster Class
###############################################################################
class ReplicaSetCluster(Cluster):

    ###########################################################################
    # Constructor and other init methods
    ###########################################################################
    def __init__(self, cluster_document):
        Cluster.__init__(self, cluster_document)
        self._members = self._resolve_members("members")

    ###########################################################################
    def _resolve_members(self, member_prop):
        member_documents = self.get_property(member_prop)
        members = []

        # if members are not set then return
        if member_documents:
            for mem_doc in member_documents:
                member = repository.new_replicaset_cluster_member(mem_doc)
                members.append(member)

        return members

    ###########################################################################
    # Interface Methods
    ###########################################################################

    def get_default_server(self):
        return self.get_primary_server()

    ###########################################################################
    def get_primary_server(self):
        primary_member = self.get_primary_member()
        if primary_member:
            return primary_member.get_server()

    ###########################################################################
    def get_primary_member(self):
        for member in self.get_members():
            if member.get_server().is_primary():
                return member

        return None

    ###########################################################################
    def suggest_primary_member(self):
        for member in self.get_members():
            if(member.can_become_primary() and
                       member.get_server() is not None and
                   member.get_server().is_online_locally()):
                return member

    ###########################################################################
    def get_status(self):
        primary_server = self.get_primary_server()

        if not primary_server:
            raise MongoctlException("Unable to determine primary member for"
                                    " cluster '%s'" % self.id)

        rs_config_members = primary_server.get_rs_config()['members']

        master_status = primary_server.get_member_rs_status()
        primary_server_address = master_status['name']
        rs_status_members = primary_server.get_rs_status()['members']

        other_members = []
        for rs_status_m in rs_status_members:
            if not rs_status_m.get("self", False):
                address = rs_status_m.get("name")
                member = {
                    "address": address,
                    "stateStr": rs_status_m.get("stateStr")
                    #"uptime": utils.time_string(rs_status_m.get("uptime"))
                }
                if rs_status_m.get("errmsg", None):
                    member['errmsg'] = rs_status_m['errmsg']
                if rs_status_m.get("stateStr", None) in ["STARTUP2", "SECONDARY",
                                               "RECOVERING"]:
                    # compute lag
                    lag_in_secs = get_member_repl_lag(rs_status_m, master_status)

                    member['replLag'] = {
                        "value": lag_in_secs,
                        "description": utils.time_string(lag_in_secs)
                    }

                for rs_config_m in rs_config_members:
                    if rs_config_m['host'] == rs_status_m['name']:
                        if rs_config_m.has_key("slaveDelay"):
                            member['slaveDelay'] = rs_config_m['slaveDelay']
                        if rs_config_m.get("priority", 1) != 1:
                            member['priority'] = rs_config_m['priority']
                        if rs_config_m.get("votes", 1) != 1:
                            member['votes'] = rs_config_m['votes']
                        if rs_config_m.get("hidden", False) != False:
                            member['hidden'] = rs_config_m['hidden'] 
                        if rs_config_m.get("tags"):
                            member['tags'] = rs_config_m['tags'] 

                other_members.append(member)
        return {
            "primary": {
                "address": primary_server_address,
                "stateStr": "PRIMARY",
                "serverStatusSummary": primary_server.get_server_status_summary()
                #"uptime": utils.time_string(master_status.get("uptime"))
            },
            "otherMembers": other_members
        }

    ###########################################################################
    def get_dump_best_secondary(self, max_repl_lag=None):
        """
        Returns the best secondary member to be used for dumping
        best = passives with least lags, if no passives then least lag
        """
        secondary_lag_tuples = []

        primary_member = self.get_primary_member()
        if not primary_member:
            raise MongoctlException("Unable to determine primary member for"
                                    " cluster '%s'" % self.id)

        master_status = primary_member.get_server().get_member_rs_status()

        if not master_status:
            raise MongoctlException("Unable to determine replicaset status for"
                                    " primary member '%s'" %
                                    primary_member.get_server().id)

        for member in self.get_members():
            if member.get_server().is_secondary():
                repl_lag = member.get_server().get_repl_lag(master_status)
                if max_repl_lag and  repl_lag > max_repl_lag:
                    log_info("Excluding member '%s' because it's repl lag "
                             "(in seconds)%s is more than max %s. " %
                             (member.get_server().id,
                              repl_lag, max_repl_lag))
                    continue
                secondary_lag_tuples.append((member,repl_lag))

        def best_secondary_comp(x, y):
            x_mem, x_lag = x
            y_mem, y_lag = y
            if x_mem.is_passive():
                if y_mem.is_passive():
                    return x_lag - y_lag
                else:
                    return -1
            elif y_mem.is_passive():
                return 1
            else:
                return x_lag - y_lag

        if secondary_lag_tuples:
            secondary_lag_tuples.sort(best_secondary_comp)
            return secondary_lag_tuples[0][0]

    ###########################################################################
    def is_replicaset_initialized(self):
        """
        iterate on all members and check if any has joined the replica
        """

        # it's possible isMaster returns an "incomplete" result if we
        # query a replica set member while it's loading the replica set config
        # https://jira.mongodb.org/browse/SERVER-13458
        # let's try to detect this state before proceeding
        # seems like if the "secondary" field is present, but "setName" isn't,
        # it's a good indicator that we just need to wait a bit
        # add an uptime check in for good measure

        for member in self.get_members():
            server = member.get_server()

            if server.has_joined_replica():
                return True

        return False

    ###########################################################################
    def initialize_replicaset(self, suggested_primary_server=None):
        log_info("Initializing replica set cluster '%s' %s..." %
                 (self.id,
                  "" if suggested_primary_server is None else
                  "to contain only server '%s'" %
                  suggested_primary_server.id))

        ##### Determine primary server
        log_info("Determining which server should be primary...")
        primary_server = suggested_primary_server
        if primary_server is None:
            primary_member = self.suggest_primary_member()
            if primary_member is not None:
                primary_server = primary_member.get_server()

        if primary_server is None:
            raise MongoctlException("Unable to determine primary server."
                                    " At least one member server has"
                                    " to be online.")
        log_info("Selected server '%s' as primary." % primary_server.id)

        init_cmd = self.get_replicaset_init_all_db_command(
            suggested_primary_server)

        try:

            log_db_command(init_cmd)
            primary_server.timeout_maybe_db_command(init_cmd, "admin")

            # wait for replset to init
            def is_init():
                return self.is_replicaset_initialized()

            log_info("Will now wait for the replica set to initialize.")
            wait_for(is_init,timeout=60, sleep_duration=1)

            if self.is_replicaset_initialized():
                log_info("Successfully initiated replica set cluster '%s'!" %
                         self.id)
            else:
                msg = ("Timeout error: Initializing replicaset '%s' took "
                       "longer than expected. This does not necessarily"
                       " mean that it failed but it could have failed. " %
                       self.id)
                raise MongoctlException(msg)
                ## add the admin user after the set has been initiated
            ## Wait for the server to become primary though (at MongoDB's end)

            def is_primary_for_real():
                return primary_server.is_primary()

            log_info("Will now wait for the intended primary server to "
                     "become primary.")
            wait_for(is_primary_for_real,timeout=60, sleep_duration=1)

            if not is_primary_for_real():
                msg = ("Timeout error: Waiting for server '%s' to become "
                       "primary took longer than expected. "
                       "Please try again later." % primary_server.id)
                raise MongoctlException(msg)

            log_info("Server '%s' is primary now!" % primary_server.id)

            # setup cluster users
            users.setup_cluster_users(self, primary_server)

            log_info("New replica set configuration:\n%s" %
                     document_pretty_string(self.read_rs_config()))
            return True
        except Exception, e:
            log_exception(e)
            raise MongoctlException("Unable to initialize "
                                    "replica set cluster '%s'. Cause: %s" %
                                    (self.id,e) )

    ###########################################################################
    def configure_replicaset(self, add_server=None, force_primary_server=None):

        # Check if this is an init VS an update
        if not self.is_replicaset_initialized():
            self.initialize_replicaset()
            return

        primary_member = self.get_primary_member()

        # force server validation and setup
        if force_primary_server:
            force_primary_member = self.get_member_for(force_primary_server)
            # validate is cluster member
            if not force_primary_member:
                msg = ("Server '%s' is not a member of cluster '%s'" %
                       (force_primary_server.id, self.id))
                raise MongoctlException(msg)

            # validate is administrable
            if not force_primary_server.is_administrable():
                msg = ("Server '%s' is not running or has connection problems."
                       " For more details, Run 'mongoctl status %s'" %
                       (force_primary_server.id,
                        force_primary_server.id))
                raise MongoctlException(msg)

            if not force_primary_member.can_become_primary():
                msg = ("Server '%s' cannot become primary. Reconfiguration of"
                       " a replica set must be sent to a node that can become"
                       " primary" % force_primary_server.id)
                raise MongoctlException(msg)

            if primary_member:
                msg = ("Cluster '%s' currently has server '%s' as primary. "
                       "Proceed with force-reconfigure on server '%s'?" %
                       (self.id,
                        primary_member.get_server().id,
                        force_primary_server.id))
                if not prompt_confirm(msg):
                    return
            else:
                log_info("No primary server found for cluster '%s'" %
                         self.id)
        elif primary_member is None:
            raise MongoctlException("Unable to determine primary server"
                                    " for replica set cluster '%s'" %
                                    self.id)

        cmd_server = (force_primary_server if force_primary_server
                      else primary_member.get_server())

        log_info("Re-configuring replica set cluster '%s'..." % self.id)

        force = force_primary_server is not None
        rs_reconfig_cmd = \
            self.get_replicaset_reconfig_db_command(add_server=add_server,
                                                    force=force)
        desired_config = rs_reconfig_cmd['replSetReconfig']

        try:
            log_info("Executing the following command on server '%s':"
                     "\n%s" % (cmd_server.id,
                               document_pretty_string(rs_reconfig_cmd)))

            cmd_server.disconnecting_db_command(rs_reconfig_cmd, "admin")

            log_info("Re-configuration command for replica set cluster '%s'"
                     " issued successfully." % self.id)

            # wait until there is a primary

            log_info("Wait for the new primary to be elected...")
            def has_primary():
                return self.get_primary_member() is not None

            if not wait_for(has_primary, timeout=60, sleep_duration=1):
                raise Exception("No primary elected 60 seconds after reconfiguration!")

            # Probably need to reconnect.  May not be primary any more.
            desired_cfg_version = desired_config['version']

            def got_the_memo(cur_cfg=None):
                current_config = cur_cfg or self.read_rs_config()
                # might've gotten None if nobody answers & tells us, so:
                current_cfg_version = (current_config['version']
                                       if current_config else 0)
                version_diff = (current_cfg_version - desired_cfg_version)
                return ((version_diff == 0) or
                        # force => mongo adds large random # to 'version'.
                        (force and version_diff >= 0))

            realized_config = self.read_rs_config()
            if not got_the_memo(realized_config):
                log_verbose("Really? Config version %s? "
                            "Let me double-check that ..." %
                            "unchanged" if realized_config else "unavailable")

                if not wait_for(got_the_memo, timeout=45, sleep_duration=5):
                    raise Exception("New config version not detected!")
                    # Finally! Resample.
                realized_config = self.read_rs_config()

            log_info("New replica set configuration:\n %s" %
                     document_pretty_string(realized_config))
            return True
        except Exception, e:
            log_exception(e)
            raise MongoctlException("Unable to reconfigure "
                                    "replica set cluster '%s'. Cause: %s " %
                                    (self.id,e) )

    ###########################################################################
    def add_member_to_replica(self, server):
        self.configure_replicaset(add_server=server)


    ###########################################################################
    def get_replicaset_reconfig_db_command(self, add_server=None, force=False):
        current_rs_conf = self.read_rs_config()
        new_config = self.make_replset_config(add_server=add_server,
                                              current_rs_conf=current_rs_conf)
        if current_rs_conf is not None:
            # update the rs config version
            new_config['version'] = current_rs_conf['version'] + 1

        log_info("Current replica set configuration:\n %s" %
                 document_pretty_string(current_rs_conf))

        return {"replSetReconfig": new_config, "force": force}

    ###########################################################################
    def get_replicaset_init_all_db_command(self, only_for_server=None):
        replset_config = \
            self.make_replset_config(only_for_server=only_for_server)

        return {"replSetInitiate": replset_config}

    ###########################################################################
    def is_member_configured_for(self, server):
        member = self.get_member_for(server)
        mem_conf = member.get_member_repl_config()
        rs_conf = self.read_rs_config()
        return (rs_conf is not None and
                self.match_member_id(mem_conf, rs_conf['members']) is not None)

    ###########################################################################
    def has_any_server_that(self, predicate):
        def server_predicate(member):
            server = member.get_server()
            return predicate(server) if server is not None else False

        return len(filter(server_predicate, self.get_members())) > 0

    ###########################################################################
    def get_all_members_configs(self):
        member_configs = []
        for member in self.get_members():
            member_configs.append(member.get_member_repl_config())

        return member_configs

    ###########################################################################
    def validate_members(self, current_rs_conf):

        members = self.get_members()
        length = len(members)
        for member in members:
            # basic validation
            member.validate()

        for i in range(0, length):
            member = members[i]
            # validate member against other members
            for j in range(i+1, length):
                member.validate_against_other(members[j])

            # validate members against current config
            member.validate_against_current_config(current_rs_conf)


    ###########################################################################
    def make_replset_config(self,
                            only_for_server=None,
                            add_server=None,
                            current_rs_conf=None):

        # validate members first
        self.validate_members(current_rs_conf)
        member_confs = None
        if add_server is not None:
            member = self.get_member_for(add_server)
            member_confs = []
            member_confs.extend(current_rs_conf['members'])
            member_confs.append(member.get_member_repl_config())
        elif only_for_server is not None:
            member = self.get_member_for(only_for_server)
            member_confs = [member.get_member_repl_config()]
        else:
            member_confs = self.get_all_members_configs()

        # populate member ids when needed
        self.populate_member_conf_ids(member_confs, current_rs_conf)

        return {"_id" : self.id,
                "members": member_confs}

    ###########################################################################
    def populate_member_conf_ids(self, member_confs, current_rs_conf=None):
        new_id = 0
        current_member_confs = None
        if current_rs_conf is not None:
            current_member_confs = current_rs_conf['members']
            new_id = self.max_member_id(current_member_confs) + 1

        for mem_conf in member_confs:
            if mem_conf.get('_id') is None :
                member_id = self.match_member_id(mem_conf,
                                                 current_member_confs)

                # if there is no match then use increment
                if member_id is None:
                    member_id = new_id
                    new_id = new_id + 1

                mem_conf['_id'] = member_id

    ###########################################################################
    def match_member_id(self, member_conf, current_member_confs):
        """
        Attempts to find an id for member_conf where fom current members confs
        there exists a element.
        Returns the id of an element of current confs
        WHERE member_conf.host and element.host are EQUAL or map to same host
        """
        if current_member_confs is None:
            return None

        for curr_mem_conf in current_member_confs:
            if is_same_address(member_conf['host'], curr_mem_conf['host']):
                return curr_mem_conf['_id']

        return None

    ###########################################################################
    def max_member_id(self, member_confs):
        max_id = 0
        for mem_conf in member_confs:
            if mem_conf['_id'] > max_id:
                max_id = mem_conf['_id']
        return max_id

    ###########################################################################
    def read_rs_config(self):

        # first attempt to read the conf from the primary server

        log_debug("Attempting to read rs conf for cluster %s" % self.id)
        log_debug("Locating primary server...")
        primary_member = self.get_primary_member()
        if primary_member:
            log_debug("Reading rs conf from primary server %s." %
                      primary_member.get_server().id)
            rs_conf = primary_member.read_rs_config()
            log_debug("RS CONF: %s" % document_pretty_string(rs_conf))
            return rs_conf

        log_debug("No primary server found. Iterate on all members "
                  "until an rs conf is found...")
        # iterate on all members until you get a non null rs-config
        # Read from arbiters only when needed so skip members until the end

        arb_members = []
        for member in self.get_members():
            if member.is_arbiter():
                arb_members.append(member)
                continue
            else:
                rs_conf = member.read_rs_config()
                if rs_conf is not None:
                    return rs_conf

        # No luck yet... iterate on arbiters
        for arb in arb_members:
            rs_conf = arb.read_rs_config()
            if rs_conf is not None:
                return rs_conf

    ###########################################################################
    def get_sharded_cluster(self):
        return repository.lookup_cluster_by_shard(self)

    ###########################################################################
    def is_shard_member(self):
        return self.get_sharded_cluster() is not None

###############################################################################
def get_member_repl_lag(member_status, master_status):

    lag_in_seconds = abs(timedelta_total_seconds(
        member_status['optimeDate'] -
        master_status['optimeDate']))

    return lag_in_seconds


