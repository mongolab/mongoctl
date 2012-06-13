# The MIT License

# Copyright (c) 2012 ObjectLabs Corporation

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

__author__ = 'abdul'

MONGOCTL_PARSER_DEF = {
    "prog": "mongoctl",
    "usage": "Usage: mongoctl [<options>] <command> [<command-args>]",
    "description" : "A utility that simplifies the management of MongoDB servers and replica set clusters.",
    "args": [
            {
            "name": "mongoctlVerbose",
            "type" : "optional",
            "help": "make mongoctl more verbose",
            "cmd_arg": [
                "-v",
                "--verbose"
            ],
            "nargs": 0,
            "action": "store_true",
            "default": False
        },
            {
            "name": "noninteractive",
            "type" : "optional",
            "help": "bypass prompting for user interaction",
            "cmd_arg": [
                "-n",
                "--noninteractive"
            ],
            "nargs": 0,
            "action": "store_true",
            "default": False
        },

            {
            "name": "yesToEverything",
            "type" : "optional",
            "help": "auto yes to all yes/no prompts",
            "cmd_arg": [
                "--yes"
            ],
            "nargs": 0,
            "action": "store_true",
            "default": False
        },

            {
            "name": "noToEverything",
            "type" : "optional",
            "help": "auto no to all yes/no prompts",
            "cmd_arg": [
                "--no"
            ],
            "nargs": 0,
            "action": "store_true",
            "default": False
        },
            {
            "name": "configRoot",
            "type" : "optional",
            "help": "path to mongoctl config root; defaults to ~/.mongoctl",
            "cmd_arg": [
                "--config-root"
            ],
            "nargs": 1
        }

    ],

    "child_groups": [
            {
            "name" :"adminCommands",
            "display": "Admin Commands"
        },
            {
            "name" :"clientCommands",
            "display": "Client Commands"
        },
            {
            "name" :"serverCommands",
            "display": "Server Commands"
        },

            {
            "name" :"clusterCommands",
            "display": "Cluster Commands"
        }
    ],

    "children":[

        #### start ####
            {
            "prog": "start",
            "group": "serverCommands",
            #"usage" : generate default usage
            "shortDescription" : "start a server",
            "description" : "Starts a specific server.",
            "function": "mongoctl.mongoctl.start_command",
            "args":[

                    {
                    "name": "server",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SERVER_ID",
                    "help": "a valid server id"
                },

                    {
                    "name": "dryRun",
                    "type" : "optional",
                    "cmd_arg":  ["-n" , "--dry-run"],
                    "nargs": 0,
                    "help": "prints the mongod command to execute without "
                            "executing it",
                    "default": False
                },
                    {
                    "name": "assumeLocal",
                    "type" : "optional",
                    "cmd_arg": "--assume-local",
                    "nargs": 0,
                    "help": "Assumes that the server will be started on local"
                            " host. This will skip local address/dns check",
                    "default": False
                },
                    {
                    "name": "rsAdd",
                    "type" : "optional",
                    "cmd_arg": "--rs-add",
                    "nargs": 0,
                    "help": "Automatically add server to replicaset conf if "
                            "its not added yet",
                    "default": False
                },
                # confusing
                #                {
                #                    "name": "config",
                #                    "type" : "optional",
                #                    "cmd_arg":  ["-f", "--config"],
                #                    "nargs": 1,
                #                    "help": "configuration file specifying additional options"
                #                },

                    {
                    "name": "verbose",
                    "type" : "optional",
                    "cmd_arg":  ["-v", "--verbose"],
                    "nargs": 0,
                    "help": "be more verbose (include multiple times for more"
                            " verbosity e.g. -vvvvv)"
                },

                    {
                    "name": "quiet",
                    "type" : "optional",
                    "cmd_arg":  "--quiet",
                    "nargs": 0,
                    "help": "quieter output"
                },

                    {
                    "name": "port",
                    "type" : "optional",
                    "cmd_arg":  "--port",
                    "nargs": 1,
                    "help": "specify port number"
                },

                    {
                    "name": "bind_ip",
                    "type" : "optional",
                    "cmd_arg": "--bind_ip",
                    "nargs": 1,
                    "help": "comma separated list of ip addresses to listen "
                            "on- all local ips by default"
                },

                    {
                    "name": "maxConns",
                    "type" : "optional",
                    "cmd_arg":  "--maxConns",
                    "nargs": 1,
                    "help": "max number of simultaneous connections"
                },

                    {
                    "name": "objcheck",
                    "type" : "optional",
                    "cmd_arg":  "--objcheck",
                    "nargs": 0,
                    "help": "inspect client data for validity on receipt"
                },

                    {
                    "name": "logpath",
                    "type" : "optional",
                    "cmd_arg":  "--logpath",
                    "nargs": 1,
                    "help": "log file to send write to instead of stdout -"
                            " has to be a file, not directory. "
                            "mongoctl defaults that to dbpath/mongodb.log"
                },

                    {
                    "name": "logappend",
                    "type" : "optional",
                    "cmd_arg":  "--logappend",
                    "nargs": 1,
                    "help": "append to logpath instead of over-writing"
                },

                    {
                    "name": "pidfilepath",
                    "type" : "optional",
                    "cmd_arg":  "--pidfilepath",
                    "nargs": 1,
                    "help": "full path to pidfile (if not set,"
                            " no pidfile is created). "
                            "mongoctl defaults that to dbpath/pid.txt"
                },

                    {
                    "name": "keyFile",
                    "type" : "optional",
                    "cmd_arg":  "--keyFile",
                    "nargs": 1,
                    "help": "private key for cluster authentication "
                            "(only for replica sets)"
                },

                    {
                    "name": "nounixsocket",
                    "type" : "optional",
                    "cmd_arg":  "--nounixsocket",
                    "nargs": 0,
                    "help": "disable listening on unix sockets"
                },

                    {
                    "name": "unixSocketPrefix",
                    "type" : "optional",
                    "cmd_arg":  "--unixSocketPrefix",
                    "nargs": 1,
                    "help": "alternative directory for UNIX domain sockets "
                            "(defaults to /tmp)"
                },

                    {
                    "name": "fork",
                    "type" : "optional",
                    "cmd_arg":  "--fork",
                    "nargs": 0,
                    "help": "noop; mongoctl will always fork the mongod process",
                    "default": None
                },

                    {
                    "name": "auth",
                    "type" : "optional",
                    "cmd_arg":  "--auth",
                    "nargs": 0,
                    "help": "run with security"
                },

                    {
                    "name": "cpu",
                    "type" : "optional",
                    "cmd_arg":  "--cpu",
                    "nargs": 0,
                    "help": "periodically show cpu and iowait utilization"
                },

                    {
                    "name": "dbpath",
                    "type" : "optional",
                    "cmd_arg":  "--dbpath",
                    "nargs": 1,
                    "help": "directory for datafiles"
                },

                    {
                    "name": "diaglog",
                    "type" : "optional",
                    "cmd_arg":  "--diaglog",
                    "nargs": 1,
                    "help": "0=off 1=W 2=R 3=both 7=W+some reads"
                },

                    {
                    "name": "directoryperdb",
                    "type" : "optional",
                    "cmd_arg":  "--directoryperdb",
                    "nargs": 0,
                    "help": "each database will be stored in a"
                            " separate directory"
                },

                    {
                    "name": "journal",
                    "type" : "optional",
                    "cmd_arg":  "--journal",
                    "nargs": 0,
                    "help": "enable journaling"
                },

                    {
                    "name": "journalOptions",
                    "type" : "optional",
                    "cmd_arg":  "--journalOptions",
                    "nargs": 1,
                    "help": "journal diagnostic options"
                },

                    {
                    "name": "journalCommitInterval",
                    "type" : "optional",
                    "cmd_arg":  "--journalCommitInterval",
                    "nargs": 1,
                    "help": "how often to group/batch commit (ms)"
                },

                    {
                    "name": "ipv6",
                    "type" : "optional",
                    "cmd_arg":  "--ipv6",
                    "nargs": 0,
                    "help": "enable IPv6 support (disabled by default)"
                },

                    {
                    "name": "jsonp",
                    "type" : "optional",
                    "cmd_arg":  "--jsonp",
                    "nargs": 0,
                    "help": "allow JSONP access via http "
                            "(has security implications)"
                },

                    {
                    "name": "noauth",
                    "type" : "optional",
                    "cmd_arg":  "--noauth",
                    "nargs": 0,
                    "help": "run without security"
                },

                    {
                    "name": "nohttpinterface",
                    "type" : "optional",
                    "cmd_arg":  "--nohttpinterface",
                    "nargs": 0,
                    "help": "disable http interface"
                },

                    {
                    "name": "nojournal",
                    "type" : "optional",
                    "cmd_arg":  "--nojournal",
                    "nargs": 0,
                    "help": "disable journaling (journaling is on by default "
                            "for 64 bit)"
                },

                    {
                    "name": "noprealloc",
                    "type" : "optional",
                    "cmd_arg":  "--noprealloc",
                    "nargs": 0,
                    "help": "disable data file preallocation - "
                            "will often hurt performance"
                },

                    {
                    "name": "notablescan",
                    "type" : "optional",
                    "cmd_arg":  "--notablescan",
                    "nargs": 0,
                    "help": "do not allow table scans"
                },

                    {
                    "name": "nssize",
                    "type" : "optional",
                    "cmd_arg":  "--nssize",
                    "nargs": 1,
                    "help": ".ns file size (in MB) for new databases"
                },

                    {
                    "name": "profile",
                    "type" : "optional",
                    "cmd_arg":  "--profile",
                    "nargs": 1,
                    "help": "0=off 1=slow, 2=all"
                },

                    {
                    "name": "quota",
                    "type" : "optional",
                    "cmd_arg":  "--quota",
                    "nargs": 0,
                    "help": "limits each database to a certain number"
                            " of files (8 default)"
                },

                    {
                    "name": "quotaFiles",
                    "type" : "optional",
                    "cmd_arg":  "--quotaFiles",
                    "nargs": 1,
                    "help": "number of files allower per db, requires --quota"
                },

                    {
                    "name": "rest",
                    "type" : "optional",
                    "cmd_arg":  "--rest",
                    "nargs": 1,
                    "help": "turn on simple rest api"
                },

                    {
                    "name": "repair",
                    "type" : "optional",
                    "cmd_arg":  "--repair",
                    "nargs": 0,
                    "help": "run repair on all dbs"
                },

                    {
                    "name": "repairpath",
                    "type" : "optional",
                    "cmd_arg":  "--repairpath",
                    "nargs": 1,
                    "help": "root directory for repair files - defaults "
                            "to dbpath"
                },

                    {
                    "name": "slowms",
                    "type" : "optional",
                    "cmd_arg":  "--slowms",
                    "nargs": 1,
                    "help": "value of slow for profile and console log"
                },

                    {
                    "name": "smallfiles",
                    "type" : "optional",
                    "cmd_arg":  "--smallfiles",
                    "nargs": 0,
                    "help": "use a smaller default file size"
                },

                    {
                    "name": "syncdelay",
                    "type" : "optional",
                    "cmd_arg":  "--syncdelay",
                    "nargs": 1,
                    "help": "seconds between disk syncs "
                            "(0=never, but not recommended)"
                },

                    {
                    "name": "sysinfo",
                    "type" : "optional",
                    "cmd_arg":  "--sysinfo",
                    "nargs": 0,
                    "help": "print some diagnostic system information"
                },

                    {
                    "name": "upgrade",
                    "type" : "optional",
                    "cmd_arg":  "--upgrade",
                    "nargs": 0,
                    "help": "upgrade db if needed"
                },

                    {
                    "name": "fastsync",
                    "type" : "optional",
                    "cmd_arg":  "--fastsync",
                    "nargs": 0,
                    "help": "indicate that this instance is starting from "
                            "a dbpath snapshot of the repl peer"
                },

                    {
                    "name": "oplogSize",
                    "type" : "optional",
                    "cmd_arg":  "--oplogSize",
                    "nargs": 1,
                    "help": "size limit (in MB) for op log"
                },

                    {
                    "name": "master",
                    "type" : "optional",
                    "cmd_arg":  "--master",
                    "nargs": 0,
                    "help": "master mode"
                },

                    {
                    "name": "slave",
                    "type" : "optional",
                    "cmd_arg":  "--slave",
                    "nargs": 0,
                    "help": "slave mode"
                },

                    {
                    "name": "source",
                    "type" : "optional",
                    "cmd_arg":  "--source",
                    "nargs": 1,
                    "help": "when slave: specify master as <server:port>"
                },

                    {
                    "name": "only",
                    "type" : "optional",
                    "cmd_arg":  "--only",
                    "nargs": 1,
                    "help": "when slave: specify a single database"
                            " to replicate"
                },

                    {
                    "name": "slavedelay",
                    "type" : "optional",
                    "cmd_arg":  "--slavedelay",
                    "nargs": 1,
                    "help": "specify delay (in seconds) to be used when "
                            "applying master ops to slave"
                },

                    {
                    "name": "autoresync",
                    "type" : "optional",
                    "cmd_arg":  "--autoresync",
                    "nargs": 0,
                    "help": "automatically resync if slave data is stale"
                },

                    {
                    "name": "replSet",
                    "type" : "optional",
                    "cmd_arg":  "--replSet",
                    "nargs": 1,
                    "help": "arg is <setname>[/<optionalseedhostlist>]"
                },

                    {
                    "name": "configsvr",
                    "type" : "optional",
                    "cmd_arg":  "--configsvr",
                    "nargs": 0,
                    "help": "declare this is a config db of a cluster;"
                            " default port 27019; default dir /data/configdb"
                },

                    {
                    "name": "shardsvr",
                    "type" : "optional",
                    "cmd_arg":  "--shardsvr",
                    "nargs": 0,
                    "help": "declare this is a shard db of a cluster;"
                            " default port 27018"
                },

                    {
                    "name": "noMoveParanoia",
                    "type" : "optional",
                    "cmd_arg":  "--noMoveParanoia",
                    "nargs": 0,
                    "help": "turn off paranoid saving of data for moveChunk."
                            " this is on by default for now,"
                            " but default will switch"
                },

                ]
        },
        #### stop ####
            {
            "prog": "stop",
            "group": "serverCommands",
            "shortDescription" : "stop a server",
            "description" : "Stops a specific server.",
            "function": "mongoctl.mongoctl.stop_command",
            "args":[
                    {   "name": "server",
                        "type" : "positional",
                        "nargs": 1,
                        "displayName": "SERVER_ID",
                        "help": "A valid server id"
                },
                    {   "name": "forceStop",
                        "type": "optional",
                        "cmd_arg": ["-f", "--force"],
                        "nargs": 0,
                        "help": "force stop if needed via kill",
                        "default": False
                },
                    {
                    "name": "assumeLocal",
                    "type" : "optional",
                    "cmd_arg": "--assume-local",
                    "nargs": 0,
                    "help": "Assumes that the server will be stopped on local"
                            " host. This will skip local address/dns check",
                    "default": False
                }
            ]
        },
        #### restart ####
            {
            "prog": "restart",
            "group": "serverCommands",
            "shortDescription" : "restart a server",
            "description" : "Restarts a specific server.",
            "function": "mongoctl.mongoctl.restart_command",
            "args":[
                    {   "name": "server",
                        "type" : "positional",
                        "nargs": 1,
                        "displayName": "SERVER_ID",
                        "help": "A valid server id"
                },
                    {
                    "name": "assumeLocal",
                    "type" : "optional",
                    "cmd_arg": "--assume-local",
                    "nargs": 0,
                    "help": "Assumes that the server will be stopped on local"
                            " host. This will skip local address/dns check",
                    "default": False
                }
            ]
        },
        #### status ####
            {
            "prog": "status",
            "group": "serverCommands",
            "shortDescription" : "retrieve status of server",
            "description" : "Retrieves the status of a server.",
            "function": "mongoctl.mongoctl.status_command",
            "args":[
                    {   "name": "server",
                        "type" : "positional",
                        "nargs": 1,
                        "displayName": "SERVER_ID",
                        "help": "A valid server id"
                },
                    {   "name": "statusVerbose",
                        "type" : "optional",
                        "cmd_arg": ["-v", "--verbose"],
                        "nargs": 0,
                        "help": "include more information in status"
                }
            ]
        },
        #### list-servers ####
            {
            "prog": "list-servers",
            "group": "serverCommands",
            "shortDescription" : "show list of configured servers",
            "description" : "Shows a list of configured servers.",
            "function": "mongoctl.mongoctl.list_servers_command"
        },
        #### show-server ####
            {
            "prog": "show-server",
            "group": "serverCommands",
            "shortDescription" : "show server's configuration",
            "description" : "Shows the configuration for a specific server.",
            "function": "mongoctl.mongoctl.show_server_command" ,
            "args":[
                    {   "name": "server",
                        "type" : "positional",
                        "nargs": 1,
                        "displayName": "SERVER_ID",
                        "help": "A valid server id"
                }
            ]
        },
        #### connect ####
            {
            "prog": "connect",
            "group": "clientCommands",
            "shortDescription" : "open a mongo shell connection to a server",
            "description" : "Opens a mongo shell connection to a specific server. "
                            "Uses the 'address' \nfield of the specified server "
                            "config if specified, otherwise \ntries to connect "
                            "to the configured port on localhost.",
            "function": "mongoctl.mongoctl.connect_command",
            "args": [
                    {
                    "name": "server",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SERVER_ID",
                    "help": "A valid server id"
                }
            ]
        },
        #### configure-cluster ####
            {
            "prog": "configure-cluster",
            "group": "clusterCommands",
            "shortDescription" : "initiate or reconfigure a cluster",
            "description" : "Initiaties or reconfigures a specific replica set cluster. "
                            "This command is \nused both to initiate the "
                            "cluster for the first time \nand to reconfigure "
                            "the cluster.",
            "function": "mongoctl.mongoctl.configure_cluster_command",
            "args": [
                    {
                    "name": "cluster",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "CLUSTER_ID",
                    "help": "A valid cluster id"
                },

                    {
                    "name": "dryRun",
                    "type" : "optional",
                    "cmd_arg":  ["-n" , "--dry-run"],
                    "nargs": 0,
                    "help": "prints configure cluster db command to execute "
                            "without executing it",
                    "default": False
                }
            ]
        },
        #### list-clusters ####
            {
            "prog": "list-clusters",
            "group": "clusterCommands",
            "shortDescription" : "show list of configured clusters",
            "description" : "Shows a list configured clusters.",
            "function": "mongoctl.mongoctl.list_clusters_command"
        },

        #### show-cluster ####
            {
            "prog": "show-cluster",
            "group": "clusterCommands",
            "shortDescription" : "show cluster's configuration",
            "description" : "Shows specific cluster's configuration",
            "function": "mongoctl.mongoctl.show_cluster_command",
            "args": [
                    {
                    "name": "cluster",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "CLUSTER_ID",
                    "help": "A valid cluster id"
                }
            ]
        },

        #### install ####
            {
            "prog": "install",
            "group": "adminCommands",
            "shortDescription" : "install MongoDB",
            "description" : "install MongoDB",
            "function": "mongoctl.mongoctl.install_command",
            "args": [
                    {
                    "name": "version",
                    "type" : "positional",
                    "nargs": "?",
                    "displayName": "VERSION",
                    "help": "MongoDB version to install"
                }
            ]
        },
        #### uninstall ####
            {
            "prog": "uninstall",
            "group": "adminCommands",
            "shortDescription" : "uninstall MongoDB",
            "description" : "uninstall MongoDB",
            "function": "mongoctl.mongoctl.uninstall_command",
            "args": [
                    {
                    "name": "version",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "VERSION",
                    "help": "MongoDB version to uninstall"
                }
            ]
        },
        #### list-versions ####
            {
            "prog": "list-versions",
            "group": "adminCommands",
            "shortDescription" : "list all available MongoDB installations on"
                                 " this machine",
            "description" : "list all available MongoDB installations on"
                            " this machine",
            "function": "mongoctl.mongoctl.list_versions_command",
        }


        ]

}

