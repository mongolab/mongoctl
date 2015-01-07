#
# The MIT License
#
# Copyright (c) 2012 ObjectLabs Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

__author__ = 'abdul'

import version

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
        },

        {
            "name": "version",
            "type" : "optional",
            "cmd_arg":  "--version",
            "nargs": 0,
            "help": "print version",
            "action": "version",
            "version": "mongoctl %s" % version.MONGOCTL_VERSION
        },

        {
            "name": "clientSslMode",
            "type": "optional",
            "help": "SSL mode of client (disabled, alloe, prefer, require)",
            "cmd_arg": [
                "--client-ssl-mode"
            ],
            "nargs": 1,
            "default": None
        },

        {
            "name": "useAltAddress",
            "type": "optional",
            "help": "Name of alternative address property to use when "
                    "connecting to servers instead of the 'address' property",
            "cmd_arg": [
                "--use-alt-address"
            ],
            "nargs": 1,
            "default": None
        },

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
        },
            {
            "name" :"miscCommands",
            "display": "Miscellaneous"
        },

        {
            "name" :"shardCommands",
            "display": "Sharding"
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
            "function": "mongoctl.commands.server.start.start_command",
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
                {
                    "name": "rsAddNoInit",
                    "type" : "optional",
                    "cmd_arg": "--rs-add-noinit",
                    "nargs": 0,
                    "help": "Automatically add server to an "
                            "initialized replicaset conf if "
                            "its not added yet",
                    "default": False
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }

                ]
        },
        #### stop ####
            {
            "prog": "stop",
            "group": "serverCommands",
            "shortDescription" : "stop a server",
            "description" : "Stops a specific server.",
            "function": "mongoctl.commands.server.stop.stop_command",
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
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        },
        #### restart ####
            {
            "prog": "restart",
            "group": "serverCommands",
            "shortDescription" : "restart a server",
            "description" : "Restarts a specific server.",
            "function": "mongoctl.commands.server.restart.restart_command",
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
                },

                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }

            ]
        },
        #### status ####
            {
            "prog": "status",
            "group": "serverCommands",
            "shortDescription" : "retrieve status of server or a cluster",
            "description" : "Retrieves the status of a server or a cluster",
            "function": "mongoctl.commands.common.status.status_command",
            "args":[
                    {   "name": "id",
                        "type" : "positional",
                        "nargs": 1,
                        "displayName": "[SERVER OR CLUSTER ID]",
                        "help": "A valid server or cluster id"
                },
                    {   "name": "statusVerbose",
                        "type" : "optional",
                        "cmd_arg": ["-v", "--verbose"],
                        "nargs": 0,
                        "help": "include more information in status"
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        },
        #### list-servers ####
            {
            "prog": "list-servers",
            "group": "serverCommands",
            "shortDescription" : "show list of configured servers",
            "description" : "Show list of configured servers.",
            "function": "mongoctl.commands.server.list_servers.list_servers_command"
        },
        #### show-server ####
            {
            "prog": "show-server",
            "group": "serverCommands",
            "shortDescription" : "show server's configuration",
            "description" : "Shows the configuration for a specific server.",
            "function": "mongoctl.commands.server.show.show_server_command" ,
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
            "description" : "Opens a mongo shell connection to the specified database. If a\n"
                            "cluster is specified command will connect to the primary server.\n\n"
                            "<db-address> can be one of:\n"
                            "   (a) a mongodb URI (e.g. mongodb://localhost:27017[/mydb])\n"
                            "   (b) <server-id>[/<db>]\n"
                            "   (c) <cluster-id>[/<db>]\n",
            "function": "mongoctl.commands.common.connect.connect_command",
            "args": [
                    {
                    "name": "dbAddress",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "<db-address>",
                    "help": "database addresses supported by mongoctl."
                            " Check docs for more details."
                },
                    {
                    "name": "jsFiles",
                    "type" : "positional",
                    "nargs": "*",
                    "displayName": "[file names (ending in .js)]",
                    "help": "file names: a list of files to run. files have to"
                            " end in .js and will exit after unless --shell"
                            " is specified"
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                },

                    {
                    "name": "shell",
                    "type" : "optional",
                    "help": "run the shell after executing files",
                    "cmd_arg": [
                        "--shell"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "norc",
                    "type" : "optional",
                    "help": 'will not run the ".mongorc.js" file on start up',
                    "cmd_arg": [
                        "--norc"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "quiet",
                    "type" : "optional",
                    "help": 'be less chatty',
                    "cmd_arg": [
                        "--quiet"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "eval",
                    "type" : "optional",
                    "help": 'evaluate javascript',
                    "cmd_arg": [
                        "--eval"
                    ],
                    "nargs": 1
                },

                    {
                    "name": "verbose",
                    "type" : "optional",
                    "help": 'increase verbosity',
                    "cmd_arg": [
                        "--verbose"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "ipv6",
                    "type" : "optional",
                    "help": 'enable IPv6 support (disabled by default)',
                    "cmd_arg": [
                        "--ipv6"
                    ],
                    "nargs": 0
                },



            ]
        },
        #### tail-log ####
            {
            "prog": "tail-log",
            "group": "serverCommands",
            "shortDescription" : "tails a server's log file",
            "description" : "Tails server's log file. Works only on local host",
            "function": "mongoctl.commands.server.tail_log.tail_log_command",
            "args": [
                    {
                    "name": "server",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SERVER_ID",
                    "help": "a valid server id"
                },
                    {
                    "name": "assumeLocal",
                    "type" : "optional",
                    "cmd_arg": "--assume-local",
                    "nargs": 0,
                    "help": "Assumes that the server is running on local"
                            " host. This will skip local address/dns check",
                    "default": False
                }
            ]
        },

        #### dump ####
            {
            "prog": "dump",
            "group": "clientCommands",
            "shortDescription" : "Export MongoDB data to BSON files (using mongodump)",
            "description" : "Runs a mongodump  to the specified database address or dbpath. If a\n"
                            "cluster is specified command will run the dump against "
                            "the primary server.\n\n"
                            "<db-address> can be one of:\n"
                            "   (a) a mongodb URI (e.g. mongodb://localhost:27017[/mydb])\n"
                            "   (b) <server-id>[/<db>]\n"
                            "   (c) <cluster-id>[/<db>]\n",
            "function": "mongoctl.commands.common.dump.dump_command",
            "args": [
                    {
                    "name": "target",
                    "displayName": "TARGET",
                    "type" : "positional",
                    "nargs": 1,
                    "help": "database address or dbpath. Check docs for"
                            " more details."
                },
                    {
                    "name": "useBestSecondary",
                    "type" : "optional",
                    "help": "Only for clusters. Dump from the best secondary "
                            "(passive / least repl lag)",
                    "cmd_arg": [
                        "--use-best-secondary"
                    ],
                    "nargs": 0
                },
                #   {
                #    "name": "maxReplLag",
                #    "type" : "optional",
                #    "help": "Used only with --use-best-secondary. Select "
                #            "members whose repl lag is less than than "
                #            "specified max ",
                #    "cmd_arg": [
                #        "--max-repl-lag"
                #    ],
                #    "nargs": 1
                #},
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                },

                    {
                    "name": "verbose",
                    "type" : "optional",
                    "help": 'increase verbosity',
                    "cmd_arg": [
                        "-v",
                        "--verbose"
                    ],
                    "nargs": 0
                },
                    {
                    "name": "directoryperdb",
                    "type" : "optional",
                    "help": "if dbpath specified, each db is in a separate directory",
                    "cmd_arg": [
                        "--directoryperdb"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "journal",
                    "type" : "optional",
                    "help": "enable journaling",
                    "cmd_arg": [
                        "--journal"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "collection",
                    "type" : "optional",
                    "displayName": "COLLECTION",
                    "help": "collection to use (some commands)",
                    "cmd_arg": [
                        "-c",
                        "--collection"
                    ],
                    "nargs": 1
                },

                    {
                    "name": "out",
                    "type" : "optional",
                    "displayName": "DIR",
                    "help": "output directory or '-' for stdout",
                    "cmd_arg": [
                        "-o",
                        "--out"
                    ],
                    "nargs": 1
                },

                    {
                    "name": "query",
                    "type" : "optional",
                    "displayName": "QUERY",
                    "help": "json query",
                    "cmd_arg": [
                        "-q",
                        "--query"
                    ],
                    "nargs": 1
                },

                    {
                    "name": "oplog",
                    "type" : "optional",
                    "help": " Use oplog for point-in-time snapshotting",
                    "cmd_arg": [
                        "--oplog"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "repair",
                    "type" : "optional",
                    "help": " try to recover a crashed database",
                    "cmd_arg": [
                        "--repair"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "forceTableScan",
                    "type" : "optional",
                    "help": " force a table scan (do not use $snapshot)",
                    "cmd_arg": [
                        "--forceTableScan"
                    ],
                    "nargs": 0
                },
                    {
                    "name": "ipv6",
                    "type" : "optional",
                    "cmd_arg":  "--ipv6",
                    "nargs": 0,
                    "help": "enable IPv6 support (disabled by default)"
                },
                    {
                    "name": "authenticationDatabase",
                    "type" : "optional",
                    "cmd_arg":  "--authenticationDatabase",
                    "nargs": 1,
                    "help": "user source (defaults to dbname). 2.4.x or greater only."
                },

                    {
                    "name": "dumpDbUsersAndRoles",
                    "type" : "optional",
                    "cmd_arg":  "--dumpDbUsersAndRoles",
                    "nargs": 0,
                    "help": "Dump user and role definitions for the given "
                            "database. 2.6.x or greater only."
                }

            ]
        },

        #### restore ####
            {
            "prog": "restore",
            "group": "clientCommands",
            "shortDescription" : "Restore MongoDB (using mongorestore)",
            "description" : "Runs a mongorestore from specified file or directory"
                            " to database address or dbpath. If a\n"
                            "cluster is specified command will restore against "
                            "the primary server.\n\n"
                            "<db-address> can be one of:\n"
                            "   (a) a mongodb URI (e.g. mongodb://localhost:27017[/mydb])\n"
                            "   (b) <server-id>[/<db>]\n"
                            "   (c) <cluster-id>[/<db>]\n",
            "function": "mongoctl.commands.common.restore.restore_command",
            "args": [
                    {
                    "name": "destination",
                    "displayName": "DESTINATION",
                    "type" : "positional",
                    "nargs": 1,
                    "help": "database address or dbpath. Check docs for"
                            " more details."
                },

                    {
                    "name": "source",
                    "displayName": "SOURCE",
                    "type" : "positional",
                    "nargs": 1,
                    "help": "directory or filename to restore from"
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                },

                    {
                    "name": "verbose",
                    "type" : "optional",
                    "help": 'increase verbosity',
                    "cmd_arg": [
                        "-v",
                        "--verbose"
                    ],
                    "nargs": 0
                },
                    {
                    "name": "directoryperdb",
                    "type" : "optional",
                    "help": "if dbpath specified, each db is in a separate directory",
                    "cmd_arg": [
                        "--directoryperdb"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "journal",
                    "type" : "optional",
                    "help": "enable journaling",
                    "cmd_arg": [
                        "--journal"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "collection",
                    "type" : "optional",
                    "displayName": "COLLECTION",
                    "help": " collection to use (some commands)",
                    "cmd_arg": [
                        "-c",
                        "--collection"
                    ],
                    "nargs": 1
                },


                    {
                    "name": "objcheck",
                    "type" : "optional",
                    "help": "validate object before inserting",
                    "cmd_arg": [
                        "--objectcheck"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "filter",
                    "type" : "optional",
                    "displayName": "FILTER",
                    "help": "filter to apply before inserting",
                    "cmd_arg": [
                        "--filter"
                    ],
                    "nargs": 1
                },

                    {
                    "name": "drop",
                    "type" : "optional",
                    "help": " drop each collection before import",
                    "cmd_arg": [
                        "--drop"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "oplogReplay",
                    "type" : "optional",
                    "help": "replay oplog for point-in-time restore",
                    "cmd_arg": [
                        "--oplogReplay"
                    ],
                    "nargs": 0
                },

                    {
                    "name": "keepIndexVersion",
                    "type" : "optional",
                    "help": " don't upgrade indexes to newest version",
                    "cmd_arg": [
                        "--keepIndexVersion"
                    ],
                    "nargs": 0
                },
                    {
                    "name": "ipv6",
                    "type" : "optional",
                    "cmd_arg":  "--ipv6",
                    "nargs": 0,
                    "help": "enable IPv6 support (disabled by default)"
                },

                {
                    "name": "authenticationDatabase",
                    "type" : "optional",
                    "cmd_arg":  "--authenticationDatabase",
                    "nargs": 1,
                    "help": "user source (defaults to dbname). 2.4.x or greater only."
                },

                {
                    "name": "restoreDbUsersAndRoles",
                    "type": "optional",
                    "cmd_arg":  "--restoreDbUsersAndRoles",
                    "nargs": 0,
                    "help": "Restore user and role definitions for the given "
                            "database. 2.6.x or greater only."
                },

                {
                    "name": "noIndexRestore",
                    "type": "optional",
                    "cmd_arg":  "--noIndexRestore",
                    "nargs": 0,
                    "help": "don't restore indexes"
                }

            ]
        },
        #### resync-secondary ####
            {
            "prog": "resync-secondary",
            "group": "serverCommands",
            "shortDescription" : "Resyncs a secondary member",
            "description" : "Resyncs a secondary member",
            "function": "mongoctl.commands.server.resync_secondary.resync_secondary_command",
            "args": [
                    {
                    "name": "server",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SERVER_ID",
                    "help": "a valid server id"
                },
                    {
                    "name": "assumeLocal",
                    "type" : "optional",
                    "cmd_arg": "--assume-local",
                    "nargs": 0,
                    "help": "Assumes that the server is running on local"
                            " host. This will skip local address/dns check",
                    "default": False
                },
                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
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
            "function": "mongoctl.commands.cluster.configure.configure_cluster_command",
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
                },

                    {
                    "name": "forcePrimaryServer",
                    "type" : "optional",
                    "displayName": "SERVER",
                    "cmd_arg":  [ "-f", "--force"],
                    "nargs": 1,
                    "help": "force member to become primary",
                    "default": None
                },

                    {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                    {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        },

        #### list-clusters ####
            {
            "prog": "list-clusters",
            "group": "clusterCommands",
            "shortDescription" : "show list of configured clusters",
            "description" : "Show list of configured servers",
            "function": "mongoctl.commands.cluster.list_clusters.list_clusters_command"
        },

        #### show-cluster ####
            {
            "prog": "show-cluster",
            "group": "clusterCommands",
            "shortDescription" : "show cluster's configuration",
            "description" : "Shows specific cluster's configuration",
            "function": "mongoctl.commands.cluster.show.show_cluster_command",
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

        #### install-mongodb ####
            {
            "prog": "install-mongodb",
            "group": "adminCommands",
            "shortDescription": "install MongoDB",
            "description": "install MongoDB",
            "function": "mongoctl.commands.misc.install.install_command",
            "args": [
                    {
                    "name": "version",
                    "type": "positional",
                    "nargs": "?",
                    "displayName": "VERSION",
                    "help": "MongoDB version to install"
                },

                {
                    "name": "edition",
                    "type": "optional",
                    "help": "edition (community (default) or community_ssl)",
                    "cmd_arg": [
                        "--edition"
                    ],
                    "nargs": 1
                },

                {
                    "name": "fromSource",
                    "type": "optional",
                    "help": "build from source",
                    "cmd_arg": [
                        "--build-from-source"
                    ],
                    "action": "store_true",
                    "default": False,
                    "nargs": 0
                },

                {
                    "name": "buildThreads",
                    "type": "optional",
                    "help": "Value for -j option of scons. Specifies the"
                            " number of jobs (commands) to run simultaneously."
                            " For more info, see 'man scons'",
                    "cmd_arg": [
                        "--build-threads"
                    ],
                    "nargs": 1
                },

                {
                    "name": "buildTmpDir",
                    "type": "optional",
                    "help": "build tmp dir (defaults to current working dir)",
                    "cmd_arg": [
                        "--build-tmp-dir"
                    ],
                    "default": None,
                    "nargs": 1
                },

                {
                    "name": "includeOnly",
                    "type": "optional",
                    "help": "Include only specified list of exes "
                            "(e.g --include-only mongod mongo)",
                    "cmd_arg": [
                        "--include-only"
                    ],
                    "default": None,
                    "nargs": "+",
                }
            ]
        },
        #### uninstall-mongodb ####
            {
            "prog": "uninstall-mongodb",
            "group": "adminCommands",
            "shortDescription" : "uninstall MongoDB",
            "description" : "uninstall MongoDB",
            "function": "mongoctl.commands.misc.install.uninstall_command",
            "args": [
                    {
                    "name": "version",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "VERSION",
                    "help": "MongoDB version to uninstall"
                } ,

                {
                    "name": "edition",
                    "type" : "optional",
                    "help": "edition (community (default) or enterprise)",
                    "cmd_arg": [
                        "--edition"
                    ],
                    "nargs": 1
                }
            ]
        },
        #### list-versions ####
            {
            "prog": "list-versions",
            "group": "adminCommands",
            "shortDescription": "list all available MongoDB installations on"
                                " this machine",
            "description": "list all available MongoDB installations on"
                           " this machine",
            "function": "mongoctl.commands.misc.install.list_versions_command",
        },

        #### install-mongodb ####
        {
            "prog": "publish-mongodb",
            "group": "adminCommands",
            "shortDescription": "Publish MongoDB to a binary repository",
            "description": "Publish MongoDB to a binary repository",
            "function": "mongoctl.commands.misc.publish_mongodb."
                        "publish_mongodb_command",
            "args": [

                {
                    "name": "repo",
                    "type": "positional",
                    "nargs": 1,
                    "displayName": "REPO",
                    "help": "Repo name"
                },

                {
                    "name": "version",
                    "type": "positional",
                    "nargs": 1,
                    "displayName": "VERSION",
                    "help": "MongoDB version to build"
                },

                {
                    "name": "edition",
                    "type": "optional",
                    "help": "edition",
                    "cmd_arg": [
                        "--edition"
                    ],
                    "nargs": 1
                },


                {
                    "name": "accessKey",
                    "type": "optional",
                    "help": "S3 access key",
                    "cmd_arg": [
                        "--access-key"
                    ],
                    "nargs": 1
                },


                {
                    "name": "secretKey",
                    "type": "optional",
                    "help": "S3 secret key",
                    "cmd_arg": [
                        "--secret-key"
                    ],
                    "nargs": 1
                },


            ]
        },
        #### print-uri ####
        {
        "prog": "print-uri",
        "group": "miscCommands",
        "shortDescription" : "prints connection URI for a"
                             " server or cluster",
        "description" : "Prints MongoDB connection URI of the specified"
                        " server or clurter",
        "function": "mongoctl.commands.misc.print_uri.print_uri_command",
        "args": [
                {
                "name": "id",
                "type" : "positional",
                "nargs": 1,
                "displayName": "SERVER or CLUSTER ID",
                "help": "Server or cluster id"
            },
                {
                "name": "db",
                "type" : "optional",
                "help": "database name",
                "cmd_arg": [
                    "-d",
                    "--db"
                ],
                "nargs": 1
            }
        ]
        },

        {
            "prog": "add-shard",
            "group": "shardCommands",
            "shortDescription" : "Adds specified shard to sharded cluster",
            "description" : "Adds specified shard to sharded cluster",
            "function": "mongoctl.commands.sharding.sharding.add_shard_command",
            "args": [
                {
                    "name": "shardId",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SHARD_ID",
                    "help": "A valid shard cluster id or shard server id"
                },

                {
                    "name": "dryRun",
                    "type" : "optional",
                    "cmd_arg":  ["-n" , "--dry-run"],
                    "nargs": 0,
                    "help": "prints configure cluster db command to execute "
                            "without executing it",
                    "default": False
                },

                {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        },

        {
            "prog": "remove-shard",
            "group": "shardCommands",
            "shortDescription": "Removes shard from sharded cluster",
            "description": "Removes shard from sharded cluster",
            "function": "mongoctl.commands.sharding.sharding.remove_shard_command",
            "args": [
                {
                    "name": "shardId",
                    "type" : "positional",
                    "nargs": 1,
                    "displayName": "SHARD_ID",
                    "help": "A valid shard cluster id or shard server id"
                },

                {
                    "name": "dryRun",
                    "type" : "optional",
                    "cmd_arg":  ["-n" , "--dry-run"],
                    "nargs": 0,
                    "help": "prints db command to execute "
                            "without executing it",
                    "default": False
                },

                {
                    "name": "unshardedDataDestination",
                    "displayName": "SHARD_ID",
                    "type" : "optional",
                    "cmd_arg":  ["--move-unsharded-data-to"],
                    "nargs": 1,
                    "help": "Moves unsharded to data to specified shard id",
                    "default": None
                },

                {
                    "name": "synchronized",
                    "type" : "optional",
                    "cmd_arg": ["--synchronized"],
                    "nargs": 0,
                    "help": "synchronized",
                    "default": False
                },

                {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        },
        #### configure-cluster ####
        {
            "prog": "configure-shard-cluster",
            "group": "shardCommands",
            "shortDescription" : "configures a sharded cluster",
            "description" : "configures a sharded cluster",
            "function": "mongoctl.commands.sharding.sharding.configure_sharded_cluster_command",
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
                },

                {
                    "name": "username",
                    "type" : "optional",
                    "help": "admin username",
                    "cmd_arg": [
                        "-u"
                    ],
                    "nargs": 1
                },
                {
                    "name": "password",
                    "type" : "optional",
                    "help": "admin password",
                    "cmd_arg": [
                        "-p"
                    ],
                    "nargs": "?"
                }
            ]
        }


        ]

}



MONGOD_OPTIONS = [
    # mongod supported options
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

    {
        "name": "setParameter",
        "type" : "optional",
        "cmd_arg":  "--setParameter",
        "nargs": 1,
        "help": "Set a configurable parameter"
    },
    ########### SSL OPTIONS
    {
        "name": "sslOnNormalPorts",
        "type": "optional",
        "cmd_arg":  "--sslOnNormalPorts",
        "nargs": 0,
        "help": "use ssl on configured ports"
    },

    {
        "name": "sslMode",
        "type": "optional",
        "cmd_arg":  "--sslMode",
        "nargs": 1,
        "help": "set the SSL operation mode (disabled|allowSSL|preferSSL|requireSSL)"
    },

    {
        "name": "sslPEMKeyFile",
        "type": "optional",
        "cmd_arg":  "--sslPEMKeyFile",
        "nargs": 1,
        "help": "PEM file for ssl"
    },

    {
        "name": "sslPEMKeyPassword",
        "type": "optional",
        "cmd_arg":  "--sslPEMKeyPassword",
        "nargs": 1,
        "help": "PEM file password"
    },

    {
        "name": "sslClusterFile",
        "type": "optional",
        "cmd_arg":  "--sslClusterFile",
        "nargs": 1,
        "help": "Key file for internal SSL authentication"
    },

    {
        "name": "sslClusterPassword",
        "type": "optional",
        "cmd_arg":  "--sslClusterPassword",
        "nargs": 1,
        "help": "Internal authentication key file password"
    },

    {
        "name": "sslCAFile",
        "type": "optional",
        "cmd_arg":  "--sslCAFile",
        "nargs": 1,
        "help": "Certificate Authority file for SSL"
    },

    {
        "name": "sslCRLFile",
        "type": "optional",
        "cmd_arg":  "--sslCRLFile",
        "nargs": 1,
        "help": "Certificate Revocation List file for SSL"
    },

    {
        "name": "sslWeakCertificateValidation",
        "type": "optional",
        "cmd_arg":  "--sslWeakCertificateValidation",
        "nargs": 0,
        "help": "allow client to connect without presenting a certificate"
    },

    {
        "name": "sslAllowInvalidCertificates",
        "type": "optional",
        "cmd_arg":  "--sslAllowInvalidCertificates",
        "nargs": 0,
        "help": "allow connections to servers with invalid certificates"
    },

    {
        "name": "sslFIPSMode",
        "type": "optional",
        "cmd_arg":  "--sslFIPSMode",
        "nargs": 0,
        "help": "activate FIPS 140-2 mode at startup"
    }


]


###############################################################################
MONGOD_OPTION_NAMES = map(lambda option: option["name"], MONGOD_OPTIONS)

###############################################################################

def _add_options(options, command_name):
    command_parser_def = filter(
        lambda command_parser: command_parser["prog"] == command_name,
        MONGOCTL_PARSER_DEF["children"])[0]

    command_parser_def["args"].extend(options)

###############################################################################
## Add mongod options to start/restart commands
_add_options(MONGOD_OPTIONS, "start")
_add_options(MONGOD_OPTIONS, "restart")