Command reference
--------------------

#### mongoctl

```
Usage: mongoctl [<options>] <command> [<command-args>]

A utility that simplifies the management of MongoDB servers and replica set clusters.

Options:
  -h, --help            show this help message and exit
  -v, --verbose         make mongoctl more verbose
  -n, --noninteractive  bypass prompting for user interaction
  --yes                 auto yes to all yes/no prompts
  --no                  auto no to all yes/no prompts
  --config-root CONFIGROOT
                        path to mongoctl config root; defaults to ~/.mongoctl

Commands:
  Admin Commands:
    install-mongodb           - install MongoDB
    uninstall-mongodb         - uninstall MongoDB
    list-versions             - list all available MongoDB installations on this machine

  Client Commands:
    connect                   - open a mongo shell connection to a server
    dump                      - Export MongoDB data to BSON files (using mongodump)
    restore                   - Restore MongoDB (using mongorestore)

  Server Commands:
    start                     - start a server
    stop                      - stop a server
    restart                   - restart a server
    status                    - retrieve status of server
    list-servers              - show list of configured servers
    show-server               - show server's configuration
    tail-log                  - tails a server's log file
    resync-secondary          - Resyncs a secondary member

  Cluster Commands:
    configure-cluster         - initiate or reconfigure a cluster
    list-clusters             - show list of configured clusters
    show-cluster              - show cluster's configuration

  Miscellaneous:
    print-uri                 - prints connection URI for a server or cluster

See 'mongoctl <command> --help' for more help on a specific command.
```

Admin commands
---------------

##### install-mongodb
```
Usage: install-mongodb <version>

Install the specified version of MongoDB

Options:
  -h, --help  show this help message and exit
```

##### uninstall-mongodb
```
Usage: uninstall <version>

Uninstall the specified version of MongoDB

Options:
  -h, --help  show this help message and exit
```

##### list-versions
```
Usage: list-versions 

List all available MongoDB installations on this machine

Options:
  -h, --help  show this help message and exit
```

Client commands
-----------------

#### connect 

```
Usage: connect [<options>] <db-address> [<js-files>]

Opens a mongo shell connection to the specified database. If a
cluster is specified command will connect to the primary server.

<db-address> can be one of:
   (a) a mongodb URI (e.g. mongodb://localhost:27017/mydb)
   (b) <server-id>/<db>
   (c) <cluster-id>/<db> (not yet supported)

Options:
  -h, --help     show this help message and exit
  -u USERNAME    username
  -p PASSWORD    password
  --shell        run the shell after executing files
  --norc         will not run the ".mongorc.js" file on start up
  --quiet        be less chatty
  --eval EVAL    evaluate javascript
  --verbose      increase verbosity
  --ipv6         enable IPv6 support (disabled by default)
```

#### dump

```
Usage: dump [<options>] TARGET

Runs a mongodump  to the specified database address or dbpath. If a
cluster is specified command will run the dump against the primary server.

<db-address> can be one of:
   (a) a mongodb URI (e.g. mongodb://localhost:27017[/mydb])
   (b) <server-id>[/<db>]
   (c) <cluster-id>[/<db>]


Arguments:
  TARGET  database addresse or dbpath. Check docs for more details.

Options:
  -h, --help            show this help message and exit
  -u USERNAME           username
  -p [PASSWORD]         password
  -v, --verbose         increase verbosity
  --directoryperdb      if dbpath specified, each db is in a separate
                        directory
  --journal             enable journaling
  -c COLLECTION, --collection COLLECTION
                        collection to use (some commands)
  -o DIR, --out DIR     output directory or '-' for stdout
  -q QUERY, --query QUERY
                        json query
  --oplog               Use oplog for point-in-time snapshotting
  --repair              try to recover a crashed database
  --forceTableScan      force a table scan (do not use $snapshot)
  --ipv6                enable IPv6 support (disabled by default)
```

##### restore

```
Usage: restore [<options>] DESTINATION SOURCE

Runs a mongorestore from specified file or directory to database address or dbpath. If a
cluster is specified command will restore against the primary server.

<db-address> can be one of:
   (a) a mongodb URI (e.g. mongodb://localhost:27017[/mydb])
   (b) <server-id>[/<db>]
   (c) <cluster-id>[/<db>]


Arguments:
  DESTINATION  database address or dbpath. Check docs for more details.
  SOURCE       directory or filename to restore from

Options:
  -h, --help            show this help message and exit
  -u USERNAME           username
  -p [PASSWORD]         password
  -v, --verbose         increase verbosity
  --directoryperdb      if dbpath specified, each db is in a separate
                        directory
  --journal             enable journaling
  -c COLLECTION, --collection COLLECTION
                        collection to use (some commands)
  --objectcheck         validate object before inserting
  --filter FILTER       filter to apply before inserting
  --drop                drop each collection before import
  --oplogReplay         replay oplog for point-in-time restore
  --keepIndexVersion    don't upgrade indexes to newest version
  --ipv6                enable IPv6 support (disabled by default)
```

Server commands
-----------------

##### start

The ```start``` command calls ```mongod``` with arguments and options
based on the configuration of the specified server document and its
configured ```cmdOptions```.  You can see the generated ```mongod```
command-line string by calling ```start``` with the dry run 
option (```-n``` or ```--dry-run```).

```start``` allows you to override all ```cmdOptions``` defined in the
specified server configuration via options specified at the
command-line. This is useful for one-off situations (i.e. running 
a ```--repair```). In general, ```start``` supports all of the command-line
options of ```mongod```.

```
Usage: start [<options>] <server>

Start a server

Options:
  -h, --help            show this help message and exit
  --version VERSION     show version information
  -n, --dry-run         prints the mongod command to execute without executing
                        it
  --rs-add              Automatically add server to replicaset conf if its not
                        added yet
  -u USERNAME           admin username
  -p [PASSWORD]         admin password

  -v, --verbose         be more verbose (include multiple times for more
                        verbosity e.g. -vvvvv)
  --quiet               quieter output
  --port PORT           specify port number
  --bind_ip BIND_IP     comma separated list of ip addresses to listen on- all
                        local ips by default
  --maxConns MAXCONNS   max number of simultaneous connections
  --objcheck            inspect client data for validity on receipt
  --logpath LOGPATH     log file to send write to instead of stdout - has to
                        be a file, not directory. mongoctl defaults that to
                        dbpath/mongodb.log
  --logappend LOGAPPEND
                        append to logpath instead of over-writing
  --pidfilepath PIDFILEPATH
                        full path to pidfile (if not set, no pidfile is
                        created). mongoctl defaults that to dbpath/pid.txt
  --keyFile KEYFILE     private key for cluster authentication (only for
                        replica sets)
  --nounixsocket        disable listening on unix sockets
  --unixSocketPrefix UNIXSOCKETPREFIX
                        alternative directory for UNIX domain sockets
                        (defaults to /tmp)
  --fork                forks the mongod. mongoctl defaults that to True
  --auth                run with security
  --cpu                 periodically show cpu and iowait utilization
  --dbpath DBPATH       directory for datafiles
  --diaglog DIAGLOG     0=off 1=W 2=R 3=both 7=W+some reads
  --directoryperdb      each database will be stored in a separate directory
  --journal             enable journaling
  --journalOptions JOURNALOPTIONS
                        journal diagnostic options
  --journalCommitInterval JOURNALCOMMITINTERVAL
                        how often to group/batch commit (ms)
  --ipv6                enable IPv6 support (disabled by default)
  --jsonp               allow JSONP access via http (has security
                        implications)
  --noauth              run without security
  --nohttpinterface     disable http interface
  --nojournal           disable journaling (journaling is on by default for 64
                        bit)
  --noprealloc          disable data file preallocation - will often hurt
                        performance
  --notablescan         do not allow table scans
  --nssize NSSIZE       .ns file size (in MB) for new databases
  --profile PROFILE     0=off 1=slow, 2=all
  --quota               limits each database to a certain number of files (8
                        default)
  --quotaFiles QUOTAFILES
                        number of files allower per db, requires --quota
  --rest REST           turn on simple rest api
  --repair              run repair on all dbs
  --repairpath REPAIRPATH
                        root directory for repair files - defaults to dbpath
  --slowms SLOWMS       value of slow for profile and console log
  --smallfiles          use a smaller default file size
  --syncdelay SYNCDELAY
                        seconds between disk syncs (0=never, but not
                        recommended)
  --sysinfo             print some diagnostic system information
  --upgrade             upgrade db if needed
  --fastsync            indicate that this instance is starting from a dbpath
                        snapshot of the repl peer
  --oplogSize           size limit (in MB) for op log
  --master              master mode
  --slave               slave mode
  --source SOURCE       when slave: specify master as <server:port>
  --only ONLY           when slave: specify a single database to replicate
  --slavedelay SLAVEDELAY
                        specify delay (in seconds) to be used when applying
                        master ops to slave
  --autoresync          automatically resync if slave data is stale
  --replSet REPLSET     arg is <setname>[/<optionalseedhostlist>]
  --configsvr           declare this is a config db of a cluster; default port
                        27019; default dir /data/configdb
  --shardsvr            declare this is a shard db of a cluster; default port
                        27018
  --noMoveParanoia      turn off paranoid saving of data for moveChunk. this
                        is on by default for now, but default will switch
```

##### stop

```
Usage: stop [<options>] <server>

Stop a server

Options:
  -h, --help   show this help message and exit
  -f, --force  force stop if needed via kill
  --user USER     pass in a user config using the format 'database:user:password'
  -u USERNAME     admin username
  -p [PASSWORD]   admin password
```

```mongoctl stop``` will first attempt to stop a server by sending a ```{"shutdown" : 1}``` command to the server.
If this fails, ```mongoctl``` will then prompt you asking if it can kill the process via ```kill``` and then 
```kill -9```.

```mongoctl stop``` must be executed local to the machine running the server. 

##### restart

```
Usage: restart <server>

Restart a server

Options:
  -h, --help  show this help message and exit
  -u USERNAME     admin username
  -p [PASSWORD]   admin password
```

##### status

```
Usage: status [<options>] <server>

Retrieve status of a server

Options:
  -h, --help     show this help message and exit
  -v, --verbose  include more information in status
  -u USERNAME    admin username
  -p [PASSWORD]  admin password
```

##### list-servers

```
Usage: list-servers  

List all server configurations

Options:
  -h, --help  show this help message and exit
```

##### show-server

```
Usage: show-server <server>

Show specified server configuration

Options:
  -h, --help  show this help message and exit
```

##### tail-log

```
Usage: tail-log [<options>] SERVER_ID

Tails server's log file. Works only on local host

Arguments:
  SERVER_ID  a valid server id

Options:
  -h, --help      show this help message and exit
  --assume-local  Assumes that the server is running on local host. This will
                  skip local address/dns check
```

##### resync-secondary

```
Usage: resync-secondary [<options>] SERVER_ID

Resyncs a secondary member

Arguments:
  SERVER_ID  a valid server id

Options:
  -h, --help      show this help message and exit
  --assume-local  Assumes that the server is running on local host. This will
                  skip local address/dns check
  -u USERNAME     admin username
  -p [PASSWORD]   admin password
```

Cluster commands
-----------------

##### list-clusters

```
Usage: list-clusters  

List all cluster configurations

Options:
  -h, --help  show this help message and exit
```

##### show-cluster

```
Usage: show-cluster <cluster>  

Show specified cluster configuration

Options:
  -h, --help  show this help message and exit
```

##### configure-cluster

```
Usage: configure-cluster [<options>] <cluster>

Configure specified mongodb cluster. This command can be
used both to initiate the cluster for the first time
and to reconfigure the cluster.

Options:
  -h, --help     show this help message and exit
  -n, --dry-run  prints configure cluster db command to execute without
                 executing it
  -f SERVER, --force SERVER
                 force member to become primary
  -u USERNAME    admin username
  -p [PASSWORD]  admin password
```

Miscellaneous commands
-----------------

##### print-uri
```
Usage: print-uri [<options>] SERVER or CLUSTER ID

Prints MongoDB connection URI of the specified server or clurter

Arguments:
  SERVER or CLUSTER ID  Server or cluster id

Options:
  -h, --help      show this help message and exit
  -d DB, --db DB  database name
```