mongoctl (edit 1)
--------

```mongoctl``` is a lightweight command line utility that simplifies the
management of MongoDB servers and replica set clusters. It is
particularly useful if you maintain many MongoDB environments with
lots of configurations to manage.

### Overview

```
Usage: mongoctl [<options>] <command> [<command-args>]

Utility that controls MongoDB servers

Options:
  -h, --help            show this help message and exit
  -v, --verbose         make mongoctl be more verbose
  -n, --noninteractive  bypass prompting for user interaction

Commands:
  Server Commands:
    start                - start a server
    stop                 - stop a server
    restart              - restart a server
    status               - retrieve status of a server
    list-servers         - show list of server configurations
    show-server          - show specified server configuration
    connect              - open a mongo shell to a server

  Cluster Commands:
    list-clusters        - show list of cluster configurations
    show-cluster         - show specified cluster configuration
    configure-cluster    - configure specified mongodb cluster

See 'mongoctl <command> --help' for more help on a specific command
```

The core philosophy behind ```mongoctl``` is that all server and
cluster configuration be defined declaratively as data, rather than
procedurally as code or scripts. So, instead of typing shell commands
with lots of options that you can never remember, or
writing a bunch of shell scripts hard-coded with hosts, port numbers,
and file-system paths, you simply define the universe of servers and
clusters with JSON objects and pass them to ```mongoctl``` commands.

#### A simple example

First define a configuration for your ```mongod``` server:

```
{
    "_id" : "abdul-dev",

    "mongoVersion" : "2.0.2",
    "cmdOptions" : {
        "port" : 27017,
        "dbpath": "/Users/abdul/dbs/abdul-dev",
        "logpath" : "/Users/abdul/dbs/abdul-dev/mongodb.log",
        "logappend" : true,
        "auth": false,
        "journal": true,
        "quota": true,
        "quotaFiles": 4,
        "maxConns" : 10000
    }
}
```

Then start the server, referencing it by ```_id```

```
% mongoctl start abdul-dev
```

Check status

```
% mongoctl status abdul-dev
{
    "connection": true,
    "ping": {
        "ok": 1.0
    }
}
```

```
% mongoctl status -v abdul-dev
<more stuff> // TODO
```

Connect 

```
% mongoctl connect abdul-dev
Connecting to server abdul-dev...
Attempting to use mongo home MONGO_HOME=/usr/local/mongodb/mongodb-osx-x86_64-2.0.2
MongoDB shell version: 2.0.2
connecting to: localhost:27017/test
> 
```

Stop the server

```
% mongoctl stop abdul-dev
```
Requirements
--------------------

* Python >= 2.6
* pip >= 1.0.2
   + Download: http://pypi.python.org/pypi/pip#downloads
   + Operating instructions: http://www.pip-installer.org/en/latest/index.html
   + Installation instructions: http://www.pip-installer.org/en/latest/index.html

Installing pip from git is the easiest:

```
% git clone https://github.com/pypa/pip.git
% cd pip
% python setup.py install # may need to be root or need to use sudo
```

Installing
--------------------

```
% sudo pip install mongoctl
```

This will download and install all packages required by mongoctl, as
well as mongoctl itself.  It will also create a directory called
`.mongoctl` in the home directory of the current OS user. This
directory is used to hold configuration files used to configure
`mongoctl`.

To update:

```
% sudo pip install --upgrade mongoctl
```

To uninstall:

```
% sudo pip uninstall mongoctl
```

This will uninstall mongoctl, although it will not uninstall all of the
packages mongoctl requires. One must remove those packages manually from
Python's 'site-packages' directory.

Supported MongoDB versions
--------------------
mongoctl supports MongoDB versions >= 1.8

Running mongoctl's Test Suite
--------------------
To run mongoctl's test suite, execute the following command:

```
% python -m mongoctl.tests.test_suite
```

Configuring mongoctl
--------------------

The main configuration file for ```mongoctl``` is
```~/.mongoctl/mongoctl.config```. This configuration file allows you
to control all of the configurable aspects of ```mongoctl```'s
behavior.

Below is the default configuration generated via the installation:

```
{       
    "fileRepository" : {
       "servers" : "servers.json",
       "clusters" : "clusters.json"
    },

//    "databaseRepository" : {
//       "databaseURI" : "mongodb://localhost:27017",
//       "servers" : "servers",
//       "clusters" : "clusters",
//    }

}
```

### Configuration options

The ```mongoctl.config``` config file supports the following:

* ```fileRepository``` : If not null, this object tells ```mongoctl```
where to look for configuration files defining servers and clusters
* ```databaseRepository``` : If not null, this configures
```mongoctl``` with a database endpoint for finding server and cluster 
configurations

### ```_id``` resolution

When ```_id```s designating servers or clusters are passed to
```mongoctl``` commands they are first resolved against the
configurations defined in the ```fileRepository``` (if configured),
and if not found then looked-up in the ```databaseRepository```.

This presents two basic strategies for managing your server and
cluster configurations:

* Keep your configurations in files (which can be version controlled)
* Keep your configurations in a central MongoDB configuration database that is accessible by all database hosts

### Using a fileRepository

To define your configurations via flat files, create one file to hold
all server definitions and another to hold all cluster definitions. By
default, servers are defined in ```~/.mongoctl/servers.json``` and
clusters in ```~/.mongoctl/clusters.json```. Each file should contain
an JSON array of objects. If you wish to place these files somewhere
else simply configure the ```fileRepository``` property in
```mongoctl.config``` appropriately. 

### Using a databaseRepository

You may also store your configurations in a MongoDB database. This is
a conventient way of sharing configurations amongst disperate
machines. When storting configurations in a database, we suggest you
have a good document editor to make editing server and cluster objects
easy, or maintaining the data as files and importing them into the
configuration database using ```mongoimport```.

Servers
--------------------

### Configuration

All server definitions have the following schema:

```
{
    "_id" : <id>,

    ["mongoVersion" : <version>,]
    
    ["address" : <host>[:<port>],]

    "cmdOptions" : {
        ["port" : <port>,]
        ["dbpath": <path>,]
        ["logpath" : <path>,]
        ["logappend" : <boolean>,]
        ["auth": <boolean>,]
        ["journal": <boolean>,]
        ["quota": <boolean>,]
        ["quotaFiles": <n>,]
        ["maxConns" : <n>,]
        .
        .
        . // TODO
    }
    
    "users": {
        "<dbname>": [
            {
                "username": <string>,
                "password": <string>
            }
            ...
        ],
        ...
    }

}
```

Clusters
--------------------

### Configuration

All cluster definitions have the following schema:

```
{
    "_id": <id>,

    ["replKey": "<secret-keyphrase>",]

    "members": [
        {
            "server": {"$ref": "servers", "$id": <id>},
            ["arbiterOnly" : <boolean>,]
            ["buildIndexes" : <boolean>,],
            ["hidden" : <boolean>,]
            ["priority" : <priority>,]
            ["tags" : <tag>*,]
            ["slaveDelay" : <n>,]
            ["votes" : <n>,]
        },
        .
        .
        .
    ]
}
```
Required environment variables
--------------------

In order for mongoctl to be able to determine compatible MongoDB installation folder, you will have to set
at least one of the enviorment variables:
### MONGO_HOME
 Default MongoDB installation folder. mongoctl will use MONGO_HOME when mongoVersion is not set in
 server config or mongoVersion equals the version of MongoDB that MONGO_HOME points to

```
export MONGO_HOME=/Users/abdul/Library/mongodb/mongodb-osx-x86_64-2.0.3-rc1
```

### MONGO_VERSIONS
 If MONGO_HOME was not compatible with the mongoVersion of the server, mongoctl will
 then look in the MONGO_VERSIONS folder and try to find an installation that matches the mongoVersion of the server.

```
export MONGO_VERSIONS=/Users/abdul/Library/mongodb
```

### Starting a cluster for the first time

To start a cluster for the first time:

* Start each server in the cluster by issuing a ```mongoctl start
  <server>``` from each server's respective host. The first server you start
  will become the primary

* Initialize the cluster via the ```mongoctl configure-cluster <cluster>``` command

### Reconfiguring a cluster

To reconfigure a cluster:

* Modify the cluster configuration
* Execute the ```mongoctl configure-cluster <cluster>``` command. This will reconfigure
the cluster based on the new configurations.

Command reference
--------------------

##### start

The ```start``` command calls ```mongod``` with arguments and options
based on the configuration of the specified server document and its
configured ```cmdOptions```.  You can see the generated ```mongod```
command-line string by adding the dry run option (```-n``` or ```--dry-run```).

```
% mongoctl start --dry-run abdul-dev
************** Dry Run *****************
Using mongo home MONGO_HOME=/Users/abdul/Library/mongodb/mongodb-osx-x86_64-2.0.3-rc1
Fork Command :

/Users/abdul/Library/mongodb/mongodb-osx-x86_64-2.0.3-rc1/bin/mongod --dbpath /Users/abdul/dbs/abdul-dev --journal --logappend --logpath /Users/abdul/dbs/abdul-dev/mongodb.log --maxConns 10000 --pidfilepath /Users/abdul/dbs/abdul-dev/pid.txt --port 27017 --quota --quotaFiles 4
```

```start``` also allows for the overriding of the options defined the
server configuration via flags specified at the
command-line. ```start``` passes these options as-is to ```mongod```.
This is useful for one-off situations (i.e. running a ```--repair```).

```
Usage: start [<options>] SERVER_ID

Start a server

Options:
  -h, --help            show this help message and exit
  --version VERSION     show version information
  -n, --dry-run         prints the mongod command to execute without executing
                        it
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
```

mongoctl stop server by sending a {"shutdown": 1} db command to the server. If the server
 is a primary of a cluster, mongoctl steps it down first then sends shutdown command.

##### restart

```
Usage: restart <server>

Restart a server

Options:
  -h, --help  show this help message and exit
```

##### status

```
Usage: status [<options>] SERVER_ID

Retrieve status of a server

Options:
  -h, --help     show this help message and exit
  -v, --verbose  include more information in status
```

##### list-servers

```
Usage: list-servers  

Show list of server configurations

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

##### connect

```
Usage: connect server

Open a mongo shell to the specified server. Uses the 'address' 
field of the specified server config if specified, otherwise 
tries to connect to the configured port on localhost.

Options:
  -h, --help  show this help message and exit
```

##### list-clusters

```
Usage: list-clusters  

Show list of cluster configurations

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
Usage: configure-cluster [<options>] CLUSTER_ID

Configure specified mongodb cluster. This command is
used both to initiate the cluster for the first time
and to reconfigure the cluster.

Options:
  -h, --help     show this help message and exit
  -n, --dry-run  prints configure cluster db command to execute without
                 executing it
```

