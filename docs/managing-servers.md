Configuring and managing servers
-------------------

Server configurations, which define ```mongod``` processes, are managed in a configuration repository, which can 
be either a [FileRepository](configuring-mongoctl.md#using-a-filerepository) or a 
[DatabaseRepository](configuring-mongoctl.md#using-a-databaserepository),
and are managed by ```mongoctl```s [set of server commands](command-reference.md#server-commands).

#### Configuration

All server definitions have the following schema:

```
{
    "_id" : <string>,

    ["description" : <string>,]
    
    ["mongoVersion" : <string>,]
    
    ["address" : <host>[:<port>],]

    "cmdOptions" : {
        ["port" : <int>,]
        ["dbpath": <string>,]
        ["directoryperdb" : <boolean>,]
        ["logpath" : <string>,]
        ["logappend" : <string>,]
        ["auth": <boolean>,]
        ["journal": <boolean>,]
        ["journalOptions": <string>,]
        ["journalCommitInterval": <int>,]
        ["quota": <boolean>,]
        ["quotaFiles": <int>,]
        ["maxConns" : <int>,]
        ["objcheck" : <boolean>,]
        ["pidfilepath" : <string>,]
        ["keyFile" : <string>,]
        ["verbose" : <string>,]
        ["quiet" : <boolean>,]
        ["nounixsocket" : <boolean>,]
        ["unixSocketPrefix" : <string>,]
        ["cpu" : <boolean>,]
        ["bind_ip" : <string>,]
        ["diaglog" : <string>,]
        ["ipv6" : <string>,]
        ["jsonp" : <boolean>,]
        ["noauth" : <boolean>,]
        ["nohttpinterface" : <boolean>,]
        ["nojournal" : <boolean>,]
        ["noprealloc" : <boolean>,]
        ["notablescan" : <boolean>,]
        ["nssize" : <int>,]
        ["profile" : <string>,]
        ["rest" : <boolean>,]
        ["repair" : <boolean>,]
        ["repairPath" : <string>,]
        ["slowms" : <int>,]
        ["smallfiles" : <boolean>,]
        ["syncdelay" : <int>,]
        ["sysinfo" : <boolean>,]
        ["upgrade" : <boolean>,]
        ["fastsync" : <boolean>,]
        ["oplogSize" : <int>,]
        ["master" : <boolean>,]
        ["slave" : <boolean>,]
        ["source" : <string>,]
        ["only" : <string>,]
        ["slavedelay" : <int>,]
        ["autoresync" : <boolean>,]
        ["replSet" : <string>,]
        ["configsvr" : <boolean>,]
        ["shardsvr" : <boolean>,]
        ["noMOveParanoia" : <string>,]
    },
    
    "seedUsers": {
        "<dbname>": [
            {
                "username": <string>,
                ["password": <string>]
            }
            ...
        ],
        ...
    }

}
```

The set of ```cmdOptions``` very closely mirrors the set of command-line options of the ```mongod``` command 
([see detailed documentation here](http://www.mongodb.org/display/DOCS/Command+Line+Parameters)).

#### Paths

Path values for ```cmdOptions``` such as ```dbpath``` and ```logpath``` can be assigned values relative to the current
user's home directory with ```~/<path>``` and ```$HOME/<path>```. 

#### Seeding database users

The ```seedUsers``` field on server configurations allow you to define, per database, a set of users for ```mongoctl``` to create
for you the first time the server is started. ```mongoctl start``` will first check to see if a user already exists before adding it. For any user
for which there is no ```password``` defined, the operator will be prompted at startup to provide one.  
