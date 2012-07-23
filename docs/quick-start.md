Quick start guide
-----------------

```mongoctl``` comes packaged with sample configurations of servers and clusters that allow you to get going quickly.
These samples are in ```~/.mongoctl/servers.config``` and ```~/.mongoctl/clusters.config```. Core configuration of 
```mongoctl``` is defined in ```~/.mongoctl/mongoctl.config```.

Install MongoDB
----------------

The first time you use ```mongoctl``` you will want to download MongoDB. To get the latest stable version:

```
% mongoctl install
```

You can also specify a specific version:

```
% mongoctl install 2.0.2
```

Simple server example
------------------------

Let's start with a very simple single-server example. To see the set of servers currently 
defined we use the ```list-servers``` command:

```
% mongoctl list-servers

--------------------------------------------------------------------------------
_id                        address                   description
--------------------------------------------------------------------------------
SampleClusterServer1       localhost:28017           Sample cluster member
SampleClusterServer2       localhost:28027           Sample cluster member
SampleClusterArbiter       localhost:28037           Sample arbiter
SampleServer               localhost:27017           Sample server (single mongod)
```

You can look at the details of the server configuration for ```SampleServer``` like this:

```
% mongoctl show-server SampleServer

Configuration for server 'SampleServer':
{
    "_id": "SampleServer"
    "cmdOptions": {
        "port": 27017, 
        "dbpath": "~/mongodb-data/sample-server"
    }
}
```

#### Start the server

```
% mongoctl start SampleServer             

Checking to see if server 'SampleServer' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Starting server 'SampleServer' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/mongodb-data/sample-server --logpath /Users/abdul/mongodb-data/sample-server/mongodb.log --pidfilepath /Users/abdul/mongodb-data/sample-server/pid.txt --port 27017

<lots of stuff>

Server 'SampleServer' started successfully! (pid=50877)

Preparing server 'SampleServer' for use as configured...
Checking if there are any users that need to be added for server 'SampleServer'...
No users configured for admin DB...
Did not add any new users.
```

#### Check the status

```
% mongoctl status SampleServer

Status for server 'SampleServer':
{
    "connection": true, 
    "serverStatusSummary": {
        "host": "My-MacBook-Pro.local", 
        "version": "2.0.6"
    }
}
```

#### Connect 

```
% mongoctl connect SampleServer    

Connecting to server 'SampleServer'...
Using mongo at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongo'...
MongoDB shell version: 2.0.5
connecting to: 127.0.0.1:27017/test
> 
```

### Stop the server

```
% mongoctl stop SampleServer              

Checking to see if server 'SampleServer' is actually running before stopping it...
Stopping server 'SampleServer' (PID=50877) ...

Sending the following command to 127.0.0.1:27017:
{
    "shutdown": 1, 
    "force": false
}

Will now wait for server 'SampleServer' to stop.
-- waiting --
Server 'SampleServer' has stopped.
```

Replica-set cluster example
----------------------------

Here we show how you configure and start a replica-set cluster. 

```
% mongoctl list-clusters

-------------------------------------------------------------------------------------------------------------
_id             members                                                           description            
-------------------------------------------------------------------------------------------------------------
SampleCluster   [ SampleClusterServer1, SampleClusterServer2, SampleArbiter ]     A 2 + arbiter replica-set
```

```
% mongoctl show-cluster SampleCluster

{
    "_id": "SampleCluster", 
    
    "description" : "A 2 + arbiter replica-set",
    
    "members": [
        {
            "server": {
                "$ref": "servers", 
                "$id": "SampleClusterServer1"
            }
        }, 
        {
            "server": {
                "$ref": "servers", 
                "$id": "SampleClusterServer2"
            }
        }, 
        {
            "arbiterOnly": true, 
            "server": {
                "$ref": "servers", 
                "$id": "SampleArbiter"
            }
        }
    ]
}
```

#### Start the primary

Let's start the server we wish to be the primary. You will be prompted on startup by ```mongoctl``` asking if you 
would like to initialize the replica-set with this server. You can say no, but here we will say 'y'. 


```
% mongoctl start SampleClusterServer1

Checking to see if server 'SampleClusterServer1' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'SampleCluster'...
Starting server 'SampleClusterServer1' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/sample-cluster-server1 --directoryperdb --logpath /Users/abdul/sample-cluster-server1/mongodb.log --pidfilepath /Users/abdul/sample-cluster-server1/pid.txt --port 28017 --replSet SampleCluster

<lots of stuff>

Server 'SampleClusterServer1' started successfully! (pid=50908)

Replica set cluster 'SampleCluster' has not been initialized yet.
Do you want to initialize replica set cluster 'SampleCluster' using server 'SampleClusterServer1'? [y/n] y

<lots of stuff>
```

Now we have a running replica-set with one node: 

```
% mongoctl status SampleClusterServer1 

Status for server 'SampleClusterServer1':
{
    "connection": true, 
    "serverStatusSummary": {
        "host": "My-MacBook-Pro.local:37017", 
        "version": "2.0.6", 
        "repl": {
            "ismaster": true
        }
    }, 
    "selfReplicaSetStatusSummary": {
        "stateStr": "PRIMARY", 
        "name": "localhost:37017"
    }
}
```

#### Start the secondary

Next we start the second server and add it to the replica-set when prompted:

```
% mongoctl start SampleClusterServer2

Checking to see if server 'SampleClusterServer2' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'SampleCluster'...
Starting server 'SampleClusterServer2' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/sample-cluster-server2 --directoryperdb --logpath /Users/abdul/sample-cluster-server2/mongodb.log --pidfilepath /Users/abdul/sample-cluster-server2/pid.txt --port 28027 --replSet SampleCluster

<lots of stuff>

Server 'SampleClusterServer2' started successfully! (pid=50913)

Do you want to add server 'SampleClusterServer2' to replica set cluster 'SampleCluster'? [y/n] y

<lots of stuff>
```

#### Start the arbiter

Finally, we add the arbiter. 

```
% mongoctl start SampleArbiter       

Checking to see if server 'SampleArbiter' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'SampleCluster'...
Starting server 'SampleArbiter' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/sample-arbiter --directoryperdb --logpath /Users/abdul/sample-arbiter/mongodb.log --pidfilepath /Users/abdul/sample-arbiter/pid.txt --port 28037 --replSet SampleCluster

<lots of stuff>

Server 'SampleArbiter' started successfully! (pid=50918)

Do you want to add server 'SampleArbiter' to replica set cluster 'SampleCluster'? [y/n] y
 
<lots of stuff>
```

You now have a fully operational replica-set cluster you can connect to:

```
% mongoctl connect SampleClusterServer1

Using mongo at '/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.6/bin/mongo'...
Executing command: 
/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.6/bin/mongo 127.0.0.1:37017/admin
MongoDB shell version: 2.0.6
connecting to: 127.0.0.1:37017/admin
PRIMARY> 
```
