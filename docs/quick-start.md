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
MyClusterServer1       localhost:28017           My cluster member
MyClusterServer2       localhost:28027           My cluster member
MyClusterArbiter       localhost:28037           My cluster arbiter
MyServer               localhost:27017           My server (single mongod)
```

You can look at the details of the server configuration for ```MyServer``` like this:

```
% mongoctl show-server MyServer

Configuration for server 'MyServer':
{
    "_id": "MyServer"
    "cmdOptions": {
        "port": 27017, 
        "dbpath": "~/mongodb-data/my-server"
    }
}
```

#### Start the server

```
% mongoctl start MyServer             

Checking to see if server 'MyServer' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Starting server 'MyServer' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/mongodb-data/my-server --logpath /Users/abdul/mongodb-data/my-server/mongodb.log --pidfilepath /Users/abdul/mongodb-data/my-server/pid.txt --port 27017

<lots of stuff>

Server 'MyServer' started successfully! (pid=50877)

Preparing server 'MyServer' for use as configured...
Checking if there are any users that need to be added for server 'MyServer'...
No users configured for admin DB...
Did not add any new users.
```

#### Check the status

```
% mongoctl status MyServer

Status for server 'MyServer':
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
% mongoctl connect MyServer    

Connecting to server 'MyServer'...
Using mongo at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongo'...
MongoDB shell version: 2.0.5
connecting to: 127.0.0.1:27017/test
> 
```

### Stop the server

```
% mongoctl stop MyServer              

Checking to see if server 'MyServer' is actually running before stopping it...
Stopping server 'MyServer' (PID=50877) ...

Sending the following command to 127.0.0.1:27017:
{
    "shutdown": 1, 
    "force": false
}

Will now wait for server 'MyServer' to stop.
-- waiting --
Server 'MyServer' has stopped.
```

Replica-set cluster example
----------------------------

Here we show how you configure and start a replica-set cluster. 

```
% mongoctl list-clusters

-------------------------------------------------------------------------------------------------------------
_id             members                                                           description            
-------------------------------------------------------------------------------------------------------------
MyCluster   [ MyClusterServer1, MyClusterServer2, MyClusterArbiter ]     A 2 + arbiter replica-set
```

```
% mongoctl show-cluster MyCluster

{
    "_id": "MyCluster", 
    
    "description" : "A 2 + arbiter replica-set",
    
    "members": [
        {
            "server": {
                "$ref": "servers", 
                "$id": "MyClusterServer1"
            }
        }, 
        {
            "server": {
                "$ref": "servers", 
                "$id": "MyClusterServer2"
            }
        }, 
        {
            "arbiterOnly": true, 
            "server": {
                "$ref": "servers", 
                "$id": "MyClusterArbiter"
            }
        }
    ]
}
```

#### Start the primary

Let's start the server we wish to be the primary. You will be prompted on startup by ```mongoctl``` asking if you 
would like to initialize the replica-set with this server. You can say no, but here we will say 'y'. 


```
% mongoctl start MyClusterServer1

Checking to see if server 'MyClusterServer1' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'MyCluster'...
Starting server 'MyClusterServer1' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/my-cluster-server1 --directoryperdb --logpath /Users/abdul/my-cluster-server1/mongodb.log --pidfilepath /Users/abdul/my-cluster-server1/pid.txt --port 28017 --replSet MyCluster

<lots of stuff>

Server 'MyClusterServer1' started successfully! (pid=50908)

Replica set cluster 'MyCluster' has not been initialized yet.
Do you want to initialize replica set cluster 'MyCluster' using server 'MyClusterServer1'? [y/n] y

<lots of stuff>
```

Now we have a running replica-set with one node: 

```
% mongoctl status MyClusterServer1 

Status for server 'MyClusterServer1':
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
% mongoctl start MyClusterServer2

Checking to see if server 'MyClusterServer2' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'MyCluster'...
Starting server 'MyClusterServer2' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/my-cluster-server2 --directoryperdb --logpath /Users/abdul/my-cluster-server2/mongodb.log --pidfilepath /Users/abdul/my-cluster-server2/pid.txt --port 28027 --replSet MyCluster

<lots of stuff>

Server 'MyClusterServer2' started successfully! (pid=50913)

Do you want to add server 'MyClusterServer2' to replica set cluster 'MyCluster'? [y/n] y

<lots of stuff>
```

#### Start the arbiter

Finally, we add the arbiter. 

```
% mongoctl start MyClusterArbiter       

Checking to see if server 'MyClusterArbiter' is already running before starting it...
Using mongod at '/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod'...
Validating cluster 'MyCluster'...
Starting server 'MyClusterArbiter' for the first time...

Executing command:
/Users/abdul/mongodb-installs/mongodb-osx-x86_64-2.0.5/bin/mongod --dbpath /Users/abdul/my-cluster-arbiter --directoryperdb --logpath /Users/abdul/my-cluster-arbiter/mongodb.log --pidfilepath /Users/abdul/my-cluster-arbiter/pid.txt --port 28037 --replSet MyCluster

<lots of stuff>

Server 'MyClusterArbiter' started successfully! (pid=50918)

Do you want to add server 'MyClusterArbiter' to replica set cluster 'MyCluster'? [y/n] y
 
<lots of stuff>
```

You now have a fully operational replica-set cluster you can connect to:

```
% mongoctl connect MyClusterServer1

Using mongo at '/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.6/bin/mongo'...
Executing command: 
/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.6/bin/mongo 127.0.0.1:37017/admin
MongoDB shell version: 2.0.6
connecting to: 127.0.0.1:37017/admin
PRIMARY> 
```
