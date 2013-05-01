Managing clusters
--------------------

Cluster configurations are managed in a configuration repository, which can be either a 
[FileRepository](configuring-mongoctl.md#using-a-filerepository) or a 
[DatabaseRepository](configuring-mongoctl.md#using-a-databaserepository),
and are managed by ```mongoctl```s [set of cluster commands](command-reference.md#cluster-commands).

#### Configuration

All cluster definitions have the following schema:

```
{
    "_id": <id>,

    ["description" : <string>,]
    
    ["replKey": "<secret-keyphrase>",]

    "members": [
        {
            ("server": {"$ref": "servers", "$id": <id>}, |
             "host" : "<host>:<port>", )
            ["arbiterOnly" : <boolean>,]
            ["buildIndexes" : <boolean>,]
            ["hidden" : <boolean>,]
            ["priority" : <int>,]
            ["tags" : <tag>*,]
            ["slaveDelay" : <int>,]
            ["votes" : <int>,]
        },
        .
        .
        .
    ]
}
```

For each member declaration there are two ways to refer to the corresponding server:

* ```server```: A dbref to the server configuration that defines the server
* ```host```: An address-based reference of the form ```<host>:<port>```

One of these two fields must be present in every member definition.

### Starting a cluster for the first time

To start a cluster for the first time:

* Start each server in the cluster by issuing a ```mongoctl start
<server>``` from each server's respective host. You will be prompted each time to
choose if you would like to add that server to the cluster. If you choose 'y' for each, 
you're done.

* If you declined to add one of the servers to the cluster you can later initialize the entire cluster via 
the ```mongoctl configure-cluster <cluster>``` command. 

For a detailed example of starting a cluster see the [quick start guide](quick-start.md#replica-set-cluster-example).

### Reconfiguring a cluster

To reconfigure a cluster:

* Modify the cluster configuration
* Execute the ```mongoctl configure-cluster <cluster>``` command. This will reconfigure
the cluster based on the new configurations.


