Configuring mongoctl
---------------

The main configuration file for ```mongoctl``` is
```~/.mongoctl/mongoctl.config```. This configuration file allows you
to control all of the configurable aspects of ```mongoctl```'s
behavior.

Below is the default configuration generated via the installation:

```
{
   "mongoDBInstallationsDirectory": "~/mongodb",

   "fileRepository": {
      "servers": "servers.config", // servers file name
      "clusters": "clusters.config" // clusters file name
    },

/**
   "databaseRepository": {
     "databaseURI": "mongodb://localhost:27017", 
     "servers": "servers", // servers collection name
     "clusters": "clusters" // clusters collection name
   },
 **/   
}
```

Configuration options
---------------------

The ```mongoctl.config``` config file supports the following:

* ```mongoDBInstallationsDirectory```: Directory where ```mongoctl``` will manage MongoDB installations. ```mongoctl install``` will download MongoDB installations to this directory.
* ```fileRepository``` : If not null, this object tells ```mongoctl```
where to look for configuration files defining servers and clusters. These can be defined as local filesystem paths, 'file:', or 'http:' URLs. 
* ```databaseRepository``` : If not null, this configures
```mongoctl``` with a database endpoint for finding server and cluster 
configurations

#### ```_id``` resolution

When ```_id```s designating servers or clusters are passed to
```mongoctl``` commands they are first resolved against the
configurations defined in the ```fileRepository``` (if configured),
and if not found then looked-up in the ```databaseRepository```.

This presents a few basic strategies for managing your server and
cluster configurations:

* Keep your configurations in files (which can be version controlled and distributed to db host machines)
* Serve your files from a web server, potentially fronting a version control system such as Github 
* Keep your configurations in a central MongoDB configuration database that is accessible by all database hosts

#### Using a fileRepository

To define your configurations via flat files, create one file to hold
all server definitions and another to hold all cluster definitions. By
default, servers are defined in ```~/.mongoctl/servers.config``` and
clusters in ```~/.mongoctl/clusters.config```. Each file should contain
an JSON array of objects. If you wish to place these files somewhere
else simply configure the ```fileRepository``` property in
```mongoctl.config``` appropriately. These files can reside on the local
filesystem of each database host machine or they can be served by a 
web server and specified via 'http:' URLs in ```mongoctl.config```.

#### Using a databaseRepository

You may also store your configurations in a MongoDB database. This is
a conventient way of sharing configurations amongst disperate
machines. When storting configurations in a database, we suggest you
have a good document editor to make editing server and cluster objects
easy, or maintaining the data as files and importing them into the
configuration database using ```mongoimport```.

