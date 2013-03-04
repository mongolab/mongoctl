Overview
--------

```mongoctl``` is a lightweight command line utility that simplifies the
installation of MongoDB and management of MongoDB servers and replica set clusters. It is
particularly useful if you maintain many MongoDB environments with
lots of configurations to manage.

The core philosophy behind ```mongoctl``` is that all server and
cluster configuration be defined declaratively as data, rather than
procedurally as code or scripts. So, instead of typing shell commands
with lots of options that you can never remember, or
writing a bunch of shell scripts hard-coded with hosts, port numbers,
and file-system paths, you simply define the universe of servers and
clusters with JSON objects and pass them to ```mongoctl``` commands.
Server and cluster definitions can reside in flat-files, behind a web-server 
(like Github for instance), or in a MongoDB database. 

#### Usage

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
    dump                      - export MongoDB data to BSON files (using mongodump)
    restore                   - restore MongoDB (using mongorestore)

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

Installation
------------------------------

#### Requirements

* Linux or MacOSX (```mongoctl``` does not currently support Windows) 
* Python >= 2.6
* pip >= 1.0.2 ([instructions on installing pip](docs/installing-pip.md))

#### Supported MongoDB versions
```mongoctl``` supports MongoDB versions >= 1.8.

#### Installing mongoctl

```mongoctl``` is registered in the Python package index pypi.


```
% sudo pip install mongoctl
```

To update:

```
% sudo pip install --upgrade mongoctl
```

To uninstall:

```
% sudo pip uninstall mongoctl
```

#### Running mongoctl's test suite

To run mongoctl's test suite, execute the following command:

```
% python -m mongoctl.tests.test_suite
```

Documentation
----------

* [Quick-start guide](docs/quick-start.md)
* [Configuring mongoctl](docs/configuring-mongoctl.md)
* [Managing MongoDB installations](docs/managing-installations.md)
* [Managing servers](docs/managing-servers.md)
* [Managing clusters](docs/managing-clusters.md)
* [Connecting to servers with the mongo shell](docs/connecting.md)
* [Command reference](docs/command-reference.md)





