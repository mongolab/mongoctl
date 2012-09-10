Managing MongoDB installations
--------------------

```mongoctl``` allows you to manage multiple versions of MongoDB. You can install (and uninstall) different versions using the 
[```mongoctl install-mongodb```](command-reference.md#install-mongodb) and
[```mongoctl uninstall-mongodb```](command-reference.md#uninstall-mongodb) commands respecively. At any time you can see what versions
of MongoDB are currently installed using [```mongoctl list-versions```](command-reference.md#list-versions).

#### Configuring the location of MongoDB installations

You can configure the directory where ```mongoctl``` manages MongoDB instalations in ```mongoctl.config``` 
(see [Configuring mongoctl](configuring-mongoctl.md)). The default location is ```~/mongodb```. 

#### Example

```
% mongoctl list-versions                   

--------------------------------------------------------------------------------
VERSION              LOCATION
--------------------------------------------------------------------------------
2.0.4                /Users/abdul/mongodb/mongodb-osx-x86_64-2.0.4
```

```
% mongoctl install-mongodb 2.0.5

Running install for osx 64bit to mongoDBInstallations=/Users/abdul/mongodb
Downloading http://fastdl.mongodb.org/osx/mongodb-osx-x86_64-2.0.5.tgz ...
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 40.9M  100 40.9M    0     0   581k      0  0:01:12  0:01:12 --:--:--  515k

Extracting mongodb-osx-x86_64-2.0.5.tgz ...
x mongodb-osx-x86_64-2.0.5/
x mongodb-osx-x86_64-2.0.5/bin/
x mongodb-osx-x86_64-2.0.5/bin/bsondump
x mongodb-osx-x86_64-2.0.5/bin/mongo
x mongodb-osx-x86_64-2.0.5/bin/mongod
x mongodb-osx-x86_64-2.0.5/bin/mongodump
x mongodb-osx-x86_64-2.0.5/bin/mongoexport
x mongodb-osx-x86_64-2.0.5/bin/mongofiles
x mongodb-osx-x86_64-2.0.5/bin/mongoimport
x mongodb-osx-x86_64-2.0.5/bin/mongorestore
x mongodb-osx-x86_64-2.0.5/bin/mongos
x mongodb-osx-x86_64-2.0.5/bin/mongosniff
x mongodb-osx-x86_64-2.0.5/bin/mongostat
x mongodb-osx-x86_64-2.0.5/bin/mongotop
x mongodb-osx-x86_64-2.0.5/GNU-AGPL-3.0
x mongodb-osx-x86_64-2.0.5/README
x mongodb-osx-x86_64-2.0.5/THIRD-PARTY-NOTICES

Moving extracted folder to /Users/abdul/mongodb
Deleting archive mongodb-osx-x86_64-2.0.5.tgz
MongoDB 2.0.5 installed successfully!
```

```
% mongoctl list-versions                   

--------------------------------------------------------------------------------
VERSION              LOCATION
--------------------------------------------------------------------------------
2.0.4                /Users/abdul/mongodb/mongodb-osx-x86_64-2.0.4
2.0.5                /Users/abdul/mongodb/mongodb-osx-x86_64-2.0.5
```

```
% mongoctl uninstall-mongodb 2.0.4

Found MongoDB '2.0.4' in '/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.4'
Proceed uninstall? [y/n] y
 Deleting '/Users/abdul/mongodb/mongodb-osx-x86_64-2.0.4'
MongoDB '2.0.4' Uninstalled successfully!
```

```
% mongoctl list-versions                   

--------------------------------------------------------------------------------
VERSION              LOCATION
--------------------------------------------------------------------------------
2.0.5                /Users/abdul/mongodb/mongodb-osx-x86_64-2.0.5
```