Connecting to servers with the mongo shell
------------------------------------------

The [```mongoctl connect```](command-reference.md#connect) command is a convenience wrapper 
around the ```mongo``` shell. The main conveniences are the ability to pass server or 
cluster ```_id```s to the ```connect``` command (e.g. ```mongoctl connect MyServer```), and
support for standard MongoDB URIs as a way of specifying connection addresses. 

```
Usage: connect [<options>] <db-address> [file names (ending in .js)]

Opens a mongo shell connection to the specified database. If a
cluster is specified command will connect to the primary server.

<db-address> can be one of:
   (a) a mongodb URI (e.g. mongodb://localhost:27017/mydb)
   (b) <server-id>/<db>
   (c) <cluster-id>/<db> (will find and connect to primary)

Arguments:
  <db-address>          database addresses supported by mongoctl. Check docs
                        for more details.
  [file names (ending in .js)]
                        file names: a list of files to run. files have to end
                        in .js and will exit after unless --shell is specified

Options:
  -h, --help     show this help message and exit
  -u USERNAME    username
  -p [PASSWORD]  password
  --shell        run the shell after executing files
  --norc         will not run the ".mongorc.js" file on start up
  --quiet        be less chatty
  --eval EVAL    evaluate javascript
  --verbose      increase verbosity
  --ipv6         enable IPv6 support (disabled by default)
```
