# Configuration

## Writing the configuration

In this example, we'll be configuring the following three nodes in our QuarkDB
cluster: `qdb-test-1.cern.ch:7777`, `qdb-test-2.cern.ch:7777`, `qdb-test-3.cern.ch:7777`.

We will also need an identifier string which uniquely identifies our cluster - a
[UUID](https://www.uuidgenerator.net) will do nicely. This prevents nodes from
different clusters from communicating by accident.

The first step is to initialize the database directory for each node in our cluster.
Run the following command on each node, only potentially changing `--path`. 
For every single node in the cluster, including potential future additions, 
the values used for `--clusterID` and `--nodes` must be consistent.

```
quarkdb-create --path /var/lib/quarkdb/node-1 --clusterID your-cluster-id --nodes qdb-test-1.cern.ch:7777,qdb-test-2.cern.ch:7777,qdb-test-3.cern.ch:7777
```

If you use the default systemd service file to run QuarkDB, you'll also need to
change the owner of the newly created files: ``chown -R xrootd:xrootd /var/lib/quarkdb/node-1``.

This is an example configuration file, running in Raft mode:

```
xrd.port 7777
xrd.protocol redis:7777 libXrdQuarkDB.so
redis.mode raft
redis.database /var/lib/quarkdb/node-1
redis.myself qdb-test-1.cern.ch:7777
```

Each node in the cluster has its own configuration file. The meaning of the above
directives is as follows:

* __xrd.port__: The port where the xrootd server should listen to.
* __xrd.protocol__: The protocol which the xrootd server should load, along with
  the associated dynamic library. If you compiled QuarkDB manually, you need
  to replace `libXrdQuarkDB.so` with the full path in which the dynamic library
  resides in, such as `/path/to/quarkdb/build/src/libXrdQuarkDB.so`.
* __redis.mode__: The mode in which to run. Possible values: _raft_ to run with
  consensus replication, _standalone_ to simply have a single node running.
  Please note that once you have an established setup, it's not easy to switch between the
  two modes. We'll focus on Raft mode for now.
* __redis.database__: The local path in which to store the data - needs to exist
  beforehand.
* __redis.myself__: Only used in Raft mode: identifies a particular node within the cluster.

A few important details:
* The specified port needs to be identical across `xrd.port`, `xrd.protocol`,
  and `redis.myself`.
* `redis.database` needs to exist beforehand - initialize by running `quarkdb-create`,
  as found above.

You probably want to use `systemd` to run QuarkDB as a daemon - there is already a generic
systemd service file bundled with XRootD. Store your configuration file in
`/etc/xrootd/xrootd-quarkdb.cfg`, then run `systemctl start xrootd@quarkdb` to start
the node. The logs can be found in `/var/log/xrootd/quarkdb/xrootd.log`.

Otherwise, it's also possible to run manually with `xrootd -c /etc/xrootd/xrootd-quarkdb.cfg`,
or whichever is the location of your configuration file.

## Starting the cluster

To have a fully operational cluster, a majority (or *quorum*) of nodes need to
be alive and available: at least **2 out of 3**, 3 out of 5, 4 out of 7, etc.

Start at least two out of the three nodes in our test cluster. If all goes well,
they will hold an election with one becoming leader, and the others followers.

Using `redis-cli -p 7777`, it's possible to inspect the state of each node by issuing
`raft-info` and `quarkdb-info` commands. The output from `raft-info`
should look a bit like this:

```
127.0.0.1:7777> raft-info
 1) TERM 6
 2) LOG-START 0
 3) LOG-SIZE 21
 4) LEADER qdb-test-1.cern.ch:7777
 5) CLUSTER-ID ed174a2c-3c2d-4155-85a4-36b7d1c841e5
 6) COMMIT-INDEX 20
 7) LAST-APPLIED 20
 8) BLOCKED-WRITES 0
 9) LAST-STATE-CHANGE 155053 (1 days, 19 hours, 4 minutes, 13 seconds)
10) ----------
11) MYSELF qdb-test-1.cern.ch:7777
12) STATUS LEADER
13) ----------
14) MEMBERSHIP-EPOCH 0
15) NODES qdb-test-1.cern.ch:7777,qdb-test-2.cern.ch:7777,qdb-test-3.cern.ch:7777
16) OBSERVERS
17) ----------
18) REPLICA qdb-test-2.cern.ch:7777 ONLINE | UP-TO-DATE | NEXT-INDEX 21
19) REPLICA qdb-test-3.cern.ch:7777 ONLINE | UP-TO-DATE | NEXT-INDEX 21
```

Verify that everything works by issuing a write towards the leader and
retrieving the data back:

```
qdb-test-1.cern.ch:7777> set mykey myval
OK
qdb-test-1.cern.ch:7777> get mykey
"myval"
```
