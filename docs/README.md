# quarkdb

[quarkdb](https://gitlab.cern.ch/eos/quarkdb) is a highly available key-value store that implements a small subset
of the redis command set, developed by IT-ST at CERN.

We build on top of [rocksdb](https://github.com/facebook/rocksdb), a strongly
consistent, embeddable key-value store.

High availability is achieved through multiple replicated nodes and the
[raft](https://raft.github.io) distributed consensus algorithm.

# Installation

The easiest way is to install from an RPM - check out our
[CI repo](https://storage-ci.web.cern.ch/storage-ci/quarkdb/).

Minimum supported platform is CentOS 7, since rocksdb requires C++11 and a recent
compiler.

# Building from source

Check out `utils/el7-packages.sh` for a list of build dependencies.
The following will compile quarkdb and run the tests.

```
git clone https://gitlab.cern.ch/eos/quarkdb && cd quarkdb
git submodule update --recursive --init

mkdir build && cd build
cmake ..
make
./test/tests
```

RocksDB is embedded as a submodule, but you can also compile it yourself
and specify `-DROCKSDB_ROOT_DIR` to the cmake invocation, in order to speed
things up if you do a full recompilation of quarkdb often.

# Setting up a cluster

Let's set up a 3-node test cluster on localhost, with each node listening on a
different port.

1. Decide on which nodes will be part of the cluster.
Let's use localhost:7777,localhost:7778,localhost:7779.

2. Decide on a unique clusterID for your new cluster.
A [UUID](https://www.uuidgenerator.net) will do nicely.
It's a string that uniquely identifies a cluster - nodes with
different clusterIDs will refuse to talk with one another.

3. For each node in the cluster, run the following commands:

  ```
  mkdir /path/to/node-database
  quarkbd-journal --create --path /path/to/node-database/raft-journal --clusterID your-cluster-id --nodes localhost:7777,localhost:7778,localhost:7779
  ```

  Both clusterID and nodes *must* be identical in each invocation. At the end, you
  should have a directory structure similar to the following:

  ```
  .
  ├── node-0
  │   └── raft-journal
  │       └──  ...
  ├── node-1
  │   └── raft-journal
  │       └──  ...
  └── node-2
      └── raft-journal
          └──  ...
  ```

4. Create an xrootd configuration file for each node - here's an example.

  ```
  if exec xrootd
    xrd.port 7777
    xrd.protocol redis:7777 /path/to/quarkdb/build/src/libXrdRedis.so
    redis.mode raft
    redis.database /path/to/node-database
    redis.myself localhost:7777
  fi
  ```

  Change the parameters for each node accordingly - make sure to use the same
  port in all places: `xrd.port xyz`, `xrd.protocol redis:xyz`,
  `redis.myself localhost:xyz`

5. Run at least two out of the three nodes. If all goes well, they will hold an
election with one becoming a leader, and the others followers.

  Using `redis-cli`, you can inspect the state of each node by issuing the
  `raft_info` and `quarkdb_info` redis commands. The output from `raft_info`
  should look a bit like this:

  ```
  127.0.0.1:7777> raft_info
   1) "TERM 5"
   2) "LOG-START 0"
   3) "LOG-SIZE 1"
   4) "MYSELF localhost:7777"
   5) "LEADER localhost:7777"
   6) "MEMBERSHIP-EPOCH 0"
   7) "NODES localhost:7777,localhost:7778,localhost:7779"
   8) "OBSERVERS "
   9) "CLUSTER-ID your-cluster-id"
  10) "STATUS LEADER"
  11) "COMMIT-INDEX 0"
  12) "LAST-APPLIED 0"
  13) "BLOCKED-WRITES 0"
  ```

  You can now start issuing writes towards the leader. Try `set mykey myvalue` and
  `get mykey` in `redis-cli`.

  A quarkdb cluster is operational as long as at least a majority (or *quorum*)
  of nodes are alive and available: at least **2 out of 3**, 3 out of 5, 4 out of 7, etc.
