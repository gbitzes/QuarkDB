# Bulkload mode

QuarkDB supports a special mode during which writes are sped up significantly,
at the cost of disallowing reads. Preventing reads enables several optimizations,
such as not having to maintain an in-memory index in order to support key
lookups for memtables.

Bulkload mode is available _only_ for newly created instances. If you try to
open in bulkload an instance which contains data already (either standalone
_or_ raft instance), the server will refuse to start.

## Starting a node in bulkload mode

1. Use quarkdb-create to make the database directory, specifying only the path.

    ```
    quarkdb-create --path /var/lib/quarkdb/bulkload
    ```

2. Start the QuarkDB process, specify `bulkload` as the mode in the configuration:

    ```
    xrd.port 4444
    xrd.protocol redis:4444 libXrdQuarkDB.so
    redis.mode bulkload
    redis.database /var/lib/quarkdb/bulkload
    ```

3. Here are some example commands ran towards a bulkload node:

    ```
    127.0.0.1:4444> quarkdb-info
    1) MODE BULKLOAD
    2) BASE-DIRECTORY /var/lib/quarkdb/bulkload
    3) QUARKDB-VERSION 0.3.7
    4) ROCKSDB-VERSION 5.18.3
    5) MONITORS 0
    6) BOOT-TIME 0 (0 seconds)
    7) UPTIME 18 (18 seconds)
    127.0.0.1:4444> set test 123
    OK
    127.0.0.1:4444> get test
    (nil)
    ```

    Note how attempting to read just-written values results in an empty string: All
    reads during bulkload will receive empty values.

4. Now you are free to load QuarkDB with any data you wish.

## Finalizing bulkload

Once your data is written, how to turn this into a fully functioning raft cluster,
with reads enabled and replication?

1. Run ``quarkdb-bulkload-finalize`` - this needs to be the last command ran
   on the node, more writes are disallowed from this point on. Let the command
   run, which may take a while.

2. Once done, shut down the QuarkDB process.

3. To initialize a new raft cluster out of bulkloaded data:

    1. Run ``quarkdb-create --path /var/lib/quarkdb/raft --clusterID my-cluster-id
    --nodes node1:7777,node2:7777,node3:7777 --steal-state-machine /var/lib/quarkdb/bulkload/current/state-machine``.

    2. ``/var/lib/quarkdb/raft`` now contains the full data. _Copy_ this directory
    in full into _all_ nodes destined to be part of this new cluster: ``scp -r /var/lib/quarkdb/raft node1:/some/path``

    3. Create the xrootd configuration files for each node, pointing to the location
    where the above directory was copied into.

4. To initialize a new standalone node out of bulkloaded data:

    1. Run ``quarkdb-create --path /var/lib/quarkdb/standalone --steal-state-machine /var/lib/quarkdb/bulkload/current/state-machine``

    2. Create the xrootd configuration file as usual, pointing to ``/var/lib/quarkdb/standalone``.

