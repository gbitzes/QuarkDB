# Backup

## QuarkDB is replicated, do I really need backups?

Yes, absolutely! Replication is very different from a backup. While
replication will most likely protect your data against a failing disk,
it will not protect from software bugs or other accidents.

## Cool, how to take a backup?

1. Create a point-in-time checkpoint by issuing ``quarkdb-checkpoint /path/to/backup`` redis command on the node you wish to backup. Both `state-machine` and `raft-journal` are included in the checkpoint.

2. Validate the newly created checkpoint using the command-line tool ``quarkdb-validate-checkpoint``.

3. Stream or rsync the contents of `/path/to/backup` over the network to the intended long-term storage destination.

4. Delete `/path/to/backup`.


Things to note:

- ``/path/to/backup`` should be on the same physical filesystem as the actual
data. This allows hard-linking of the underlying SST files, resulting in a backup
that consumes virtually no additional space on disk, and takes a couple of
seconds to create even for large datasets.

- ``/path/to/backup`` may not consume much disk space at first, but it will as
soon as the contents between ``/path/to/backup`` and your DB start to diverge
significantly. Make sure to delete it once the contents have been copied to
their final destination.

## Can't I just copy the main data directory? (ie ```/var/lib/quarkdb```)

Nooo. When directly copying the files of a running live instance you are likely
to end up with a backup that is corrupted. Between the time you start the backup
to the time it finishes, the underlying SST files will have likely changed.

If the node is not currently running, however, just copying the main data directory
is safe.

## How to restore?

Restore works by creating an entirely new cluster out of a checkpoint. However, our checkpoint
initially remembers the old cluster members --- we need to reconfigure it and specify
new node and clusterID.

1. Change the cluster's `MEMBERS` and `CLUSTER-ID` using `quarkdb-recovery` command
line tool:

    ```
    quarkdb-recovery --path /path/to/backup/current/raft-journal --command "recovery-force-reconfigure-journal localhost:8888| new-cluster-uuid"
    ```

    This way, the new cluster will be composed of a single new member, such as
    ```localhost:8888``` and have a different clusterID. This is important to
    prevent the old and new clusters from accidentally interfering with one another.

2. Spin up a new node from the checkpoint directory --- example configuration file:

     <pre>```
xrd.port 8888
xrd.protocol redis:8888 libXrdQuarkDB.so
redis.mode raft
redis.database /path/to/backup
redis.myself localhost:8888
     ```</pre>

     Use the same host:port pair in `redis.myself` and```quarkdb-recovery``` command invocation.


The resulting cluster will be raft-enabled, and single-node. It's possible
to expand it through regular membership updates.
