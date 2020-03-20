# Backup

## How to backup my data?

Let's assume there's QuarkDB running at ```/var/lib/quarkdb``` - how to backup
this directory?

First of all, it is a **bad** idea to directly copy the files of a running, live instance.
Please don't do that! Between the time you start the backup, to the time it finishes,
the underlying SST files will have likely changed, resulting in a backup that is corrupted.

Instead, take a checkpoint by issuing ``raft-checkpoint /path/to/backup``,
which will create a point-in-time consistent snapshot, containing both state machine
and journal, from which you will be able to easily spin up another QDB instance if need be.

Please make sure that ```/var/lib/quarkdb``` is on the **same** physical filesystem
as ```/path/to/backup```. This allows hard-linking the SST files, resulting in a
backup that takes virtually no additional space on the disk, and takes a couple
of seconds to create, even if your DB is half a terabyte.

After that, you should be able to stream or rsync this directory over the network.
Once you're done, make sure to **remove** it. Otherwise, the contents of ```/var/lib/quarkdb```
and ```/path/to/backup``` will soon start diverging as QDB processes more writes,
and new SST files are written. In short: ```/path/to/backup``` will no longer be
"for free", and start to consume actual disk space.

## Restore

Once we have a checkpoint, here are the steps to spin up an entirely new QuarkDB
instance out of it:

1. If the checkpoint was produced by a standalone instance, you can skip this
step. Otherwise, you need to change the hostname associated to the raft journal
by running the following command:

  ```
  quarkdb-recovery --path /path/to/backup/current/raft-journal --command "recovery-force-reconfigure-journal localhost:8888| new-cluster-uuid"
  ```

  This way, the new node will identify as ```localhost:8888```, for example, instead
  of whatever hostname the machine we retrived the backup from had. Replace
  ```new-cluster-uuid``` with a unique string, such as a UUID.

2. It should now be possible to directly spin up a new node from the checkpoint
   directory - example configuration file for raft mode:

   ```
   xrd.port 8888
   xrd.protocol redis:8888 libXrdQuarkDB.so
   redis.mode raft
   redis.database /path/to/backup
   redis.myself localhost:8888
   ```

   Make sure to use the same hostname:port pair in the configuration file, as
   well as in ```quarkdb-recovery``` command invocation.

   The resulting cluster will be raft-enabled, and single-node. It's possible
   to expand it through regular membership updates.
