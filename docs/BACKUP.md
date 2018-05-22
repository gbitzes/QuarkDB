# Backup & restore

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
