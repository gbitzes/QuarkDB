# Disk space

## How much space do I need?

* The **raft journal** will grow in size until it hits the [trimming](journal-trimming.md) threshold.
For pipelined small writes (1-2 kb), expect around 20-40 GB in the default trimming configuration,
but this could vary a lot.

	If the journal is consuming too much space, consider lowering the trimming threshold.

* The **state machine** will grow in proportion to how much data is stored.

## What about when storing the EOS namespace?

The general rule of thumb is 0.1 - 0.2 GB per million metadata entries. The actual figure will vary
depending on how long, or how compressible the filenames are. Examples from some of our instances:

| Number of files | Number of directories | Raft journal | State machine | Size per million metadata entries for state machine |
|-----------------|-----------------------|--------------|---------------|-----------------------------------------------------|
| 236M            | 51M                   |   24 GB      |  39 GB        | 0.13 GB                                             |
| 145M            | 9M                    |   28 GB      |  17 GB        | 0.11 GB                                             |
| 657M            | 65M                   |   28 GB      |  104 GB       | 0.14 GB                                             |
| 310M            | 27M                   |   39 GB      |  32 GB        | 0.09 GB                                             |
| 152M            | 16M                   |   37 GB      |  15 GB        | 0.09 GB                                             |
| 186M            | 13M                   |   38 GB      |  18 GB        | 0.09 GB                                             |
| 3292M           | 138M                  |   33 GB      |  428 GB       | 0.12 GB                                             |




