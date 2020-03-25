# Disk space

## How much space do I need?

* The **raft journal** will grow in size until it hits the [trimming](journal-trimming.md) threshold.
For pipelined small writes (1-2 kb), expect around 20-40 GB in the default trimming configuration,
but this could vary a lot.

	If the journal is consuming too much space, consider lowering the trimming threshold.

* The **state machine** will grow in proportion to how much data is stored.

## What about when storing the EOS namespace?

The general rule of thumb is 0.1 - 0.2 GB per million files stored. Examples from
some of our instances:

| Number of files  | Raft journal | State machine   | Size per million files for state machine |
|------------------|--------------|-----------------|------------------------------------------|
| 236M             |   24 GB      |  39 GB          | 0.16 GB                                  |
| 145M             |   28 GB      |  17 GB          | 0.11 GB                                  |
| 657M             |   28 GB      |  104 GB         | 0.15 GB                                  |
| 310M             |   39 GB      |  32 GB          | 0.10 GB                                  |
| 152M             |   37 GB      |  15 GB          | 0.10 GB                                  |
| 186M             |   38 GB      |  18 GB          | 0.10 GB                                  |
| 3292M            |   33 GB      |  428 GB         | 0.13 GB                                  |




