# Journal trimming

As you already know, all writes into QuarkDB during raft mode are first recorded
into the raft journal. This is a necessary step during consensus and replication,
since all writes must first be stored into a quorum of nodes before being committed.

However, we cannot store all writes since ever, that would cause the raft journal
to grow out of control in size. It's necessary to occasionally trim it and only
keep the last N entries.

Two configuration options control trimming:

* The number of entries to keep in the raft journal. Default value is 50 million
journal entries. Values below 1 million are probably not reasonable for a production
deployment, and values below 100k are disallowed.

* The batch size to use during trimming: This many entries will be deleted at once
when it's time to apply trimming. Default value is 1 million. Aim for batch sizes
around `1/50` of the total number of entries to keep.

You can change the above values with the following command towards the leader. This
sets total number of entries to keep at 10M, and batch size at 200k.

```
redis-cli -p 7777 config-set raft.trimming 10000000:200000
```

You can view all configuration options with the following:

```
redis-cli -p 7777 config-getall
```

Recommendation: Keep the default values, unless the raft-journal directory is
starting to consume too much space.

You can see current trimming status by running `raft-info`:

```
127.0.0.1:7777> raft-info
 1) TERM 12259
 2) LOG-START 35939700000
 3) LOG-SIZE 35949744300
 ...
```

The above means that a total of `35949744300` writes have been recorded in the
journal so far, but the first `35939700000` have been trimmed already. Only
the last `35949744300 - 35939700000 = 10044300` entries are still available.
