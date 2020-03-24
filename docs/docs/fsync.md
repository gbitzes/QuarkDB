# Fsync policy

In an ideal world, every write into QuarkDB would first be flushed to stable storage
(fsync'ed) before being acknowledged, thus ensuring maximum durability in
case of sudden power blackouts, or kernel crashes.

Given the very high cost of fsync (often in the tens of milliseconds)
QuarkDB does not by default fsync on every journal write, as that reduces throughput
by around a factor of 10 or more, and introduces excessive wear on the underlying
disk / SSD.

Instead, starting from version 0.4.1 the journal is always fsync'ed once per second
in a background thread. This limits any potential data loss during a power blackout
to the last second of writes.

In addition to the above, the journal can be configured with the following fsync
policies:

* async: Journal is synced at the discretion of the OS.
* sync-important-updates (**default**): Important writes relating to the raft state
are explicitly synced. This protects raft invariants after a blackout, ensuring
for example that no node votes twice for the same term due to forgetting its
vote after the blackout.
* always: Journal is synced for **each and every** write - expect massive perfromance reduction.

The default is *sync-important-updates*, as it represents a good tradeoff: During
infrequent but critical raft events (voting, term changes, membership changes)
the journal is synced to ensure raft state remains sane even after simultaneous,
all-node power blackouts.

Current fsync policy can be viewed through ``raft-info``, and changed through ``raft-set-fsync-policy``:

```
some-host:7777> raft-set-fsync-policy sync-important-updates
OK
```

Things to note:

* fsync policy is specific to each node **separately**, remember to change
  the policy on every node of your cluster.
* A single node going down due to power blackout should not be a problem, as the
  rest will re-populate any lost entries through replication. Data loss becomes
  a possibility if multiple nodes are powered off *simultaneously*.
* Only power blackouts and kernel crashes pose such a problem. If just QuarkDB crashes
  (even by ``kill -9``), no data loss will occur.

