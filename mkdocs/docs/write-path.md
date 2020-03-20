# Write path

This is a rough summary of what happens for every successful write to
QuarkDB - all the events that occur between the time a client issues a command,
and the reception of an acknowledgement.

The following is a simplification, glossing over certain details, such as what
happens if the leader node crashes in the middle of some step,
or certain optimizations such as write batching.

1. A client issues a write command towards a random QuarkDB node in the cluster,
   for example ```SET mykey myvalue```.

1. Assuming the client is unlucky, and the random node it contacted is not the
   current leader, a redirect is returned asking the client to try again towards
   the leader.

1. The client issues ```SET mykey myvalue``` again, this time towards the leader.

1. The leader receives the request, parses it, and appends it to its local raft journal.
   Its ```LOG-SIZE``` increments by one.

1. The replicator tracker of the leader notices an entry was just appended, and
   issues ```APPEND-ENTRIES``` RPC towards all followers. The RPC includes
   the contents of the write operation: ```SET mykey myvalue```.

1. The followers receive ```APPEND-ENTRIES``` RPC, and append the new write
   to their local journals - their local ```LOG-SIZE``` increment by one, as well.
   The followers then acknowledge to the leader that they have received the
   new entry.

1. As soon as the leader receives enough acknowledgements that allows it to determine
   that a quorum of nodes (including itself) contain the new entry, it declares
   the new write as *commited*, and increments its local ```COMMIT-INDEX```.

1. After an entry is considered committed, it is then applied to the leader's state
   machine. ```LAST-APPLIED``` is incremented atomically together with the application
   of the entry, and the leader finally issues an acknowledgement to the client
   that the write was successful.

There are a few more details worth mentioning:

* The local ```COMMIT-INDEX``` of followers is synchronized with that of the
  leader regularly - each ```APPEND-ENTIES``` RPC includes the
  leader's current ```COMMIT-INDEX```.

* Followers run a background thread which continuously applies incoming, committed
  entries to their local state machines. This way, each follower is ready to quickly
  take over in case the leader fails.

* A client will cache who the leader currently is for subsequent requests, thus
  saving the second step and additional useless network roundtrips.

* The replicator will batch multiple entries into each ```APPEND-ENTRIES``` whenever
  possible, thus amortizing replication latency over many writes. Clients need
  to issue pipelined writes to benefit from this optimization.
