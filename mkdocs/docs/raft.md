# Raft basics

A far more comprehensive description can be found in the [raft paper](https://raft.github.io/raft.pdf).
Our implementation follows it closely.

A node can be in three states:

1. Leader, or master. This is where all write and read requests from clients
go - there can only be one leader at any point in time.

  A leader maintains its position by sending regular heartbeats to all others
  in the cluster.

1. Follower, or slave: such a node will not accept any read or write requests,
but redirect them to the leader.

1. Candidate: if a node is not receiving heartbeats from the current leader, it
will initiate an election, asking for votes from its peers in the cluster.

## Failover

During normal operation, a leader will be long-lived, potentially lasting for
weeks or months at a time.

What happens when it fails? Assuming there's still a quorum in the cluster, the
following will happen:

1. The leader fails, and stops sending heartbeats.
1. The heartbeat timeout in one of the followers expires, it becomes a candidate
and starts an election, sending vote requests to its peers.
1. Most of the time, the election will succeed, with its peers voting positively
for it.
1. The candidate receives the positive vote replies, and ascends as the new leader.

A node will vote negatively if it has a more up-to-date journal than
the candidate - in that case, the cluster might need more than one election
round before selecting its new leader, but failover will usually be very quick,
within a few of seconds.

If there's no quorum, meaning that less than a majority of nodes are available,
the election rounds will continue ad-infinitum, and the cluster will remain
unavailable for reading or writing.

## Semi-synchronous replication

Writes are replicated semi-synchronously, meaning that a client receives an
acknowledgement to a write only after it has been replicated to a quorum of
nodes, thus guaranteeing that it won't be lost, even if the leader crashes
immediately after.
