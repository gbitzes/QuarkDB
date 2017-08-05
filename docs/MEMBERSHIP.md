# Membership updates

QuarkDB supports dynamic changes to cluster membership without any impact on
availability. Caution needs to be taken that at any point in time, a quorum of
nodes is available and up-to-date for the cluster to function properly.

# Distinction between full nodes and observers

Consider the following:

1. We've been running a cluster in production consisting of nodes n1, n2, and n3.
1. One day n2 dies, so we add n4 to the cluster, without removing n2 first.
The leader starts the procedure of bringing n4 up-to-date. Quorum size becomes
three, since there are now four nodes in total.
1. Remember that writes have to be replicated to a quorum of nodes before they
are acknowledged to clients.
1. n2 remains dead, and n4 will take time to be brought up-to-date if the database
size is large. Writes can be replicated only to n1 and n3, which is less then the
quorum size, so they will all be **stalled** until n4 becomes up-to-date,
making the cluster unavailable for writes.

To prevent such an incident, QuarkDB discriminates between two types of nodes:

1. **Full members** participate in voting rounds and are capable of becoming leaders.

1. **Observers** receive all replicated entries, just like full nodes, but do
not affect quorums, do not vote, are not taken into consideration when deciding whether
a write has been successful, and will never attempt to become leaders.

The idea is to first add a node as an observer (which will *not* in any way
affect quorum size, or availability), and once it has been brought up to date,
promote it to full member.

QuarkDB will further make an effort to refuse membership updates which might
compromise availability, as a protection against operator error, but please
keep the above in mind.

# How to view current cluster membership

Issue command `raft_info` using `redis-cli` to any of the nodes, check fields `NODES` and
`OBSERVERS`. It's perfectly valid if the list of observers is empty.

# How to add a node

It's only possible to add a node with observer status at first. Issue
`raft_add_observer server_hostname:server_port` towards the current leader.

# How to promote an observer to full status

Issue `raft_promote_observer server_hostname:server_port` towards the current
leader.

First make sure it is sufficiently up to date! Issue `raft_info` towards the leader
to check which replicas are online, which are up-to-date, and which are lagging.

# How to remove a node

Issue `raft_remove_member server_hostname:server_port` towards the current leader.
Works both on full members, as well as observers.

It's not possible to remove a node which is currently a leader. To do that, stop
the node, wait until the new leader emerges, and issue `raft_remove_node` towards
it.

A membership update is represented internally as a special kind of log entry.
This means that a removed node will often not know that it has been removed,
since the cluster stops replicating entries onto it. Such a node will also
stop receiving heartbeats, and thus trigger elections indefinitely.

There is built-in protection against such disruptive nodes, so this will not
affect the rest of the cluster, but it is highly recommended to stop QuarkDB
from running on removed nodes.
