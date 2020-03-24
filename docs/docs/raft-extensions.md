# Raft extensions in QuarkDB

Although we follow the raft algorithm closely, we have made several improvements.
Understanding the rest of this page requires having a good understanding of
[raft](https://raft.github.io/raft.pdf), please read the paper first.

1. An RPC which only serves as a heartbeat.

	  Even though _appendEntries_ serves as a heartbeat, it can be problematic: A
	  pipelined storm of gigantic in size _appendEntries_ messages will heavily
	  influence message acknowledgement reception latencies. When using short raft
	  timeouts, this can easily lead to spurious timeouts and re-elections.

	  For this reason, we use a separate thread on the leader node which regularly
	  sends heartbeats, decoupled from replication.

	  The heartbeat request contains two fields:

	  * The raft term of the contacting node, for which it is a leader.
	  * The server identifier (host:port) of the contacting node.

	  The heartbeat response contains two fields as well:

	  * The raft term of the node being contacted.
	  * Whether or not the heartbeat was successful, that is, whether the contacted
	    node recognizes the sender as leader for the specified term.

	  The fact that this exchange doesn't access the journal at all makes this a
	  separate process from replication, as certain locks don't need to be taken,
	  and makes the cluster much more robust against spurious timeouts.

1. Veto responses on vote requests.

	  In QuarkDB, a node can reply in three different ways to a vote request:

	  * Granted
	  * Denied
	  * Veto

	  A veto is simply a stronger form of vote denial. The responder communicates to
	  the sender that, if they were to become a leader, a critical safety violation
	  would occur on raft invariants. Namely, if elected, the contacting node would
	  attempt to overwrite already committed journal entries with conflicting ones.

	  Clearly, raft already protects from the above scenario -- it's simply an extra,
	  paranoid precaution. It should never happen that a node receives a quorum of
	  positive votes, plus a non-zero number of vetoes. If it does happen, we print
	  a serious error in the logs, and the node does not become leader, even though
	  having received a quorum of positive votes.

	  However, this mechanism is quite useful in a different way as well: A node
	  receiving a veto _knows_ it cannot possibly be the next leader of this cluster,
	  as its journal does not contain all committed entries. Therefore, a veto
	  begins a period of election embargo, during which a node will willingly stop
	  attempting to elect itself, up until the moment it is contacted by a leader node.

	  This is useful in a number of ways:
	  
	  * Entirely mitigates "Disruptive Servers" scenario (see section 4.2.3 of the
	    Raft PhD thesis), in which a node which is not aware it has been removed from
	    the cluster repeatedly attempts to become elected, thus making the cluster
	    unavailable. In QuarkDB, such a node will get vetoed, and willingly stop
	    making further election attempts.
	  * During 1-way network partitions, where a machine is able to initiate TCP connections
	    to other boxes, but others are not able to do the same, QuarkDB nodes will be
	    unable to receive heartbeats, but able to initiate voting rounds. (We have
	    had this happen in production _:)_ ) The constant
	    election attempts would normally bring the entire cluster down, even if a quorum
	    of nodes are available, and the network partition only affects a single node.
	    However, the veto mechanism will quickly calm down the partitioned node, and
	    it will simply wait until the partitioned has been healed, without disrupting
	    the rest.
