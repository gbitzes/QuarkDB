# Upgrading

It's possible to upgrade a QuarkDB cluster with minimal impact on availability:
The time needed to perform a single leader election, which is typically a couple
of seconds.

The recommended upgrade procedure is the following:

* Find out the current cluster leader by running ``raft-info`` command.

* For each follower node, upgrade the system packages, restart the QuarkDB
  service, and wait until the leader declares the node online, and having
  an up-to-date journal.
  Run ``raft-info`` on the leader node, check ``REPLICA`` section for this
  information.

* Make sure to upgrade the followers _one by one_, not all at once. If you
  simultaneously upgrade both followers on a 3-node cluster, for example, the
  cluster will become unavailable due to loss of quorum until the nodes come
  back online. This could take a couple of minutes.

* Finally, upgrade and restart the leader. The followers will detect the
  absence of heartbeats, and elect a new leader among themselves within a
  few seconds.

## Sub-optimal ways of upgrading QuarkDB

All following methods are worse than the above, since they cause longer downtime
than a single leader election:

* Restart the leader first. Inevitably, leadership will go to one of the followers,
which will have to be restarted too at some point for an upgrade, resulting
in 2 or more elections for the upgrade in total.

* Restart all QuarkDB daemons at the same time: The cluster could potentially
go down for long, depending on how quickly the processes are able to come back
online. For large databases, this could take a couple of minutes.

* An upgrade when quorum is shaky: For example, if only 2 out of 3 nodes are
available (maybe the third died from a broken hard drive), an upgrade of any
remaining node will cause loss of quorum for the duration of time it takes for
QuarkDB to restart.
