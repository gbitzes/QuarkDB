# Version upgrades

## Can I upgrade without downtime?

_Yes_, a routine version upgrade should require only a single leader election.
The recommended steps are the following:

1. Use ``raft-info`` to find out the current cluster leader.

2. For each of the _followers_, do the following _one by one_:

    * Upgrade system packages.

    * Restart the QuarkDB service.

    * Wait until the leader declares the node as back online, and as having an up-to-date
    journal. Run ``raft-info`` on the leader node, check ``REPLICA`` section for this
    information.

3. Finally, upgrade and restart the leader. The followers will detect the
  absence of heartbeats, and elect a new leader among themselves within a
  few seconds.

Upgrading the followers _one by one_ and not all at once is **important**: If you
simultaneously upgrade both followers on a 3-node cluster, the cluster could become
unavailable for a couple of minutes due to loss of quorum until the nodes come back
online.

## What not to do

All following methods are worse than the above, since they cause longer downtime
than a single leader election.

* Upgrade the leader first. Inevitably, leadership will jump to one of the followers,
which will have to be restarted too at some point for the upgrade, resulting
in 2 or more elections for the upgrade in total.

* Upgrade all QuarkDB daemons at the same time: The cluster could potentially
go down for minutes, depending on how quickly the processes are able to come back
online.

* An upgrade when quorum is shaky: For example, if only 2 out of 3 nodes are
available (maybe the third died from a broken hard drive), an upgrade of any
remaining node will cause loss of quorum for the duration of time it takes for
QuarkDB to restart.
