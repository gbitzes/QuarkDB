# Troubleshooting

* QuarkDB does not start: `Cannot open '/var/lib/quarkdb/node-01/current/state-machine':IO error: while open a file for lock: /var/lib/quarkdb/node-01/current/state-machine/LOCK: Permission denied`

  Most likely, the database owner permissions are not correct. If running
  with the default systemd init scripts, you need run ``chown -R xrootd:xrootd /var/lib/quarkdb/node-01/``.

* I get this error in the log: `MISCONFIGURATION: received handshake with wrong cluster id: "..." (mine is "...")`

  All members in a cluster have to have the same cluster ID, as provided in `quarkdb-create`.
  You probably assigned a different one to each member.

  Each cluster has a unique identifier assigned to it, which each member node has to know.
  During the initial handshake between two nodes, the cluster ID is exchanged - if
  it does not match, the connection is shut down, and they refuse to communicate.

  This is to prevent damage from a misconfiguration where two nodes from different
  instances accidentally cross paths.

* I get this error in the log: `WARNING: I am not receiving heartbeats - not running for leader since in membership epoch 0 I am not a full node. Will keep on waiting.`

  The node identifier, as provided in `redis.myself`, is not part of the QuarkDB cluster, as
  initialized in ``quarkdb-create``. Either change `redis.myself` to match one of
  the nodes provided in ``quarkdb-create``, or re-run ``quarkdb-create`` with the correct
  nodes.

  Run `redis-cli -p 7777 raft-info` to check the current cluster membership. Each
  node expects to find `redis.myself` within `NODES`, to decide whether it's capable
  of starting an election.

* The nodes are holding elections indefinitely, even though I've started all of them!

  It's likely a firewall issue. Try running
  `redis-cli -h qdb-test-2.cern.ch -p 7777 raft-info` from a different node, for example -
  if you can't connect, it's a firewall issue.
