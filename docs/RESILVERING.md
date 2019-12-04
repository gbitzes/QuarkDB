# Resilvering

Consider the following situation:

* We have a three node QuarkDB cluster. One of the machines goes down for 10 mintues
as part of scheduled maintainance. At the time of shutdown, the node's journal
contained entries 100-500.
* When it comes back up,  the leader's journal contains entries 100-1000.
* Bringing the node up to date with the leader is trivial: Just send entries
501-1000, and we are done.

However, what happens if the intervention lasts far longer, such as a few weeks?

* When the leader comes up, it contains entries 600-3000.
* There's no way to sync the node that was offline, since the leader has
[trimmed](JOURNAL-TRIMMING.md) from its local journal entries 500-600.

The same situation occurs if a node dies from a broken hard drive, and all its data
is lost. If the leader has [trimmed](JOURNAL-TRIMMING.md) any entries whatsoever from the beginning of
its journal, bringing the follower back on sync is not possible by pushing
the missing entries, as we don't have them anymore.

In such situation, the leader will snapshot its local database (journal + state machine),
and stream it over to the follower replacing its entire current contents. We call this
process resilvering. Essentially we rebuild the entire contents of a node, which
is quite time-consuming, only in cases where we don't have any other options.

In case you'd like to disable automatic resilvering
(and perform the above snapshot + copy manually when needed), run the following:

```
redis-cli -p 7777 config-set raft.resilvering.enabled FALSE
```
