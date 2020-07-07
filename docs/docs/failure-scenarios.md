
## What happens when I'm running a three-node cluster, and...

### ... one machine is rebooted?

The cluster will continue operating normally, simply without the affected node.
Once reboot is complete, the node will rejoin the cluster and catch up on any missed
writes, automatically.

* **Data loss?** No
* **Downtime?** No

### ... two or more machines are rebooted, simultaneously?

The cluster will lose quorum until two or more nodes are up and running.

* **Data loss?** No
* **Downtime?** <span style="color:red">**Yes**</span>

### ... one QuarkDB process crashes?

The cluster will continue operating normally, simply without the affected node.
Once the node is back, it will rejoin the cluster and catch up on any missed
writes, automatically.

* **Data loss?** No
* **Downtime?** No

### ... two or more QuarkDB processes crash, simultaneously?

No writes will be lost, all are flushed to the kernel before being acknowledged
to the client. Things are different if the *kernel* crashes, though!

* **Data loss?** No
* **Downtime?** <span style="color:red">**Yes**</span>

### ... hard drive dies irreparably? (one node affected)

The cluster will continue operating normally, simply without the broken node,
as long as it has quorum. You might want to remove the broken node as a
cluster member, and add a different one.

* **Data loss?** No
* **Downtime?** No

### ... there's a power cut, or kernel crash? (one node affected)

The cluster will continue operating normally, simply without the broken node.
Once the underlying issue is resolved, the node will rejoin the
cluster and catch up on any missed writes, automatically.

* **Data loss?** No
* **Downtime?** No

### ... there's a simultaneous power cut across two or more nodes?

The default [fsync policy](fsync.md) issues an fsync on the underlying disk
around once per second, which means a simultaneous power cut across two or more
nodes could potentially cause the loss of the last second of writes.

* **Data loss?** <span style="color:red">**Potentially**</span>, last second of writes
* **Downtime?** <span style="color:red">**Yes**</span>

## What happens when I'm running a single-node cluster, and...

### ... the machine is rebooted?

* **Data loss?** No
* **Downtime?** <span style="color:red">**Yes**</span>

### ... hard drive dies irreparably?

* **Data loss?** <span style="color:red">**Yes, complete.**</span> Better have backups :)
* **Downtime?** <span style="color:red">**Yes**</span>

### ... QuarkDB crashes?

No writes will be lost, all are flushed to the kernel before being acknowledged
to the client. Things are different if the *kernel* crashes, though!

* **Data loss?** No
* **Downtime?** <span style="color:red">**Yes**</span>

### ... there's a power cut, or kernel crash?

* **Data loss?** <span style="color:red">**Potentially**</span>, last second of writes
* **Downtime?** <span style="color:red">**Yes**</span>
