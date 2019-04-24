# Changelog
All notable changes to this project will be documented in this file.

## 0.3.7 (2019-04-24)
- Heavy use of lease commands could cause performance degradation and latency
spikes, due to accumulation of long-lived deletion tombstones on expiration
events. Starting from this release, such tombstones should disappear much
more quickly without accumulating.
- Fixed regression introduced in 0.3.6, which made it impossible to create new
bulkload nodes. (Note: to workaround, delete directory ``/path/to/bulkload/current/state-machine``
right after running ``quarkdb-create``)
- Addition of ``LEASE-GET-PENDING-EXPIRATION-EVENTS`` command to list all
pending lease expiration events, and ``RAW-SCAN-TOMBSTONES`` to inspect tombstones
as an aid in debugging.
- Expanding a cluster is now easier, no need to pass ``--nodes`` to quarkdb-create
when creating a new node for an established cluster.

## 0.3.6 (2019-03-21)
- Improved memory management and recycling, putting less pressure on the global
memory allocator.
- Addition of pub/sub support, commands implemented: ``PUBLISH``, ``SUBSCRIBE``,
  ``PSUBSCRIBE``, ``UNSUBSCRIBE``, ``PUNSUBSCRIBE``
- Addition of ``--steal-state-machine`` flag to ``quarkdb-create`` to make
  transition out of bulkload mode easier and less error-prone.
- Updated rocksdb to v5.18.3.

## 0.3.5 (2018-11-28)
- Updated rocksdb dependency to v5.17.2.
- Improved output of backup command ``raft-checkpoint``. The generated directory
can be used directly to spin-up a full QuarkDB node, without manual tinkering.
Command aliased to ``quarkdb-checkpoint``.
- Removed flood of ``attempting connection to .. `` messages when some node is unavailable.
- Light refactoring, more widespread use of ``std::string_view``, which paves the
way for certain performance optimizations in the future.
- Fixed several flaky tests.

## 0.3.4 (2018-10-09)
- Updated rocksdb dependency to v5.15.10.
- Added `TYPE` command.
- A read-only MULTI immediatelly after a read-write MULTI could
  cause the cluster to crash.
- Added command `LHSCAN` for scanning through a locality hash.
- Added convenience command in recovery mode for performing forced membership updates.
- It's now possible to issue one-off commands from the recovery tool, without setting up a server.

## 0.3.3 (2018-09-14)

### Added
- Ability to run QuarkDB in raft mode with only a single node. This allows starting
with a single node, and growing the cluster in the future if needed, which is
cumbersome to do in standalone mode.
- Added `quarkdb-version` command, which simply returns the current quarkdb version.
- Commands `deque-scan-back`, `deque-trim-front`.

### Changed
- Dropped list commands, as the underlying implementation makes it impossible to support all list commands found in official redis. (most notably `linsert`)

  Still, the old implementation makes for an excellent deque. Renamed list commands `lpush`, `lpop`, `rpush`, `rpop`, `rlen` to `deque-push-front`, `deque-pop-front`, `deque-push-back`, `deque-pop-back`, `deque-len`. The underlying data format did not change, only the command names.

  This makes it possible to implement lists properly in the future.

  No-one has been using the list operations, which gives us the opportunity to change the command names.
- Minor code and performance improvements.

## 0.3.2 (2018-08-14)
### Added
- Protection against 1-way network partitions, in which a cluster node
  is able to establish TCP connections to others, but the rest cannot do the same.

  This resulted in cluster disruption as the affected node would not be receiving
  heartbeats, but could still repeatedly attempt to get elected.

  From now on, a node which has been vetoed will abstain from starting election
  rounds until it has received fresh heartbeats since receiving that veto. This
  will prevent a 1-way network partitioned node from causing extensive cluster
  disruption.

### Fixed
- A couple of minor memory leaks.

## 0.3.1 (2018-08-03)
### Added
- Command `hclone` for creating identical copies of entire hashes.

### Fixed
- An `EXEC` when not inside a `MULTI` would cause a crash.

## 0.2.9 (2018-07-16)
### Added
- Commands `convert-string-to-int`, `convert-int-to-string` to convert between
  binary-string-encoded integers and human-readable-ASCII encoding. Meant for
  interactive use only, to make life easier during low-level debugging when
  needing to edit low-level rocksdb keys, where binary-encoded integers are used.

### Changed
- Refactoring of transactions, we no longer pack / unpack a transaction into a single request within the same node, saving
  CPU cycles.
- Explicitly block zero-sized strings when parsing the redis protocol, print appropriate warning.

### Fixed
- In certain cases, such as when redirecting or reporting unavailability for pipelined writes, fewer
  responses might be provided than expected, causing the client connection to hang. This did not affect
  QClient when redirects are active, as it would shut the connection down and retry upon reception of
  the first such response.

## 0.2.8 (2018-07-04)
### Added
- Support for leases, which can be used as locks with timeouts, allowing QuarkDB to serve as a distributed lock manager.
- Commands `lease-acquire`, `lease-get`, `lease-release`.

### Changed
- A newly elected leader now stalls writers in addition to readers, until its leadership marker entry in the raft journal has been committed and applied.

### Fixed
- Sockets and threads from closed connections were not being cleaned due to misunderstanding how XRootD handles connection shutdown. Each connection would hog a socket and a deadlocked thread on the server forever. (oops)
- A particularly rare race condition was able to trigger an assertion in the Raft subsystem, causing the current cluster leader to crash.

## 0.2.7 (2018-06-22)
### Added
- Updated rocksdb dependency to 5.13.4.

### Fixed
- Certain unlikely sequences of pipelined writes were able to trigger an assertion and bring a cluster down, when part of a transaction. Without that assertion, the commands would have left ghost key-value pairs in the rocksdb keyspace.
