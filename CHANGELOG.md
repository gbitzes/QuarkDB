# Changelog

## Unreleased

### Bug fixes
- The mechanism meant to provide an early warning for potential ``MANIFEST``
corruption was flaky, and would sometimes report a problem where none existed.

Many thanks to Franck Eyraud (JRC) for the bug report concerning erroneous ``MANIFEST``-related
warning.

### Improvements
- Implementation of an optional part of raft, pre-vote. This should prevent partitioned,
or otherwise flaky rejoining servers from triggering needless elections. A node will first issue
an experimental voting round before advancing its term, and start campaigning
for real only if it has a good chance of winning.

## 0.4.2 (2020-03-12)

### Bug fixes
- Under complicated conditions (follower is very far behind leader + network instabilities),
replication towards a particular follower could become stuck. (to workaround, restart leader node)
- Running ``DEL`` on a lease key would cause all nodes in a cluster to crash
with an assertion. ``DEL`` will now simply release the given lease, as if
``lease-release`` had been called.

### New features
- Implement command ``quarkdb-verify-checksum`` for manually running a full checksum scan.
- Addition of ``quarkdb-validate-checkpoint`` tool for ensuring that a given
checkpoint is valid -- useful to run in backup scripts before streaming a given
checkpoint for long-term storage.

### Improvements
- Security hardening of the redis parser for unauthenticated clients.
- Package and distribute ``quarkdb-ldb`` tool based on the one provided by RocksDB.
- Attempt to detect potential ``MANIFEST`` corruption early by measuring mtime lag
compared to newest SST file.

Many thanks to Crystal Chua (AARNet) for the bug report and all support offered
related to RocksDB's ``MANIFEST`` corruption issue, as well as to Pete Eby (ORNL)
for finding and reporting the bug causing replication to become stuck.

## 0.4.1 (2020-01-17)

### Bug fixes
- Fixed ability to subscribe to multiple channels with one command, when push types
are active. Previously, the server would erroneously send one "OK" response per
channel subscribed, breaking QClient.

### New features
- Possibility to choose between three different journal fsync policies through
``RAFT-SET-FSYNC-POLICY`` command.
- Implementation of ``CLIENT GETNAME``, and automatic tagging of intercluster
connections.

### Improvements
- Automatic fsync of the raft journal once per second.
- Better cluster resilience in case of sudden machine powercuts.

Many thanks to Franck Eyraud (JRC) for the bug reports relating to sudden poweroff, and valuable discussion on fsync behavior.

## 0.4.0 (2019-12-06)

### Bug fixes
- Locality hints ending with a pipe symbol (|) could subsequently trigger an
assertion and crash when encountered during ``LHSCAN``, due to faulty key parsing code.
The pipe symbol (|) has special meaning inside internal QuarkDB keys, and is used
to escape field separators (#).

### New features
- Addition of ``quarkdb-server`` binary to allow running QDB without XRootD.
- Add support for ``CLIENT SETNAME`` command as aid in debugging.

### Improvements
- Improvements to replication behaviour when one of the followers is very far behind the leader.
Previously, an excessive number of entries were kept in the request pipeline, which
wasted memory and could potentially trigger OOM.
- Switch to CLI11 for command line argument parsing.
- Upgrade rocksdb dependency to v6.2.4.

## 0.3.9 (2019-09-20)

### Bug fixes
- ``DEQUE-SCAN-BACK`` was returning the wrong cursor to signal end of
iteration: ``next:0`` while it should have been ``0``.
- A race condition was sometimes causing elections to fail spuriously.
Establishing a stable quorum would occasionally require slightly more election
rounds than it should have.

### New features
- Implementation of health indicators through ``QUARKDB-HEALTH`` command.
- Added support for RESPv3 push types, activated on a per-client basis through
``ACTIVATE-PUSH-TYPES`` command.
- Implementation of ``LHLOCDEL`` command for conditionally deleting a locality hash
field, only if the provided hint matches.
- Add convenience command ``DEQUE-CLEAR``.
- Add support for ``MATCHLOC`` in ``LHSCAN``, used to filter out results based
on locality hint.
- Add ``RECOVERY-SCAN`` command for scanning through complete keyspace, including
internal rocksdb keys.
- Add tool ``quarkdb-sst-inspect`` to allow low-level inspection of SST files.
- Add command ``RAFT-JOURNAL-SCAN`` to make searching through the contents of the
raft journal easier.

### Improvements
- Protection for a strange case of corruption which brought down a development
test cluster. (last-applied jumped ahead of commit-index by 1024, causing all
writes to stall). From now on, similar kind of corruption should only take out
a single node, and not spread to the entire cluster.
- ``KEYS`` is now implemented in terms of ``SCAN``, making prefix matching of the
keyspace just as efficient as with ``SCAN``. (Note: The use of ``KEYS`` is still
generally discouraged due to potentially huge response size)
- Removed unused tool ``quarkdb-scrub``.


## 0.3.8 (2019-05-27)
- Prevent elections from hanging on the TCP timeout when one of the member hosts
is dropping packets, which could bring down an otherwise healthy cluster.
- Prevent crashing when ``LHSCAN`` is provided with a cursor missing the field
component.
- Make request statistics available through ``command-stats`` command.
- Addition of configuration file path to ``quarkdb-info``.
- Print simple error message when the given path to quarkdb-create already exists,
instead of a stacktrace.

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
