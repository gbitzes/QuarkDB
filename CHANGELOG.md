# Changelog
All notable changes to this project will be documented in this file.

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
