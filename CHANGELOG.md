# Changelog
All notable changes to this project will be documented in this file.

## Unreleased

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
