# QuarkDB, a highly available datastore

## Introduction

QuarkDB is a highly available datastore speaking a redis-compatible protocol, being
developed by IT-ST at CERN.

Highlights:

* Highly available through replication and the [raft](https://raft.github.io) distributed consensus algorithm.
* Built on top of [rocksdb](https://github.com/facebook/rocksdb), a transactional key-value store.
* Support for hashes, sets, strings, leases, `MULTI`, pubsub, and more.

We run it in production at CERN, serving as the namespace backend for [EOS](https://eos.web.cern.ch),
storing metadata for billions of files.

## Getting started

Visit [this chapter](installation.md) for instructions on how to get a
QuarkDB cluster up and running.

There's also a short [screencast demo](https://asciinema.org/a/NdX791Ah4JVkGQnUQkBVm3dDJ),
which shows how to set up a test cluster on localhost.
