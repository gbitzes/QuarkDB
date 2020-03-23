# QuarkDB, a highly available datastore

## Introduction

QuarkDB is a highly available datastore speaking the redis wire protocol (RESP2), being
developed by IT-ST at CERN.

Highlights:

* Highly available through replication and the [raft](https://raft.github.io) distributed consensus algorithm.
* Built on top of [rocksdb](https://github.com/facebook/rocksdb), a transactional key-value store.
* Support for hashes, sets, strings, leases, `MULTI`, pubsub, and more.

We run it in production at CERN, serving as the namespace backend for [EOS](https://eos.web.cern.ch),
storing metadata for billions of files.

## Getting started

Visit [this chapter](installation.md) for instructions on how to get a
QuarkDB cluster up and running. Check out the [production checklist](checklist.md) before
running in production.

There's also a short [screencast demo](https://asciinema.org/a/NdX791Ah4JVkGQnUQkBVm3dDJ),
which shows how to set up a test cluster on localhost.
