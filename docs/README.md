# QuarkDB

[QuarkDB](https://gitlab.cern.ch/eos/quarkdb) is a highly available datastore that implements a small subset
of the redis command set, developed by IT-ST at CERN.

We build on top of [rocksdb](https://github.com/facebook/rocksdb), an embeddable, transactional
key-value store.

High availability is achieved through multiple replicated nodes and the
[raft](https://raft.github.io) distributed consensus algorithm.

# Getting started

Visit [this chapter](GETTING-STARTED.md) for instructions on how to get a
QuarkDB cluster up and running.


There's also a short [screencast demo](https://asciinema.org/a/NdX791Ah4JVkGQnUQkBVm3dDJ),
which shows how to set up a test cluster on localhost.
