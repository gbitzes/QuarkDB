#!/usr/bin/env bash
set -e

# Install RocksDB into the build image, so we don't have to
# re-compile it again and again for each commit.

# Make the directory as the first thing, so as to fail fast
ROCKSDB_PATH=$(ci/canonical-rocksdb-path.sh)
mkdir -p $ROCKSDB_PATH


# Build RocksDB using the same command that would be used if
# there was no caching.
git submodule update --init --recursive
rm -rf build
mkdir build && cd build
cmake .. -DPACKAGEONLY=1 -DBUILD_ROCKSDB=1
make BuildRocksDB

# Copy necessary files.
cp deps/rocksdb/src/BuildRocksDB/librocksdb.a $ROCKSDB_PATH/
cp -r deps/rocksdb/src/BuildRocksDB/include $ROCKSDB_PATH/

