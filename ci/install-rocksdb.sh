#!/usr/bin/env bash
set -e

# Install RocksDB into the build image, so we don't have to
# re-compile it again and again for each commit.

# Make the directory as the first thing, so as to fail fast
git submodule update --init --recursive
ROCKSDB_PATH=$(ci/canonical-rocksdb-path.sh)
mkdir -p $ROCKSDB_PATH

# Find available cmake command
CMAKE="cmake3"
if ! which $CMAKE; then
  CMAKE="cmake"
fi

# Build RocksDB using the same command that would be used if
# there was no caching.
rm -rf build
mkdir build && cd build
$CMAKE .. -DPACKAGEONLY=1 -DBUILD_ROCKSDB=1
make BuildRocksDB

# Copy necessary files.
cp deps/rocksdb/src/BuildRocksDB/librocksdb.a $ROCKSDB_PATH/
cp -r deps/rocksdb/src/BuildRocksDB/include $ROCKSDB_PATH/

