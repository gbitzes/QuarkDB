#!/usr/bin/env bash
set -e

# Install RocksDB into the build image, so we don't have to
# re-compile it again and again for each commit.

# Check: Does the cache directory exist already, and is not empty?
# If so, assume it contains a cached rocksdb build already, and skip.
git submodule update --init --recursive
ROCKSDB_PATH=$(ci/canonical-rocksdb-path.sh)

if [ "$(ls -A $ROCKSDB_PATH)" ]; then
  printf "$ROCKSDB_PATH not empty: assuming rocksdb has been installed already there.\n"
  exit 0
fi

# Make the directory as the first thing, so as to fail fast
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
