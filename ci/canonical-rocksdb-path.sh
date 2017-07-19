#!/usr/bin/env bash
set -e

# Discover rocksdb commit hash
cd deps/rocksdb
ROCKSDB_COMMIT=$(git rev-parse HEAD)

# Emit the system-wide path for this rocksdb commit
printf "/rocksdb/$ROCKSDB_COMMIT"

