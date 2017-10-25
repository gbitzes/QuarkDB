#!/usr/bin/env bash
set -e

# Detect if we're using ThreadSanitizer
if [[ "${CXXFLAGS}" == *"-fsanitize=thread"* ]]; then
  TSAN_IF_ENABLED="-tsan"
fi

# Discover rocksdb commit hash
ROCKSDB_COMMIT=$(git --git-dir deps/rocksdb/.git rev-parse HEAD)

# Emit the system-wide path for this rocksdb commit
printf "/rocksdb/${ROCKSDB_COMMIT}${TSAN_IF_ENABLED}"
