#!/usr/bin/env bash

# This step ought to be a no-op usually, thanks to our custom ubuntu image.
ci/ubuntu/prepare.sh

# Determine rocksdb cached build
ROCKSDB_CACHED_BUILD=$(./ci/rocksdb-cmake.sh)

# Build QuarkDB with AddressSanitizer
git submodule update --init --recursive
rm -rf build
mkdir build && cd build
CXXFLAGS='-fsanitize=address' cmake -DTESTCOVERAGE=ON -DXROOTD_ROOT_DIR=/xrootd/install -DLIBRARY_PATH_PREFIX=lib ${ROCKSDB_CACHED_BUILD} ..
make
./test/quarkdb-tests
./test/quarkdb-stress-tests
make coverage-report

