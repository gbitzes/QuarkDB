#!/usr/bin/env bash
set -e

# This step ought to be a no-op usually, thanks to our custom ubuntu image.
ci/ubuntu/prepare.sh

# Build QuarkDB with AddressSanitizer
git submodule update --init --recursive
rm -rf build
mkdir build && cd build
CXXFLAGS='-fsanitize=address' cmake -DTESTCOVERAGE=ON -DXROOTD_ROOT_DIR=/xrootd/install -DLIBRARY_PATH_PREFIX=lib ..
make

# export ASAN_OPTIONS='detect_leaks=0'
QDB_TEST_TIMEOUT=default ./test/quarkdb-tests
./test/quarkdb-stress-tests
make coverage-report
