#!/usr/bin/env bash

# This step ought to be a no-op usually, thanks to our custom ubuntu image.
ci/ubuntu/prepare.sh

# Build QuarkDB with AddressSanitizer
rm -rf build
mkdir build && cd build
CXXFLAGS='-fsanitize=address' cmake -DTESTCOVERAGE=ON -DXROOTD_ROOT_DIR=$XRD_INSTALL -DLIBRARY_PATH_PREFIX=lib ..
make -j
./test/quarkdb-tests
./test/quarkdb-stress-tests
make coverage-report

