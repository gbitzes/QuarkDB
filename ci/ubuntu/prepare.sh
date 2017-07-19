#!/usr/bin/env bash

ci/ubuntu/packages.sh
ci/ubuntu/build-xrootd.sh /xrootd
ci/install-rocksdb.sh

