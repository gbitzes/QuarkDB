#!/usr/bin/env bash
set -e

##------------------------------------------------------------------------------
## Bootstrap packages - needed to run 'builddep' on quarkdb for the next
## step.
##------------------------------------------------------------------------------

dnf install -y gcc-c++ cmake3 make rpm-build which git yum-plugin-priorities yum-utils libtsan

##------------------------------------------------------------------------------
## Extract quarkdb build dependencies from its specfile.
##------------------------------------------------------------------------------

./packaging/make-srpm.sh
yum-builddep -y build/SRPMS/*

##------------------------------------------------------------------------------
## Install rocksdb, both with and without tsan
##------------------------------------------------------------------------------

ci/install-rocksdb.sh
CXXFLAGS='-fsanitize=thread' ci/install-rocksdb.sh
