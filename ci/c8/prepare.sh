#!/usr/bin/env bash
set -e

##------------------------------------------------------------------------------
## Bootstrap packages - needed to run 'builddep' on quarkdb for the next
## step.
##------------------------------------------------------------------------------

dnf install -y expect gcc-c++ cmake3 make rpm-build which git yum-utils libtsan dnf-plugins-core python3 epel-release

##------------------------------------------------------------------------------
## Extract quarkdb build dependencies from its specfile.
##------------------------------------------------------------------------------

./packaging/make-srpm.sh
dnf builddep -y build/SRPMS/*

##------------------------------------------------------------------------------
## Install rocksdb
##------------------------------------------------------------------------------
ci/install-rocksdb.sh
