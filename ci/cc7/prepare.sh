#!/usr/bin/env bash
set -e

##------------------------------------------------------------------------------
## Bootstrap packages - needed to run 'builddep' on quarkdb for the next
## step.
##------------------------------------------------------------------------------

yum install -y gcc-c++ cmake3 make rpm-build which git yum-plugin-priorities yum-utils

##------------------------------------------------------------------------------
## Extract quarkdb build dependencies from its specfile.
##------------------------------------------------------------------------------

./packaging/make-srpm.sh
yum-builddep -y build/SRPMS/*

##------------------------------------------------------------------------------
## Misc packages, needed for the publishing step.
##------------------------------------------------------------------------------

yum install -y sssd-client sudo createrepo http-parser http-parser-devel npm

##------------------------------------------------------------------------------
## Install gitbook, needed for publishing docs.
##------------------------------------------------------------------------------

pushd ~/
npm install -g gitbook-cli
npm install gitbook
popd
gitbook build # Gitbook will install more stuff during its first execution

##------------------------------------------------------------------------------
## Install rocksdb
##------------------------------------------------------------------------------

scl enable devtoolset-6 "ci/install-rocksdb.sh"
