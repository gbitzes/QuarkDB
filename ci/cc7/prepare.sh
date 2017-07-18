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

mkdir build
pushd build
cmake3 .. -DPACKAGEONLY=1
make srpm
yum-builddep -y SRPMS/*
popd

##------------------------------------------------------------------------------
## Misc packages, needed for the publishing step.
##------------------------------------------------------------------------------

yum install -y sssd-client sudo http-parser http-parser-devel npm createrepo
