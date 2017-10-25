#!/usr/bin/env bash

#-------------------------------------------------------------------------------
# Generate a source RPM - run this from the root fo the git repository.
#-------------------------------------------------------------------------------

VERSION_FULL=$(./genversion.py --template-string "@VERSION_FULL@")
printf "Version: ${VERSION_FULL}\n"

./packaging/make-dist.sh
TARBALL="quarkdb-${VERSION_FULL}.tar.gz"
BUILD_DIR=$PWD/build

RPM_DEFINE="--define \"_source_filedigest_algorithm md5\" --define \"_binary_filedigest_algorithm md5\""

pushd build
rpmbuild --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5" -ts ${TARBALL} --define "_topdir ${BUILD_DIR}" --with server


