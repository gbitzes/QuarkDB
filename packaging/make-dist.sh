#!/usr/bin/env bash
set -e

#-------------------------------------------------------------------------------
# Generate a release tarball - run this from the root of the git repository.
#-------------------------------------------------------------------------------

mkdir -p build
./genversion.py
./genversion.py --template packaging/quarkdb.spec.in --out packaging/quarkdb.spec

#-------------------------------------------------------------------------------
# Detect if the rocksdb cache is available.
#-------------------------------------------------------------------------------
ROCKSDB_CACHE=$(ci/rocksdb-cmake.sh)
sed "s!@ROCKSDB_CACHED_BUILD@!${ROCKSDB_CACHE}!g" packaging/quarkdb.spec -i

#-------------------------------------------------------------------------------
# Extract version number, we need this for the archive name
#-------------------------------------------------------------------------------
VERSION_FULL=$(./genversion.py --template-string "@VERSION_FULL@")
printf "Version: ${VERSION_FULL}\n"
FILENAME="quarkdb-${VERSION_FULL}"

#-------------------------------------------------------------------------------
# Make the archive
#-------------------------------------------------------------------------------
TARGET_PATH=$(basename $PWD)

pushd $PWD/..
tar --exclude '*/.git' --exclude "${TARGET_PATH}/build" --exclude "${TARGET_PATH}/_book" -pcvzf ${TARGET_PATH}/build/${FILENAME}.tar.gz ${TARGET_PATH} --transform "s!${TARGET_PATH}!${FILENAME}!" --show-transformed-names
popd
