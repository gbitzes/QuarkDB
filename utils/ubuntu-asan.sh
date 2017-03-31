#!/usr/bin/env bash
set -e

apt update
apt install -y git g++ cmake zlib1g-dev openssl libssl-dev libhiredis-dev python libbz2-dev

git submodule update --init --recursive
git clone https://github.com/xrootd/xrootd
pushd xrootd
git checkout v4.6.0
mkdir build && pushd build

XRD_INSTALL=$PWD/install
cmake .. -DCMAKE_INSTALL_PREFIX=$XRD_INSTALL && make
make install
popd; popd

mkdir build && pushd build
CXXFLAGS='-fsanitize=address' cmake -DXROOTD_ROOT_DIR=$XRD_INSTALL -DLIBRARY_PATH_PREFIX=lib ..
make
./test/quarkdb-tests
