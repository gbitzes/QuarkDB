#!/usr/bin/env bash
set -ex

apt update
apt install -y git g++ cmake zlib1g-dev openssl libssl-dev python libbz2-dev lcov uuid-dev

git submodule update --init --recursive

# Build xrootd, use cache if available
if [[ "$XRD_BUILD" == "" ]]; then
  XRD_BUILD=$PWD/xrootd
fi

if [ ! "$(ls -A $XRD_BUILD)" ]; then
  mkdir -p $XRD_BUILD
  pushd $XRD_BUILD
  rm -rf xrootd
  git clone https://github.com/xrootd/xrootd
  pushd xrootd
  git checkout v4.8.4
  mkdir build && pushd build

  XRD_INSTALL=$PWD/install
  cmake .. -DCMAKE_INSTALL_PREFIX=$XRD_INSTALL && make
  make install
  popd; popd; popd
fi
XRD_INSTALL="$XRD_BUILD/xrootd/build/install"

# Build QuarkDB
rm -rf build
mkdir build && pushd build
CXXFLAGS='-fsanitize=address' cmake -DTESTCOVERAGE=ON -DXROOTD_ROOT_DIR=$XRD_INSTALL -DLIBRARY_PATH_PREFIX=lib ..
make
./test/quarkdb-tests
./test/quarkdb-stress-tests
make coverage-report
