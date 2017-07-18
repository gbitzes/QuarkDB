#!/usr/bin/env bash
set -e

# Usage: build-xrootd.sh <path>
# xrootd will then be installed into <path>/install

# If path exists, we assume the contents have been
# cached already, and we quit.

if [[ "$#" != 1 || "$1" == "" ]]; then
  printf "Usage: build-xrootd.sh <path>\n"
  exit 1
fi

BASE_PATH="$1"

if [ "$(ls -A $BASE_PATH)" ]; then
  printf "$BASE_PATH not empty: assuming xrootd has been installed already there.\n"
  exit 0
fi

mkdir -p $BASE_PATH
cd $BASE_PATH
git clone https://github.com/xrootd/xrootd
cd xrootd
git checkout v4.6.0
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX="$BASE_PATH/install"
make -j
make -j install

