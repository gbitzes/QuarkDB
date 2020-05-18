#!/usr/bin/env bash
set -ex

export TZ=Europe/Zurich
export DEBIAN_FRONTEND=noninteractive

apt-get install -y git g++ cmake zlib1g-dev openssl libssl-dev python python3 libbz2-dev lcov uuid-dev libjemalloc-dev libdw-dev libdw1 liblz4-dev libzstd-dev
