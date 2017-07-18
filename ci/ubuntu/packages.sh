#!/usr/bin/env bash
set -ex

apt-get update
apt-get install -y git g++ cmake zlib1g-dev openssl libssl-dev libhiredis-dev python libbz2-dev lcov uuid-dev

