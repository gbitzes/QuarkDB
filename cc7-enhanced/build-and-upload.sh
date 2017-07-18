#!/usr/bin/env bash

docker login -u gitlab-ci-token -p $CI_BUILD_TOKEN gitlab-registry.cern.ch
docker build -t gitlab-registry.cern.ch/eos/quarkdb/cc7-enhanced .
docker push gitlab-registry.cern.ch/eos/quarkdb/cc7-enhanced

