# An ubuntu image + build dependencies of quarkdb + precompiled xrootd.
# Used to improve CI build time.

FROM ubuntu:17.04
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2017

RUN apt-get update; apt-get install -y git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; pushd quarkdb; ci/ubuntu/prepare.sh; popd
RUN rm -rf quarkdb

