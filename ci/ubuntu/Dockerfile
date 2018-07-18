# An ubuntu image + build dependencies of quarkdb + precompiled xrootd.
# Used to improve CI build time.

FROM ubuntu:17.10
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2018

RUN apt-get update; apt-get install -y git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/ubuntu/prepare.sh; cd ..; rm -rf quarkdb
