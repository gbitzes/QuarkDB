# A cc7-base image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM fedora:latest
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2017

RUN dnf clean all
RUN dnf install -y git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; pushd quarkdb; ci/fedora/prepare.sh; popd
RUN rm -rf quarkdb

