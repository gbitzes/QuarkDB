# A cc7-base image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM fedora:28
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2018

RUN dnf clean all
RUN dnf install -y git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/fedora/prepare.sh; cd ..; rm -rf quarkdb
