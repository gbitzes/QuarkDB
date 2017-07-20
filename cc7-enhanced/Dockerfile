# A cc7-base image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base:latest
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2017

RUN yum clean all
RUN yum install -y git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/cc7/prepare.sh
RUN rm -rf /quarkdb

