# A C8 image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM gitlab-registry.cern.ch/linuxsupport/c8-base:latest
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2020

RUN dnf clean all
RUN dnf remove -y whois-mkpasswd || true
RUN dnf install -y expect git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/c8/prepare.sh; cd ..; rm -rf quarkdb
