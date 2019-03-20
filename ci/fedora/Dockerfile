# A fedora image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM fedora:29
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2019

RUN dnf clean all
RUN dnf remove -y whois-mkpasswd || true
RUN dnf install -y expect git
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/fedora/prepare.sh; cd ..; rm -rf quarkdb
