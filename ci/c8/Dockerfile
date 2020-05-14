# A C8 image + build dependencies of quarkdb.
# Significantly improves CI build time, since all packages
# are already there.

FROM gitlab-registry.cern.ch/linuxsupport/c8-base:latest
MAINTAINER Georgios Bitzes, georgios.bitzes@cern.ch, CERN 2020

RUN dnf clean all
RUN dnf remove -y whois-mkpasswd || true
RUN dnf install -y expect git
RUN dnf install -y https://kojipkgs.fedoraproject.org//packages/zstd/1.4.4/1.el8/x86_64/libzstd-1.4.4-1.el8.x86_64.rpm https://kojipkgs.fedoraproject.org//packages/zstd/1.4.4/1.el8/x86_64/libzstd-devel-1.4.4-1.el8.x86_64.rpm
RUN git clone https://gitlab.cern.ch/eos/quarkdb.git; cd quarkdb; ci/c8/prepare.sh; cd ..; rm -rf quarkdb
