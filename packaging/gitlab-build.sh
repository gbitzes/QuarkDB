#!/usr/bin/env bash
set -e

git submodule update --init --recursive
mkdir build && cd build
cmake3 .. -DPACKAGEONLY=1
make srpm
yum-builddep -y SRPMS/*
rpmbuild --rebuild --with server --define "_build_name_fmt %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm" SRPMS/*
