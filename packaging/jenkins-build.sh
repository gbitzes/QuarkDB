#!/usr/bin/env bash

#-------------------------------------------------------------------------------
# Print help
#-------------------------------------------------------------------------------
function printHelp()
{
  echo "Usage:                                                               " 1>&2
  echo "${0} <branch_or_tag> <xrootd_tag> <build_number> <dst_path>          " 1>&2
  echo "  <branch_or_tag> branch name in the form of \"origin/master\" or tag" 1>&2
  echo "                  name e.g. 1.0.0 for which to build the project     " 1>&2
  echo "  <xrootd_tag>    XRootD tag version used for this build             " 1>&2
  echo "  <build_number>  build number value passed in by Jenkins            " 1>&2
  echo "  <platform>      build platform e.g. slc-6, el-7, fc-24             " 1>&2
  echo "  <architecture>  build architecture e.g. x86_64, i386               " 1>&2
  echo "  <dst_path>      destination path for the rpms built                " 1>&2
}

#-------------------------------------------------------------------------------
# Main - when we are called the current BRANCH_OR_TAG is already checked-out and
#        the script must be run from the **same directory** where it resides.
#-------------------------------------------------------------------------------
if [[ ${#} -ne 6 ]]; then
    printHelp
    exit 1
fi

set -e

BRANCH_OR_TAG=${1}
XROOTD_TAG=${2}
BUILD_NUMBER=${3}
PLATFORM=${4}
ARCHITECTURE=${5}
DST_PATH=${6}

# Create cmake build directory and build without dependencies
cd ..; mkdir build; cd build
cmake .. -DPACKAGEONLY=1

# Build the SRPMs
make srpm
SRC_RPM=$(find ./SRPMS -name "*.src.rpm" -print0)

# Get the mock configurations from gitlab
git clone ssh://git@gitlab.cern.ch:7999/dss/dss-ci-mock.git ../dss-ci-mock

# Prepare the mock configuration
head -n -1 ../dss-ci-mock/eos-templates/${PLATFORM}-${ARCHITECTURE}.cfg.in | sed "s/__XROOTD_TAG__/$XROOTD_TAG/" | sed "s/__BUILD_NUMBER__/${BUILD_NUMBER}/" > qdb.cfg
echo -e '"""' >> qdb.cfg

DIST=".${PLATFORM//-}"
mock --yum --init --uniqueext="qdb_${BRANCH}" -r ./qdb.cfg --rebuild ./${SRC_RPM} --resultdir ../rpms -D "dist ${DIST}" --with=server

# Make sure the directories are created and rebuild the YUM repo
YUM_REPO_PATH="${DST_PATH}/master/commit/${PLATFORM}/${ARCHITECTURE}"
echo "Save RPMs in YUM repo: ${YUM_REPO_PATH}"
aklog
mkdir -p ${YUM_REPO_PATH}
cp -f ../rpms/*.rpm ${YUM_REPO_PATH}
createrepo -q ${YUM_REPO_PATH}
