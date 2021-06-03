#!/bin/bash
#
# This script will build MammothDB Distributed Execution subsystem
# Requires build-common.sh

# Package-specific constants
RPM_PACKAGE="mammothdb-dexec"

# Find build-common.sh and source it
CURR_DIR=`pwd`
PROJECT_DIR=`dirname $CURR_DIR`
if [ -e /usr/libexec/mammothdb/build-server ] ; then
	BUILD_SERVER_DIR=/usr/libexec/mammothdb/build-server
	source $BUILD_SERVER_DIR/build-server/build-common.sh
else
	echo "ERROR: Unable to find build-common.sh"
	exit 1;
fi

# Call the common entry point
build_common $@

# Check out proper version
git_checkout

# Copy requried source files into rpmbuild source tree
cp -r $CHECKOUT_DIR/* $RPM_HOME/BUILD

# Copy the spec file for the build
copy_spec_file

# Build the RPM
build_rpms

# Declare we're good
happy_end

