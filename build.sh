#!/bin/bash

# Script to build valkeyJSON module, build it and generate .so files, run unit and integration tests.

# # Exit the script if any command fails
set -e

SCRIPT_DIR=$(pwd)
echo "Script Directory: $SCRIPT_DIR"

# If environment variable SERVER_VERSION is not set, default to "unstable"
if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION environment variable is not set. Defaulting to \"unstable\"."
    export SERVER_VERSION="unstable"
fi

# Variables
BUILD_DIR="$SCRIPT_DIR/build"

# Build the Valkey JSON module using CMake
echo "Building valkeyJSON..."
if [ ! -d "$BUILD_DIR" ]; then
    mkdir $BUILD_DIR
fi
cd $BUILD_DIR
if [ -z "${CFLAGS}" ]; then
  cmake .. -DVALKEY_VERSION=${SERVER_VERSION}
else
  cmake .. -DVALKEY_VERSION=${SERVER_VERSION} -DCFLAGS=${CFLAGS}
fi
make

# Running the Valkey JSON unit tests.
echo "Running Unit Tests..."
make -j unit

cd $SCRIPT_DIR

REQUIREMENTS_FILE="requirements.txt"

# Check if pip is available
if command -v pip > /dev/null 2>&1; then
    echo "Using pip to install packages..."
    pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
# Check if pip3 is available
elif command -v pip3 > /dev/null 2>&1; then
    echo "Using pip3 to install packages..."
    pip3 install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
else
    echo "Error: Neither pip nor pip3 is available. Please install Python package installer."
    exit 1
fi

export MODULE_PATH="$SCRIPT_DIR/build/src/libjson.so"

# Running the Valkey JSON integration tests.
echo "Running the integration tests..."
cd $BUILD_DIR
make -j test

echo "Build and Integration Tests succeeded"