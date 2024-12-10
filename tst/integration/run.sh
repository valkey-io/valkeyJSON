#!/usr/bin/env bash

# Sometimes processes are left running when test is cancelled.
# Therefore, before build start, we kill all running test processes left from previous test run.
echo "Kill old running test"
pkill -9 -x Pytest || true
pkill -9 -f "valkey-server.*:" || true
pkill -9 -f Valgrind || true
pkill -9 -f "valkey-benchmark" || true

# If environment variable SERVER_VERSION is not set, default to "unstable"
if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION environment variable is not set. Defaulting to \"unstable\"."
    export SERVER_VERSION="unstable"
fi

# cd to the current directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

export SOURCE_DIR=$2
export MODULE_PATH=${SOURCE_DIR}/build/src/libjson.so
echo "Running integration tests against Valkey version $SERVER_VERSION"

if [[ ! -z "${TEST_PATTERN}" ]] ; then
    export TEST_PATTERN="-k ${TEST_PATTERN}"
fi

BINARY_PATH=".build/binaries/${SERVER_VERSION}/valkey-server"

if [[ ! -f "${BINARY_PATH}" ]] ; then
    echo "${BINARY_PATH} missing"
    exit 1
fi

if [[ $1 == "test" ]] ; then
    python -m pytest --html=report.html --cache-clear -v ${TEST_FLAG} ./ ${TEST_PATTERN}
else
    echo "Unknown target: $1"
    exit 1
fi

