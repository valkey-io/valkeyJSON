#!/usr/bin/env bash

# Sometimes processes are left running when test is cancelled.
# Therefore, before build start, we kill all running test processes left from previous test run.
echo "Kill old running test"
pkill -9 -x Pytest || true
pkill -9 -f "valkey-server.*:" || true
pkill -9 -f Valgrind || true
pkill -9 -f "valkey-benchmark" || true

# cd to the current directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

export MODULE_PATH=$2/build/src/libjson.so \
export SERVER_VERSION="unstable"

if [[ ! -z "${TEST_PATTERN}" ]] ; then
    export TEST_PATTERN="-k ${TEST_PATTERN}"
fi

BINARY_PATH=".build/binaries/$SERVER_VERSION/valkey-server"

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