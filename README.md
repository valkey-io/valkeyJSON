# ValkeyJSON

ValkeyJSON is a C++ Valkey-Module that provides native JSON (JavaScript Object Notation) support for Valkey. The implementation complies with RFC7159 and ECMA-404 JSON data interchange standards. Users can natively store, query, and modify JSON data structures using the JSONPath query language. The query expressions support advanced capabilities including wildcard selections, filter expressions, array slices, union operations, and recursive searches.

ValkeyJSON leverages [RapidJSON](https://rapidjson.org/), a high-performance JSON parser and generator for C++, chosen for its small footprint and exceptional performance and memory efficiency. As a header-only library with no external dependencies, RapidJSON provides robust Unicode support while maintaining a compact memory profile of just 16 bytes per JSON value on most 32/64-bit machines.

## Building and Testing

#### To build the module and run tests
```text
# Build valkey-server (unstable) and run integration tests
./build.sh
```

The default valkey version is "unstable". To override it, do:
```text
# Build valkey-server (8.0.0) and run integration tests
SERVER_VERSION=8.0.0 ./build.sh
```

Custom compiler flags can be passed to the build script via environment variable CFLAGS. For example:
```text
CFLAGS="-O0 -Wno-unused-function" ./build.sh
```

#### To build just the module
```text
mdkir build
cd build
cmake ..
make
```

The default valkey version is "unstable". To override it, do:
```text
mdkir build
cd build
cmake .. -DVALKEY_VERSION=8.0.0
make
```

Custom compiler flags can be passed to cmake via variable CFLAGS. For example:
```text
mdkir build
cd build
cmake .. -DCFLAGS="-O0 -Wno-unused-function"
make
```

#### To run all unit tests:
```text
cd build
make -j unit
```

#### To run all integration tests:
```text
make -j test
```

#### To run one integration test:
```text
TEST_PATTERN=<test-function-or-file> make -j test
```
e.g.,
```text
TEST_PATTERN=test_sanity make -j test
TEST_PATTERN=test_rdb.py make -j test
```

## Load the Module
To test the module with a Valkey, you can load the module using any of the following ways:

#### Using valkey.conf:
```
1. Add the following to valkey.conf:
    loadmodule /path/to/libjson.so
2. Start valkey-server:
    valkey-server /path/to/valkey.conf
```

#### Starting valkey with --loadmodule option:
```text
valkey-server --loadmodule /path/to/libjson.so
```

#### Using Valkey command MODULE LOAD:
```
1. Connect to a running Valkey instance using valkey-cli
2. Execute Valkey command:
    MODULE LOAD /path/to/libjson.so
```
## Supported  Module Commands
```text
JSON.ARRAPPEND
JSON.ARRINDEX
JSON.ARRINSERT
JSON.ARRLEN
JSON.ARRPOP
JSON.ARRTRIM
JSON.CLEAR
JSON.DEBUG
JSON.DEL
JSON.FORGET
JSON.GET
JSON.MGET
JSON.MSET (#16)
JSON.NUMINCRBY
JSON.NUMMULTBY
JSON.OBJLEN
JSON.OBJKEYS
JSON.RESP
JSON.SET
JSON.STRAPPEND
JSON.STRLEN
JSON.TOGGLE
JSON.TYPE
```
