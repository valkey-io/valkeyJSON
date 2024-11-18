# ValkeyJSON

ValkeyJSON is a C++ Valkey-Module that provides native JSON (JavaScript Object Notation) support for Valkey. The implementation complies with RFC7159 and ECMA-404 JSON data interchange standards. Users can natively store, query, and modify JSON data structures using the JSONPath query language. The query expressions support advanced capabilities including wildcard selections, filter expressions, array slices, union operations, and recursive searches.

ValkeyJSON leverages [RapidJSON](https://rapidjson.org/), a high-performance JSON parser and generator for C++, chosen for its small footprint and exceptional performance and memory efficiency. As a header-only library with no external dependencies, RapidJSON provides robust Unicode support while maintaining a compact memory profile of just 16 bytes per JSON value on most 32/64-bit machines.

## Motivation
While Valkey core lacks native JSON support, there's significant community demand for JSON capabilities. ValkeyJSON provides a comprehensive open-source solution with extensive JSON manipulation features.

## Building and Testing

#### To build the module and run tests
```text
# Builds the valkey-server (unstable) for integration testing.
SERVER_VERSION=unstable
./build.sh

# Builds the valkey-server (8.0.0) for integration testing.
SERVER_VERSION=8.0.0
./build.sh
```

#### To build just the module
```text
mdkir build
cd build
cmake .. -DVALKEY_VERSION=unstable
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
JSON.MSET
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