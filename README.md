# ValkeyJSON

ValkeyJSON introduces a native JSON data type to Valkey open source.
JSON data interchange standard. With this feature, users can store, query, and modify JSON data structures in Valkey using a comprehensive JSONPath query language. The feature will be compatible with the API and RDB formats supported by Valkey

## Pre-requisite:
Python - 3.9
Pytest - 4

## Building ValkeyJSON module and run tests.

To build the module and the tests
```text
./build.sh
```

## Building ValkeyJSON module only.

To build just the module
```text
mdkir build
cd build
cmake .. -DVALKEY_VERSION=unstable
make
```

## Unit Tests

To run all unit tests:
```text
cd build
make -j unit
```

## Integration Tests

To run all integration tests:
```text
make -j test
```
