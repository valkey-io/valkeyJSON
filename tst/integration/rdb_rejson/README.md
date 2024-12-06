#### How to create rdb file for a new ReJSON release?

e.g., testing RDB compatibility with rejson 2.2.0.

1. Run docker image redis-stack:
   ```text
   docker run -d -p 6379:6379 --name rejson redislabs/rejson:2.2.0
   ```
2. Load store.json and create a key named "store":
   ```text
   python3 utils/load_1file_hostport.py tst/integration/data/store.json store
   ```
3. Save rdb:
   ```text
   valkey-cli save
   ```
4. Copy out the RDB file:
   ```text
   docker cp $(docker ps -q):/data/dump.rdb tst/integration/rdb_rejson/rejson-<version>.rdb
   ```
5. run test_json_rdb_import.py:
   ```text
   TEST_PATTERN=test_import_rejson_rdbs make test
   ```
