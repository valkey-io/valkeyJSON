#!python3
#
# Load a JSON file and create a key.
# Usage:
# [HOST=<host>] [PORT=<port>] [SSL=<ssl>] python3 load_1file_hostport.py <path_to_json> <key>
#
# e.g.
# python3 load_1file_hostport.py ../amztests/data/wikipedia.json wikipedia
# PORT=6380 python3 load_1file_hostport.py ../amztests/data/wikipedia.json wikipedia
#
import redis, sys, os, logging
from redis.exceptions import ResponseError, ConnectionError, TimeoutError

if len(sys.argv) < 3:
    print("Usage: [HOST=<host>] [PORT=<port>] [SSL=<ssl>] python3 load_1file_hostport.py <path_to_json> <redis_key>")
    exit(1)

host = os.getenv('HOST', 'localhost')
port = os.getenv('PORT', '6379')
ssl = os.getenv('SSL', 'False')
is_ssl = (ssl == 'True')
json_file_path = sys.argv[1]
key = sys.argv[2]

r = redis.Redis(host=host, port=port, ssl=is_ssl, socket_timeout=3)
try:
    r.ping()
    logging.info(f"Connected to valkey {host}:{port}, ssl: {is_ssl}")
except (ConnectionError, TimeoutError):
    logging.error(f"Failed to connect to valkey {host}:{port}, ssl: {is_ssl}")
    exit(1)

def load_file(json_file_path, key):
    with open(json_file_path, 'r') as f:
        data = f.read()
    r.execute_command('json.set', key, '.', data)
    logging.info("Created key %s" %key)

load_file(json_file_path, key)
