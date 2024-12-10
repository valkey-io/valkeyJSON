from utils_json import DEFAULT_MAX_PATH_LIMIT, DEFAULT_STORE_PATH
from json_test_case import JsonTestCase
from valkeytests.conftest import resource_port_tracker
import logging, os, pathlib

logging.basicConfig(level=logging.DEBUG)

class TestRdb(JsonTestCase):

    def setup_data(self):
        client = self.server.get_new_client()
        client.config_set(
            'json.max-path-limit', DEFAULT_MAX_PATH_LIMIT)
        # Need the following line when executing the test against a running Valkey.
        # Otherwise, data from previous test cases will interfere current test case.
        client.execute_command("FLUSHDB")

        # Load strore sample JSONs. We use strore.json as input to create a document key. Then, use
        # strore_compact.json, which does not have indent/space/newline, to verify correctness of serialization.
        with open(DEFAULT_STORE_PATH, 'r') as file:
            self.data_store = file.read()
        assert b'OK' == client.execute_command(
            'JSON.SET', 'store', '.', self.data_store)

    def setup(self):
        super(TestRdb, self).setup()
        self.setup_data()

    def test_rdb_saverestore(self):
        """
        Test RDB saving
        """
        client = self.server.get_new_client()
        assert True == client.execute_command('save')
        client.execute_command('FLUSHDB')
        assert b'OK' == client.execute_command('DEBUG', 'RELOAD', 'NOSAVE')

    def test_import_rejson_rdbs(self):
        '''
        Verify we can load RDBs generated from ReJSON.
        Each RDB file contains JSON key "store" (data/store.json).
        '''
        self.load_rdbs_from_dir('rdb_rejson')

    def load_rdbs_from_dir(self, dir):
        src_dir = os.getenv('SOURCE_DIR')
        rdb_dir = f"{src_dir}/tst/integration/{dir}"
        assert os.path.exists(rdb_dir)
        for (dirpath, dirnames, filenames) in os.walk(rdb_dir):
            for filename in filenames:
                if pathlib.Path(filename).suffix == '.rdb':
                    file_path = os.path.join(dirpath, filename)
                    self.load_rdb_file(file_path, filename)

    def load_rdb_file(self, rdb_path, rdb_name):
        new_path = os.path.join(self.testdir, rdb_name)
        os.system(f"cp -f {rdb_path} {new_path}")
        logging.info(f"Loading RDB file {new_path}")
        self.client.execute_command(f"config set dbfilename {rdb_name}")
        self.client.execute_command("debug reload nosave")
        self.verify_keys_in_rejson_rdb()

    def verify_keys_in_rejson_rdb(self):
        assert b'["The World Almanac and Book of Facts 2021"]' == self.client.execute_command('json.get', 'store', '$..books[?(@.price>10&&@.price<22&&@.isbn)].title')
