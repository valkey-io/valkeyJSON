from utils_json import DEFAULT_MAX_PATH_LIMIT, \
    DEFAULT_STORE_PATH
from valkey.exceptions import ResponseError, NoPermissionError
from valkeytests.conftest import resource_port_tracker
import pytest
import glob
import logging
import os
import random
import struct
import json
from math import isclose, isnan, isinf, frexp
from json_test_case import JsonTestCase


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
