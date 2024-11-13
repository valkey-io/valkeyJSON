from utils_json import DEFAULT_MAX_PATH_LIMIT, DEFAULT_MAX_DOCUMENT_SIZE, \
    DEFAULT_WIKIPEDIA_COMPACT_PATH, DEFAULT_WIKIPEDIA_PATH, \
    JSON_INFO_METRICS_SECTION, JSON_INFO_NAMES
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

DATA_ORGANISM = '''
            {
                "heavy_animal" : 200,
                "plants" : [
                  {
                    "name"   : "Cactus",
                    "height" : 120,
                    "weight" : 90
                  },
                  {
                    "name"   : "Ghost Plant",
                    "height" : "Unknown",
                    "weight" : "Unknown"
                  },
                  {
                    "name"   : "Redwood",
                    "height" : 4200,
                    "weight" : 50000
                  }
                ],
                "animals" : [
                  {
                    "name"   : "Platypus",
                    "length" : 24,
                    "weight" : 5
                  },
                  {
                    "fish" : [
                      {
                        "name"   : "Bass",
                        "length" : 34,
                        "weight" : 5
                      },
                      {
                        "name"   : "Swordfish",
                        "length" : 177,
                        "weight" : 200
                      }
            
                    ]
                  },
                  {
                    "mammals" : [
                      {
                        "name"   : "Platypus",
                        "length" : 24,
                        "weight" : 5
                      },
                      {
                        "name"   : "Horse",
                        "height" : 68,
                        "weight" : 660
                      },
                      {
                        "primates" : [
                          {
                            "name"   : "Monkey",
                            "height" : 18,
                            "weight" : 30
                          },
                          {
                            "name"   : "Baboon",
                            "height" : 26,
                            "weight" : 50
                          },
                          {
                            "apes" : [
                              {
                                "name"   : "Chimpanzee",
                                "height" : 66,
                                "weight" : 130
                              },
                              {
                                "name"   : "Gorilla",
                                "height" : 66,
                                "weight" : 400
                              },
                              {
                                "name"   : "Orangutan",
                                "height" : 70,
                                "weight" : 300
                              }
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ]
              }
            '''

# valkey keys
wikipedia = 'wikipedia'
wikipedia2 = 'wikipedia2'
wikipedia3 = 'wikipedia3'
wikipedia4 = 'wikipedia4'
store = 'store'
store2 = 'store2'
organism = 'organism'
organism2 = 'organism2'
str_key = 'str_key'
k1 = 'k1'
k2 = 'k2'
k3 = 'k3'
k4 = 'k4'
k5 = 'k5'
k6 = 'k6'
k7 = 'k7'
k8 = 'k8'
k9 = 'k9'
k10 = 'k10'
k11 = 'k11'
k12 = 'k12'
k = 'k'
nonexistentkey = 'nonexistentkey'
nonexistentpath = 'nonexistentpath'
input = 'input'
input2 = 'input2'
arr = 'arr'
foo = 'foo'
baz = 'baz'


# Base Test class containing all core json module tests
class TestJsonBasic(JsonTestCase):

    def setup_data(self):
        client = self.server.get_new_client()
        client.config_set(
            'json.max-path-limit', DEFAULT_MAX_PATH_LIMIT)
        client.config_set(
            'json.max-document-size', DEFAULT_MAX_DOCUMENT_SIZE)
        # Need the following line when executing the test against a running Valkey.
        # Otherwise, data from previous test cases will interfere current test case.
        client.execute_command("FLUSHDB")

        # Load wikipedia sample JSONs. We use wikipedia.json as input to create a document key. Then, use
        # wikipedia_compact.json, which does not have indent/space/newline, to verify correctness of serialization.
        with open(DEFAULT_WIKIPEDIA_PATH, 'r') as file:
            self.data_wikipedia = file.read()
        with open(DEFAULT_WIKIPEDIA_COMPACT_PATH, 'r') as file:
            self.data_wikipedia_compact = file.read()
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.', self.data_wikipedia)

        # Create a string key to be used for verifying that JSON.GET should not operate on a non-document key.
        client.execute_command(
            'SET', str_key, '{"firstName":"John","lastName":"Smith"}')

    def setup(self):
        super(TestJsonBasic, self).setup()
        self.setup_data()

    def test_sanity(self):
        '''
        Test simple SET/GET/MGET/DEL, both legacy and JSONPath syntax.
        '''

        client = self.server.get_new_client()
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', '{"a":"1", "b":"2", "c":"3"}')

        assert b'OK' == client.execute_command(
            'JSON.SET', k2, '.', '[1,2,3,4,5]')
        assert [b'{"a":"1","b":"2","c":"3"}', b'[1,2,3,4,5]'] == client.execute_command(
            'JSON.MGET', k1, k2, '.')
        assert b'{"a":"1","b":"2","c":"3"}' == client.execute_command(
            'JSON.GET', k1)
        for (key, path, exp) in [
            (k1, '.',    '{"a":"1","b":"2","c":"3"}'),
            (k1, '.a',   '"1"'),
            (k2, '.',    '[1,2,3,4,5]'),
            (k2, '[-1]', '5')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path).decode()
        # pretty print
        for (key, fmt, path, exp) in [
            (k1, 'SPACE',  '.',       '{"a": "1","b": "2","c": "3"}'),
            (k1, 'INDENT', '.',       '{ "a":"1", "b":"2", "c":"3"}'),
            (k2, 'INDENT', '.',       '[ 1, 2, 3, 4, 5]'),
            (k2, 'INDENT', '[-2]',    '4')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, fmt, ' ', path).decode()

        assert [b'[{"a":"1","b":"2","c":"3"}]', b'[[1,2,3,4,5]]'] == client.execute_command(
            'JSON.MGET', k1, k2, '$')
        for (key, path, exp) in [
            (k1, '$',       '[{"a":"1","b":"2","c":"3"}]'),
            (k1, '$.*',     '["1","2","3"]'),
            (k2, '$',       '[[1,2,3,4,5]]'),
            (k2, '$.[*]',   '[1,2,3,4,5]'),
            (k2, '$.[-1]',  '[5]'),
            (k2, '$.[0:3]', '[1,2,3]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path).decode()
        # pretty print
        for (key, fmt, path, exp) in [
            (k1, 'SPACE',  '$',       '[{"a": "1","b": "2","c": "3"}]'),
            (k1, 'INDENT', '$',
                '[ {  "a":"1",  "b":"2",  "c":"3" }]'),
            (k1, 'INDENT', '$.*',     '[ "1", "2", "3"]'),
            (k2, 'INDENT', '$',       '[ [  1,  2,  3,  4,  5 ]]'),
            (k2, 'INDENT', '$.[0:3]', '[ 1, 2, 3]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, fmt, ' ', path).decode()

        assert 1 == client.execute_command('JSON.DEL', k1)
        assert 1 == client.execute_command('JSON.DEL', k2)
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '$', '{"a":"1", "b":"2", "c":"3"}')
        assert b'OK' == client.execute_command(
            'JSON.SET', k2, '$', '[1,2,3,4,5]')
        for (key, val_before, del_path, del_ret, val_after) in [
            (k1, '[{"a":"1","b":"2","c":"3"}]', '$.*',   3, '[{}]'),
            (k2, '[[1,2,3,4,5]]',               '$.[*]', 5, '[[]]')
        ]:
            assert val_before == client.execute_command(
                'JSON.GET', key, '$').decode()
            assert del_ret == client.execute_command(
                'JSON.DEL', key, del_path)
            assert val_after == client.execute_command(
                'JSON.GET', key, '$').decode()

    def test_parse_invalid_json_string(self):
        client = self.server.get_new_client()
        for input_str in ['foo', '{"a"}', '[a]']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.SET', wikipedia, '.firstName', input_str)
            assert self.error_class.is_syntax_error(str(e.value))

    def test_json_set_command_supports_all_datatypes(self):
        client = self.server.get_new_client()
        for (path, value) in [('.address.city', '"Boston"'),            # string
                              # number
                              ('.age', '30'),
                              ('.foo', '[1,2,3]'),                      # array
                              # array element access
                              ('.phoneNumbers[0].number', '"1234567"'),
                              # object
                              ('.foo', '{"a":"b"}'),
                              ('.lastName', 'null'),                    # null
                              ('.isAlive', 'false')]:                   # boolean
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, path, value)
            assert value.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

    def test_json_set_command_root_document(self):
        client = self.server.get_new_client()
        # path to the root document is represented as '.'
        for (key, value) in [(k1, '"Boston"'),                # string
                             (k2, '123'),                     # number
                             (k3, '["Seattle","Boston"]'),    # array
                             (k3, '[1,2,3]'),                 # array
                             (k4, '{"a":"b"}'),               # object
                             (k4, '{}'),                      # empty object
                             (k5, 'null'),                    # null
                             (k6, 'false')]:                  # boolean
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', value)
            assert value.encode() == client.execute_command('JSON.GET', key)

    def test_json_set_command_nx_xx_options(self):
        client = self.server.get_new_client()
        for (path, value, cond, exp_set_return, exp_get_return) in [
            ('.address.city', '"Boston"', 'NX', None, b'"New York"'),
            ('.address.city', '"Boston"', 'XX', b'OK', b'"Boston"'),
            ('.foo', '"bar"', 'NX', b'OK', b'"bar"'),
                ('.firstName', '"bar"', 'NX', None, b'"John"')]:
            assert exp_set_return == client.execute_command(
                'JSON.SET', wikipedia, path, value, cond)
            assert exp_get_return == client.execute_command(
                'JSON.GET', wikipedia, path)

            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.SET', k1, '.' '"value"', 'badword')
            assert self.error_class.is_syntax_error(str(e.value))

    def test_json_set_command_with_error_conditions(self):
        client = self.server.get_new_client()
        # A new Valkey key's path must be root
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.SET', foo, '.bar', '"bar"')
        assert self.error_class.is_syntax_error(str(e.value))

        # Option XX means setting the value only if the JSON path exists, a.k.a, updating the value.
        # According to API, if the value does not exist, the command should return null instead of error.
        assert None == client.execute_command(
            'JSON.SET', k1, '.', '"some value"', 'XX')
        assert None == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '"bar"', 'XX')
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.GET', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # Option NX means setting the value only if the JSON path does not exists, a.k.a, inserting the value.
        # According to API, if the value exists, the command should return null instead of error.
        assert None == client.execute_command(
            'JSON.SET', wikipedia, '.', '"some new value"', 'NX')
        assert None == client.execute_command(
            'JSON.SET', wikipedia, '.firstName', '"Tom"', 'NX')
        assert b'"John"' == client.execute_command(
            'JSON.GET', wikipedia, '.firstName')

        # syntax error: wrong option
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.', '"some new value"', 'NXXX')
        assert self.error_class.is_syntax_error(str(e.value))
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.', '"some new value"', 'XXXX')
        assert self.error_class.is_syntax_error(str(e.value))
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.SET', wikipedia, '.', '"bar"', 'a', 'b')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_set_ancestor_keys_should_not_be_overridden(self):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.firstName.a', '"some new value"')
        assert self.error_class.is_write_error(str(e.value))

        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.age.a', '1')
        assert self.error_class.is_write_error(str(e.value))

        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.address.a.b', '"some new value"')
        assert self.error_class.is_write_error(str(e.value))

        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, '.address[0]', '"some new value"')
        assert self.error_class.is_write_error(str(e.value))

    def test_json_set_reject_out_of_boundary_array_index(self):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', wikipedia, 'phoneNumbers[9]', '"123"')
        assert self.error_class.is_outofboundaries_error(str(e.value))

    def test_json_set_insert_value(self):
        client = self.server.get_new_client()
        # insert is allowed if and only if the parent node is the last child in the path.
        for (path, new_val) in [
            ('["address"]["z"]', '"z"'),
            ('.address.z2', '"z2"')
        ]:
            client.execute_command(
                'JSON.SET', wikipedia, path, new_val)
            assert new_val.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

        # if the parent node is not the last child in the path, insertion is not allowed.
        for (path, new_val) in [
            ('["address"]["foo"]["z"]', '"z"'),
            ('.address.foo.z', '"z"')
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.SET', wikipedia, path, new_val)
            assert self.error_class.is_nonexistent_error(str(e.value))

    def test_json_set_negative_array_index(self):
        client = self.server.get_new_client()
        new_val = '"1-2-3"'
        client.execute_command(
            'JSON.SET', wikipedia, '.phoneNumbers[-1].number', new_val)
        assert b'"1-2-3"' == client.execute_command(
            'JSON.GET', wikipedia, '.phoneNumbers[-1].number')
        assert b'["212 555-1234","1-2-3"]' == client.execute_command(
            'JSON.GET', wikipedia, '$.phoneNumbers[*].number')

    def test_json_set_legacy_and_v2path_wildcard(self):
        client = self.server.get_new_client()
        data = '''
                    {"firstName":"John","lastName":"Smith","age":27,"weight":135.17,"isAlive":true,"address":
                    {"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},
                    "phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],
                    "children":[],"spouse":null,"groups":{}}
                    '''
        client.execute_command(
            'JSON.SET', wikipedia2, '.', data)
        client.execute_command(
            'JSON.SET', wikipedia3, '.', data)
        client.execute_command(
            'JSON.SET', wikipedia4, '.', data)

        for (key, path, new_val, exp, path2, exp2) in [
            (wikipedia, '$.address.*',              '"1"',
             b'["1","1","1","1"]', '$.address.*',              b'["1","1","1","1"]'),
            (wikipedia2, '.address.*',               '"1"', b'"1"',
             '$.address.*',              b'["1","1","1","1"]'),
            (wikipedia3, '$.phoneNumbers[*].number', '"1"', b'["1","1"]',
             '$.phoneNumbers[*].number', b'["1","1"]'),
            (wikipedia4, '.phoneNumbers[*].number',  '"1"', b'"1"',
             '$.phoneNumbers[*].number', b'["1","1"]')
        ]:
            client.execute_command(
                'JSON.SET', key, path, new_val)
            assert exp == client.execute_command(
                'JSON.GET', key, path)
            # verify all values
            assert exp2 == client.execute_command(
                'JSON.GET', key, path2)

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2,3]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2, "c":3}}')

        # NOTE: The expected results below account for the outcome of previous commands.
        test_cases = [
            (k1, '$.a[*]', '1', b'[]',
                b'{"a":[],"b":[1],"c":[1,2,3]}'),
            (k1, '$.b[*]', '2', b'[2]',
             b'{"a":[],"b":[2],"c":[1,2,3]}'),
            (k1, '$.c[*]', '4', b'[4,4,4]',
             b'{"a":[],"b":[2],"c":[4,4,4]}'),
            (k1, '.a[*]', '1',  None,       None),
            (k1, '.b[*]', '3',  b'3',
             b'{"a":[],"b":[3],"c":[4,4,4]}'),
            (k1, '.c[*]', '5',  b'5',
             b'{"a":[],"b":[3],"c":[5,5,5]}'),
            (k2, '$.a.*', '1',  b'[]',
             b'{"a":{},"b":{"a":1},"c":{"a":1,"b":2,"c":3}}'),
            (k2, '$.b.*', '2',
             b'[2]',     b'{"a":{},"b":{"a":2},"c":{"a":1,"b":2,"c":3}}'),
            (k2, '$.c.*', '4',
             b'[4,4,4]', b'{"a":{},"b":{"a":2},"c":{"a":4,"b":4,"c":4}}'),
            (k2, '.a.*', '1',   None,       None),
            (k2, '.b.*', '3',   b'3',
             b'{"a":{},"b":{"a":3},"c":{"a":4,"b":4,"c":4}}'),
            (k2, '.c.*', '5',   b'5',
             b'{"a":{},"b":{"a":3},"c":{"a":5,"b":5,"c":5}}')
        ]

        for (key, path, new_val, exp, exp2) in test_cases:
            if exp == None:
                with pytest.raises(ResponseError) as e:
                    client.execute_command(
                        'JSON.SET', key, path, new_val)
                    client.execute_command(
                        'JSON.GET', key, path)
                assert self.error_class.is_nonexistent_error(str(e.value))
            else:
                client.execute_command(
                    'JSON.SET', key, path, new_val)
                assert exp == client.execute_command(
                    'JSON.GET', key, path)
                # verify entire key
                assert exp2 == client.execute_command(
                    'JSON.GET', key, '.')

    def test_json_get_command_supports_all_datatypes(self):
        client = self.server.get_new_client()
        for (path, value) in [('.firstName', '"John"'),         # string
                              ('.address.city', '"New York"'),  # string
                              ('.spouse', 'null'),              # null
                              ('.children', '[]'),              # empy array
                              ('.groups', '{}'),                # empy object
                              ('.isAlive', 'true'),             # boolean
                              ('.age', '27')]:           # float number
            assert value.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

        for (path, value) in [('["weight"]', '135.17')]:  # float number
            assert value == client.execute_command(
                'JSON.GET', wikipedia, path).decode()

    def test_json_path_syntax_objectkeys(self):
        client = self.server.get_new_client()
        for (path, value) in [('["firstName"]', '"John"'),
                              ('address[\'city\']', '"New York"'),
                              ('[\'address\'][\'city\']', '"New York"'),
                              ('["address"]["city"]', '"New York"'),
                              ('["address"][\'city\']', '"New York"'),
                              ('["isAlive"]', 'true'),
                              ('[\'age\']', '27')]:
            assert value.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

        for (path, value) in [('["weight"]', '135.17')]:
            assert value == client.execute_command(
                'JSON.GET', wikipedia, path).decode()

        test_cases = [
            '[firstName"]',
            'address["city\'',
            '["address\'][[[["city"]',
            '[[["address"]]]["city"]',
            '"["address"][\'city\']',
            '"[\'address"]["city"]',
            '[""""address]["city"]',
            '[address""""]',
            '[\'address\']]][\'city\']',
            '["address"]\'[\'"city"]',
        ]

        # invalid json path
        for path in test_cases:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.GET', wikipedia, path)
            assert self.error_class.is_syntax_error(str(e.value))

    def test_json_get_command_floating_point(self):
        '''
        Test special cases of floating point values.
        '''
        client = self.server.get_new_client()
        for value in ['0', '0.1', '0.3', '1.23456789', '-0.1', '-0.3', '-1.23456789']:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', value)
            assert value == client.execute_command(
                'JSON.GET', wikipedia, '.foo').decode()

        # max double and min double: floating points will be returned exactly as is
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '1.7976e+308')
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.bar', '-1.7976e+308')

        assert '1.7976e+308' == client.execute_command(
            'JSON.GET', wikipedia, '.foo').decode()
        assert '-1.7976e+308' == client.execute_command(
            'JSON.GET', wikipedia, '.bar').decode()

        # 1.234567890123456789 exceeds the precision of a double but will persist regardless.
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '1.234567890123456789')

        assert '1.234567890123456789' == client.execute_command(
            'JSON.GET', wikipedia, '.foo').decode()

        # trailing zeros will no longer be removed.
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '0.3000000')

        assert '0.3000000' == client.execute_command(
            'JSON.GET', wikipedia, '.foo').decode()

    def test_json_get_command_with_multiple_paths(self):
        client = self.server.get_new_client()
        assert b'{".firstName":"John",".lastName":"Smith"}' == client.execute_command(
            'JSON.GET', wikipedia, '.firstName', '.lastName')

        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.GET', wikipedia, '.firstName', '.lastName', '.foo', '.bar')
        assert self.error_class.is_nonexistent_error(str(e.value))

    def test_json_get_command_returns_json_with_default_format(self):
        client = self.server.get_new_client()
        exp = self.data_wikipedia_compact

        # default format is compact JSON string - no indent, no space and no newline
        assert exp == client.execute_command(
            'JSON.GET', wikipedia).decode('utf-8')

        # test NOESCAPE: verify that NOESCAPE is ignored. See the API doc.
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, 'NOESCAPE').decode('utf-8')

    def test_json_get_command_returns_json_with_custom_format(self):
        client = self.server.get_new_client()
        exp = self.data_wikipedia

        # get the root document with custom indent/space/newline
        ret = client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')

        # test NOESCAPE: verify that NOESCAPE is ignored. See the API doc.
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n', 'NOESCAPE').decode('utf-8')
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, 'NOESCAPE', 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', 'NOESCAPE', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')

        # get a sub-document with custom indent/space/newline
        exp_json = '{\n\t"street": "21 2nd Street",\n\t"city": "New York",\n\t"state": "NY",\n\t"zipcode": "10021-3100"\n}'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '\t', 'SPACE', ' ', 'NEWLINE', '\n', '.address').decode('utf-8')

        # INDENT: =*=*, SPACE: --, NEWLINE: \r\n
        exp_json = '{\r\n=*=*"street":--"21 2nd Street",\r\n=*=*"city":--"New York",\r\n=*=*"state":--"NY",\r\n=*=*"zipcode":--"10021-3100"\r\n}'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '=*=*', 'SPACE', '--', 'NEWLINE', '\r\n', '.address').decode('utf-8')

        # verify that path args do not need to be positioned at the end
        assert exp == client.execute_command(
            'JSON.GET', wikipedia, '.', 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')
        exp_json = '"John"'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', '.firstName', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')

        exp_json = '{\n  ".firstName": "John",\n  ".lastName": "Smith"\n}'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE', ' ', '.firstName', '.lastName', 'NEWLINE', '\n').decode('utf-8')
        exp_json = '{\n\t"street": "21 2nd Street",\n\t"city": "New York",\n\t"state": "NY",\n\t"zipcode": "10021-3100"\n}'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '\t', '.address', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')
        exp_json = '[\n\t{\n\t\t"street": "21 2nd Street",\n\t\t"city": "New York",\n\t\t"state": "NY",\n\t\t"zipcode": "10021-3100"\n\t}\n]'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '\t', '$.address', 'SPACE', ' ', 'NEWLINE', '\n').decode('utf-8')

        # check that path args can have formatting in between them
        exp_json = '{\n  ".firstName": "John",\n  ".lastName": "Smith"\n}'
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, 'INDENT', '  ', '.firstName', 'SPACE', ' ', '.lastName', 'NEWLINE', '\n').decode('utf-8')
        assert exp_json == client.execute_command(
            'JSON.GET', wikipedia, '.firstName', 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE', '\n', '.lastName').decode('utf-8')

    def test_json_get_command_returns_json_with_custom_format_error_conditions(self):
        client = self.server.get_new_client()
        # NEWLINE is the last arg
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE', ' ', 'NEWLINE')
        assert self.error_class.is_syntax_error(str(e.value))

        # SPACE is the last arg
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '  ', 'SPACE')
        assert self.error_class.is_syntax_error(str(e.value))

        # INDENT is the last arg
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.GET', wikipedia, 'NEWLINE', '\n', 'SPACE', ' ', 'INDENT')
        assert self.error_class.is_syntax_error(str(e.value))

    def test_json_get_command_with_error_conditions(self):
        client = self.server.get_new_client()
        # if the document key does not exist, the command should return null without throwing an error.
        assert None == client.execute_command(
            'JSON.GET', foo, '.firstName')

        # if the key is not a document key, the command should throw an error.
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.GET', str_key)
        assert self.error_class.is_wrongtype_error(str(e.value))

        # if the JSON path does not exist, the command should throw an error.
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.GET', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command('JSON.GET')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_number_as_member_name(self):
        '''
        Given a JSON that has numbers as member names, test GET and SET.
        '''
        client = self.server.get_new_client()
        client.execute_command(
            'JSON.SET', k1, '.', '{"1":1, "2":{"3":4}}')
        client.execute_command('COPY', k1, k2)
        client.execute_command('COPY', k1, k3)
        client.execute_command('COPY', k1, k4)
        client.execute_command('COPY', k1, k5)

        # test GET
        for (path, exp) in [
            ('["2"]["3"]',      '4'),
            ('[\'2\'][\'3\']',  '4'),
            ('.1',              '1'),
            ('.2.3',            '4'),
            ('$.2.3',           '[4]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', k1, path).decode()

        # test SET
        for (key, path, val, exp_new_val) in [
            (k1, '["2"]["3"]',      '5', '{"1":1,"2":{"3":5}}'),
            (k2, '[\'2\'][\'3\']',  '5', '{"1":1,"2":{"3":5}}'),
            (k3, '.2.3',            '5', '{"1":1,"2":{"3":5}}'),
            (k4, '.1',              '2', '{"1":2,"2":{"3":4}}'),
            (k5, '$.2.*',           '5', '{"1":1,"2":{"3":5}}')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, path, val)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

    def test_json_get_legacy_and_v2path_wildcard(self):
        '''
        Test two versions of path syntax - V2 JSONPath and the legacy path. A V2 JSONPath must starts with the dollar sign
        that represents the root element. If a path does not start with the dollar sign, it's a legacy path.

        For queries, the legacy path always returns a single value, which is the first value if multiple values are
        selected. If no value is selected, the legacy path returns NONEXISTENT error. The JSONPath always returns an
        array of values, which could contain zero or one or more values.
        '''
        client = self.server.get_new_client()
        test_cases = [
            ('$.address.*',
             b'["21 2nd Street","New York","NY","10021-3100"]'),
            ('$.[\'address\'].*',
             b'["21 2nd Street","New York","NY","10021-3100"]'),
            ('.address.*',                      b'"21 2nd Street"'),
            ('.["address"].*',                  b'"21 2nd Street"'),
            ('$.phoneNumbers.*.type',           b'["home","office"]'),
            ('$.phoneNumbers[*].type',          b'["home","office"]'),
            ('$.["phoneNumbers"][*].["type"]',  b'["home","office"]'),
            ('.phoneNumbers.*.type',            b'"home"'),
            ('.phoneNumbers[*].type',           b'"home"'),
            ('.["phoneNumbers"][*].["type"]',   b'"home"'),
            ('$.[ \'address\' ].*',
             b'["21 2nd Street","New York","NY","10021-3100"]'),
            ('.[ "address" ].*',                b'"21 2nd Street"'),
            ('$.[ "phoneNumbers" ][ * ].[ "type" ]',  b'["home","office"]'),
        ]

        for (path, exp) in test_cases:
            assert exp == client.execute_command(
                'JSON.GET', wikipedia, path)

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b": {"a": 1}, "c": {"a": 1, "b": 2}}')
        client.execute_command(
            'JSON.SET', k3, '.', '{"a":[[[1,2],[3,4],[5,6]],[[7,8],[9,10],[11,12]]]}')
        client.execute_command(
            'JSON.SET', k4, '.', '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h:":1}}}}}}}}')

        # JSONPath always returns an array of values
        # Test multiple wildcards
        for (key, path, exp) in [
            (k1, '$.a[*]', b'[]'),
            (k1, '$.b[*]', b'[1]'),
            (k1, '$.c[*]', b'[1,2]'),
            (k2, '$.a.*',  b'[]'),
            (k2, '$.b.*',  b'[1]'),
            (k2, '$.c.*',  b'[1,2]'),
            (k3, '$.a[*]',  b'[[[1,2],[3,4],[5,6]],[[7,8],[9,10],[11,12]]]'),
            (k3, '$.a[*][*]',  b'[[1,2],[3,4],[5,6],[7,8],[9,10],[11,12]]'),
            (k3, '$.a[*][*][1]',  b'[2,4,6,8,10,12]'),
            (k4, '$.a.*.*',  b'[{"d":{"e":{"f":{"g":{"h:":1}}}}}]'),
            (k4, '$.a.*.*.*',  b'[{"e":{"f":{"g":{"h:":1}}}}]'),
            (k4, '$.a.*.c.*',  b'[{"e":{"f":{"g":{"h:":1}}}}]'),
            (k4, '$.a.*.c.*.e.*',  b'[{"g":{"h:":1}}]'),
            (k4, '$.a.*.c.*.e.*.g.*',  b'[1]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        # Legacy path always returns a single value, which is the first value.
        # Test multiple wildcards
        for (key, path, exp) in [
            (k1, '.b[*]', b'1'),
            (k1, '.c[*]', b'1'),
            (k2, '.b.*',  b'1'),
            (k2, '.c.*',  b'1'),
            (k3, '.a[*]',  b'[[1,2],[3,4],[5,6]]'),
            (k3, '.a[*][*]',  b'[1,2]'),
            (k3, '.a[*][*][1]',  b'2'),
            (k4, '.a.*.*',  b'{"d":{"e":{"f":{"g":{"h:":1}}}}}'),
            (k4, '.a.*.*.*',  b'{"e":{"f":{"g":{"h:":1}}}}'),
            (k4, '.a.*.c.*',  b'{"e":{"f":{"g":{"h:":1}}}}'),
            (k4, '.a.*.c.*.e.*',  b'{"g":{"h:":1}}'),
            (k4, '.a.*.c.*.e.*.g.*',  b'1')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        # Legacy path returns non-existent error if no value is selected.
        for (key, path, exp) in [
            (k1, '.a[*]', None),
            (k2, '.a.*', None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    'JSON.GET', key, path)
            assert self.error_class.is_nonexistent_error(str(e.value))

    def test_json_get_negative_array_index_legacypath(self):
        '''
        Test negative array index with the legacy path syntax.
        '''
        client = self.server.get_new_client()
        # Negative indices do not throw error in Valkey
        test_cases = [
            ('.phoneNumbers[-2].type',            '"home"'),
            ('.phoneNumbers[-1].type',            '"office"'),
            ('.phoneNumbers[ -1 ].type',          '"office"'),
            ('.["phoneNumbers"][-2]["type"]',     '"home"'),
            ('.["phoneNumbers"][-1]["type"]',     '"office"'),
            ('.["phoneNumbers"][ -1]["type" ]',   '"office"'),
            ('.["phoneNumbers"][-1 ][ "type"]',   '"office"'),
            ('.["phoneNumbers"][ -1 ][ "type" ]', '"office"')
        ]

        # Out of boundary test cases
        oob_test_cases = [
            '.phoneNumbers[2].type',
            '.phoneNumbers[10].type',
            '.phoneNumbers[-3].type',
        ]

        # Legacy path always returns a single value
        for (path, exp) in test_cases:
            assert exp.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

        # index out of bounds
        for path in oob_test_cases:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.GET', wikipedia, path)
            assert self.error_class.is_outofboundaries_error(str(e.value))

        # test using negative index on a non-array value
        for path in [
            '.firstName[-1]',
            '.age[-1]',
            '.weight[-1]',
            '.address[-1]',
            '.phoneNumbers[0][-1]'
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.GET', wikipedia, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_get_negative_array_index_v2path(self):
        '''
        Test negative array index with the V2 JSONPath syntax.
        '''
        client = self.server.get_new_client()
        test_cases = [
            ('$.phoneNumbers[-2].type',         '["home"]'),
            ('$.phoneNumbers[ -2 ].type',       '["home"]'),
            ('$.phoneNumbers[-1].type',         '["office"]'),
            ('$.phoneNumbers[ -1].type',        '["office"]'),
            ('$.phoneNumbers[-1 ].type',        '["office"]'),
            ('$.["phoneNumbers"][-2]["type"]',  '["home"]'),
            ('$.["phoneNumbers"][-1]["type"]',  '["office"]'),
            # index out of bounds
            ('$.phoneNumbers[2].type',          '[]'),
            ('$.phoneNumbers[10].type',         '[]'),
            # using negative index on a non-array value
            ('$.firstName[-1]',                 '[]'),
            ('$.age[-1]',                       '[]'),
            ('$.weight[-1]',                    '[]'),
            ('$.weight[    -1 ]',               '[]'),
            ('$.phoneNumbers[0][-1]',           '[]'),
            ('$.phoneNumbers[ 0][ -1 ]',        '[]'),
            ('$.phoneNumbers[ 0 ][ -1 ]',       '[]'),
            ('$.[ "phoneNumbers" ][ -1 ][ "type" ]',  '["office"]'),
            ('$.phoneNumbers[-3].type',         '[]'),
            ('$.phoneNumbers[ -3 ].type',       '[]'),
        ]

        # JSONPath always returns an array of values
        for (path, exp) in test_cases:
            assert exp.encode() == client.execute_command(
                'JSON.GET', wikipedia, path)

    def test_json_get_v2path_array_slice(self):
        '''
        Test negative array slice with the V2 JSONPath syntax.
        '''
        client = self.server.get_new_client()

        test_cases = [
            ('$[0:3]',         '[0,1,2]'),
            ('$[ 0 : 3 ]',     '[0,1,2]'),
            ('$[0:+3]',        '[0,1,2]'),
            ('$[ 0 : +3 ]',    '[0,1,2]'),
            ('$[0:-1]',        '[0,1,2,3,4,5,6,7,8]'),
            ('$[ 0 : -1 ]',    '[0,1,2,3,4,5,6,7,8]'),
            ('$[2:-2]',        '[2,3,4,5,6,7]'),
            ('$[ 2 : -2 ]',    '[2,3,4,5,6,7]'),
            ('$[+2:-2]',       '[2,3,4,5,6,7]'),
            ('$[1:1]',         '[]'),
            ('$[1:2]',         '[1]'),
            ('$[+1:+2]',       '[1]'),
            ('$[1:3]',         '[1,2]'),
            ('$[1:0]',         '[]'),
            ('$[5:]',          '[5,6,7,8,9]'),
            ('$[ 5 :  ]',      '[5,6,7,8,9]'),
            ('$[:3]',          '[0,1,2]'),
            ('$[  :  3]',      '[0,1,2]'),
            ('$[:+3]',         '[0,1,2]'),
            ('$[: +3]',        '[0,1,2]'),
            ('$[:6:2]',        '[0,2,4]'),
            ('$[ : 6 : 2]',    '[0,2,4]'),
            ('$[:]',           '[0,1,2,3,4,5,6,7,8,9]'),
            ('$[ : ]',         '[0,1,2,3,4,5,6,7,8,9]'),
            ('$[::]',          '[0,1,2,3,4,5,6,7,8,9]'),
            ('$[ : : ]',       '[0,1,2,3,4,5,6,7,8,9]'),
            ('$[::2]',         '[0,2,4,6,8]'),
            ('$[ : : 2 ]',     '[0,2,4,6,8]'),
            ('$[3::2]',        '[3,5,7,9]'),
            ('$[3 :: 2]',      '[3,5,7,9]'),
            ('$[0::1]',        '[0,1,2,3,4,5,6,7,8,9]'),
            ('$[0:8:2]',       '[0,2,4,6]'),
            ('$[ 0 : 8 : 2 ]', '[0,2,4,6]'),
            ('$[0:+8:+2]',     '[0,2,4,6]'),
            ('$[0 : +8 : +2]', '[0,2,4,6]'),
            ('$[6:0:-1]',      '[6,5,4,3,2,1]'),
            ('$[6::-1]',       '[]'),
            ('$[6:0:-2]',      '[6,4,2]'),
            ('$[6::-2]',       '[]'),
            ('$[8:0:-2]',      '[8,6,4,2]')
        ]

        client.execute_command(
            'JSON.SET', k1, '.', '[0,1,2,3,4,5,6,7,8,9]')
        for (path, exp) in test_cases:
            assert exp.encode() == client.execute_command('JSON.GET', k1, path)

    def test_json_get_v2path_array_union(self):
        '''
        Test array union with the V2 JSONPath syntax.
        '''
        client = self.server.get_new_client()

        test_cases = [
            ('$[0,1,2]',                             '[0,1,2]'),
            ('$[0, 1, 2]',                           '[0,1,2]'),
            ('$[0, 1, 2  ]',                         '[0,1,2]'),
            ('$[ 0, 1, 2 ]',                         '[0,1,2]'),
            ('$[ 0,1, 2 ]',                          '[0,1,2]'),
            ('$[0,1]',                               '[0,1]'),
            ('$[-1,-2]',                             '[9,8]'),
            ('$[-10,-5,-6]',                         '[0,5,4]'),
            ('$[0,1,5,0,1,2]',                       '[0,1,5,0,1,2]'),
            ('$[0, 1,5,0, 1,2]',                     '[0,1,5,0,1,2]'),
            ('$[ 0, 1, 5, 0, 1, 2 ]',                '[0,1,5,0,1,2]'),
            ('$[ -10 , -5 , -6 ]',                   '[0,5,4]'),
            ('$[-10,-9,-8,0,1,2,-1000,1000]',        '[0,1,2,0,1,2]'),
        ]

        client.execute_command(
            'JSON.SET', k1, '.', '[0,1,2,3,4,5,6,7,8,9]')
        for (path, exp) in test_cases:
            assert exp.encode() == client.execute_command('JSON.GET', k1, path)

        client.execute_command(
            'JSON.SET', k2, '.', '[{"name":"name0","id":0},{"name":"name1","id":1},{"name":"name2","id":2}]')
        for (path, exp) in [
            ('$[0,2].name',         '["name0","name2"]'),
        ]:
            assert exp.encode() == client.execute_command('JSON.GET', k2, path)

        # we do not support mixing of  unions and slices, nor do we support extraneous commas
        for path in [
            '$[0,1,2:4]',
            '$[0:2,3,4]',
            '$[0,,4]',
            '$[0,,,4]',
            '$[,4]',
            '$[4,]',
            '$[,4,]',
            '$[,,4,,]',
            '$[,]',
            '$[,0,4]',
            '$[,0,4,]',
            '$[0,4,]',
        ]:
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    'JSON.GET', k1, path)
            assert self.error_class.is_syntax_error(str(e.value))

    def test_json_get_multipaths_legacy_and_v2path_wildcard(self):
        '''
        Test JSON.GET with multiple paths, legacy path or v2 JSONPath or mixed.
        '''
        client = self.server.get_new_client()

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b": {"a": 1}, "c": {"a": 1, "b": 2}}')

        test_cases = [
            (k1, '.b[*]', '.c[*]', '{".b[*]":1,".c[*]":1}'),
            (k2, '.b.*', '.c.*',   '{".b.*":1,".c.*":1}'),
        ]

        # if all paths are legacy path, the result conforms to the legacy path version
        for (key, path1, path2, exp) in test_cases:
            # 1st path returns 1 value. 2nd path returns the first one of 2 values.
            assert exp.encode() == client.execute_command(
                'JSON.GET', key, path1, path2)

        # all paths are legacy path, the result conforms to the legacy path version.
        # If one path returns 0 value, the command should fail with NONEXISTENT error.
        for (key, path1, path2, exp) in [
            (k1, '.a[*]',   '.b[*]', None),
            (k2, '.a.*',    '.b.*',  None),
            (k2, '.foo.*',  '.c.*',  None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    'JSON.GET', key, path1, path2)
            assert self.error_class.is_nonexistent_error(str(e.value))

        # if at least one path is JSONPath, the result conforms to the JSONPath version
        for (key, path1, path2, path3, exp) in [
            (k1, '$.a[*]', '$.b[*]', '$.c[*]',
             '{"$.a[*]":[],"$.b[*]":[1],"$.c[*]":[1,2]}'),
            (k1, '$.a[*]', '.b[*]',  '.c[*]',
             '{"$.a[*]":[],".b[*]":[1],".c[*]":[1,2]}'),
            (k1, '.a[*]',  '$.b[*]', '.c[*]',
             '{".a[*]":[],"$.b[*]":[1],".c[*]":[1,2]}'),
            (k2, '.a.*',   '.b.*',   '$.c.*',
             '{".a.*":[],".b.*":[1],"$.c.*":[1,2]}'),
            (k2, '$.a.*',  '$.b.*',  '.c.*',
             '{"$.a.*":[],"$.b.*":[1],".c.*":[1,2]}'),
            (k2, '.a.*',   '$.b.*',  '$.c.*',
             '{".a.*":[],"$.b.*":[1],"$.c.*":[1,2]}'),
            # 1st path returns 0 value. 2nd path returns 1 value. 3rd path returns 2 values.
            (k2, '.foo.*', '$.b.*',  '$.c.*',
             '{".foo.*":[],"$.b.*":[1],"$.c.*":[1,2]}')
        ]:
            assert exp.encode() == client.execute_command(
                'JSON.GET', key, path1, path2, path3)

    def test_json_mget_command(self):
        client = self.server.get_new_client()
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', '{"foo":"bar1"}')
        assert b'OK' == client.execute_command(
            'JSON.SET', k2, '.', '{"foo":"bar2"}')
        assert b'OK' == client.execute_command(
            'JSON.SET', k3, '.', '{"foo":"bar3"}')
        assert [b'"bar1"', b'"bar2"', b'"bar3"'] == client.execute_command(
            'JSON.MGET', k1, k2, k3, '.foo')
        # test the condition of JSON path does not exist
        assert [None, None, None] == client.execute_command(
            'JSON.MGET', k1, k2, k3, '.bar')
        # test the condition of key does not exist
        assert [None, None] == client.execute_command(
            'JSON.MGET', baz, foo, '.')
        assert [None, b'"bar2"'] == client.execute_command(
            'JSON.MGET', baz, k2, '.foo')

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command('JSON.MGET')
        assert str(e.value).find('wrong number of arguments') >= 0

        assert b'OK' == client.execute_command(
            'json.set', k4, '.', '[2,5,{"level0":[null,true,{"level0_1":[3,false]}],"level1":{"level1_0":33}}]')
        assert b'OK' == client.execute_command(
            'json.set', k5, '.', '[4,5,{"level0":[null,false,{"level0_1":[null,false]}],"level1":{"level1_0":[12,13]}}]')
        assert b'OK' == client.execute_command(
            'json.set', k6, '.', '[2,5,{"level0":[true,20,{"level0_1":[3,true]}],"level1":{"level1_0":33}}]')
        for (path, exp) in [
            ('$..level0_1', [b"[[3,false]]",
             b"[[null,false]]", b"[[3,true]]"]),
            ('$.[2].level1', [b'[{"level1_0":33}]',
             b'[{"level1_0":[12,13]}]', b'[{"level1_0":33}]']),
            ('$.[2].level1.level1_0[2]', [b"[]", b"[]", b"[]"]),
            ('$.[2].level1.level1_0[1]', [b"[]", b"[13]", b"[]"])
        ]:
            assert [exp[0], exp[1], exp[2]] == client.execute_command(
                'JSON.MGET', k4, k5, k6, path)

    def test_json_key_declaration(self):
        client = self.server.get_new_client()
        cmd_need_val = set(
            'SET NUMMULTBY NUMINCRBY ARRAPPEND ARRINDEX STRAPPEND RESP'.split())

        # These commands should only get the single key
        for cmd in ('DEL', 'GET', 'SET', 'TYPE', 'NUMINCRBY', 'NUMMULTBY', 'TOGGLE', 'STRAPPEND', 'STRLEN',
                    'ARRAPPEND', 'ARRINDEX', 'ARRLEN', 'ARRPOP', 'CLEAR', 'OBJKEYS',
                    'OBJLEN', 'FORGET', 'RESP'):
            if cmd not in cmd_need_val:
                assert [k1] == client.execute_command(
                    'COMMAND GETKEYS', f'JSON.{cmd}', k1)
            else:
                # Dummy value in command
                assert [k1] == client.execute_command(
                    'COMMAND GETKEYS', f'JSON.{cmd}', k1, '.', '5')

            # ARRINSERT requires index
            assert ['k1'] == client.execute_command(
                'COMMAND GETKEYS', 'JSON.ARRINSERT', 'k1', '.', 0, '5')
            # ARRINSERT requires start end
            assert ['k1'] == client.execute_command(
                'COMMAND GETKEYS', 'JSON.ARRTRIM', 'k1', '.', 0, 5)

        debug_subcmd = set('MEMORY DEPTH'.split())
        for cmd in debug_subcmd:
            assert [k1] == client.execute_command(
                'COMMAND GETKEYS', 'JSON.DEBUG', cmd, k1)

        # JSON.MGET is the only multi-key command, so make sure it returns the right set of keys
        assert [k1, k2, k3] == client.execute_command(
            'COMMAND GETKEYS', 'JSON.MGET', k1, k2, k3, '.')

    def __json_del_or_forget__(self, cmd):
        client = self.server.get_new_client()
        # delete an element
        for path in ['.spouse', '.phoneNumbers']:
            assert 1 == client.execute_command(
                cmd, wikipedia, path)
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    'JSON.GET', wikipedia, path)
            assert self.error_class.is_nonexistent_error(str(e.value))

        # delete a doc: path not provided
        assert 1 == client.execute_command(cmd, wikipedia)
        assert 0 == client.execute_command('EXISTS', wikipedia)

        # delete a doc: path arg provided
        client.execute_command('json.set', k1, '.', '1')
        assert 1 == client.execute_command(cmd, k1, '.')
        assert 0 == client.execute_command('EXISTS', k1)

        # return should be 0 if the document key does not exist
        assert 0 == client.execute_command(
            cmd, foo, '.firstName')

        # return should be 0 if the path does not exist
        assert 0 == client.execute_command(
            cmd, wikipedia, '.foo')

        # Wrong number of arguments

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                cmd, wikipedia, '.children', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_del_command(self):
        self.__json_del_or_forget__('JSON.DEL')

    def test_json_forget_command(self):
        self.__json_del_or_forget__('JSON.FORGET')

    def test_json_del_command_v2path_wildcard(self):
        client = self.server.get_new_client()
        client.execute_command(
            'json.set', k1, '.', '{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}')
        client.execute_command(
            'json.set', k2, '.', '[0,1,2,3,4,5,6,7,8,9]')
        client.execute_command(
            'json.set', k3, '.', '[0,1,2,3,4,5,6,7,8,9]')
        client.execute_command(
            'json.set', k4, '.', '[0,1,2,3,4,5,6,7,8,9]')
        client.execute_command(
            'json.set', k5, '.', '[0,1,2,3,4,5,6,7,8,9]')

        # NOTE: The expected values below account for the outcome of previous commands.
        for (key, path, exp_ret, exp_val) in [
            (k1, '$.z.*',   2,  '{"x":{},"y":{"a":"a"},"z":{}}'),
            (k1, '$.*',     3,  '{}'),
            (k2, '$.[3:6]', 3,  '[0,1,2,6,7,8,9]'),
            (k2, '$.*',     7,  '[]'),
        ]:
            assert exp_ret == client.execute_command(
                'JSON.DEL', key, path)
            assert exp_val == client.execute_command(
                'JSON.GET', key).decode()

        # delete whole doc
        for key in [k1, k2]:
            assert 1 == client.execute_command(
                'JSON.DEL', key, '$')
            assert 0 == client.execute_command('EXISTS', key)

        # delete with wildcard, slice, and union
        assert 10 == client.execute_command(
            'JSON.DEL', k3, '$[*]')
        assert b'[]' == client.execute_command('JSON.GET', k3)
        assert 4 == client.execute_command(
            'JSON.DEL', k4, '$[3:7]')
        assert b'[0,1,2,7,8,9]' == client.execute_command(
            'JSON.GET', k4)
        assert 4 == client.execute_command(
            'JSON.DEL', k5, '$[1,5,7,8]')
        assert b'[0,2,3,4,6,9]' == client.execute_command(
            'JSON.GET', k5)

        assert b'OK' == client.execute_command(
            'json.set', k1, '.', '[2,5,{"level0":[null,true,{"level0_1":[3,false]}],"level1":{"level1_0":33}}]')
        for (key, path, exp_ret, exp_val) in [
                (k1, '$[2].level0..level0_1[1]',   1,
                 '[2,5,{"level0":[null,true,{"level0_1":[3]}],"level1":{"level1_0":33}}]'),
                (k1, '$[2].level0..level0_1',      1,
                 '[2,5,{"level0":[null,true,{}],"level1":{"level1_0":33}}]'),
                (k1, '$..level1.level1_0',         1,
                 '[2,5,{"level0":[null,true,{}],"level1":{}}]'),
                (k1, '$..level1',                  1,
                 '[2,5,{"level0":[null,true,{}]}]'),
                (k1, '.*',                         3,  '[]'),
        ]:
            assert exp_ret == client.execute_command(
                'JSON.DEL', key, path)
            assert exp_val == client.execute_command(
                'JSON.GET', key).decode()
            if path != '.*':
                assert '[]' == client.execute_command(
                    'JSON.GET', key, path).decode()
            else:
                with pytest.raises(ResponseError) as e:
                    client.execute_command(
                        'JSON.GET', key, path).decode()
                assert self.error_class.is_nonexistent_error(str(e.value))

    def test_json_unicode_is_supported(self):
        client = self.server.get_new_client()
        for unicode_str in [
            '"Eat, drink, 愛"',
            '"hyvää-élève"'
        ]:
            utf8 = unicode_str.encode('utf-8')
            assert b'OK' == client.execute_command(
                'JSON.SET', k1, '.', unicode_str)
            assert utf8 == client.execute_command(
                'JSON.GET', k1, 'NOESCAPE', '.')
            assert 1 == client.execute_command('JSON.DEL', k1)
            assert 0 == client.execute_command('EXISTS', k1)

    def test_nonASCII_in_jsonpath(self):
        client = self.server.get_new_client()
        client.execute_command(
            'JSON.SET', k1, '.', '{"a": {"愛": "love", "b": "b"}}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"愛": [1,2,3]}')
        for (key, path1, path2, exp) in [
            (k1, '.a.愛', None,   b'"love"'),
            (k1, '.a.愛', '.a.b', b'{".a.\xe6\x84\x9b":"love",".a.b":"b"}'),
            (k2, '.愛[1]', None,  b'2'),
            (k2, '$.愛[*]', None,  b'[1,2,3]')
        ]:
            if path2 is None:
                assert exp == client.execute_command(
                    'JSON.GET', key, path1)
            else:
                # Valkey behavior is weird and returns dictionaries in a non-deterministic order
                assert exp == client.execute_command(
                    'JSON.GET', key, path1, path2)

    def test_json_number_scanner(self):
        '''Test that numeric conversion gets the right types around various edge cases'''
        client = self.server.get_new_client()
        maxpos = (1 << 63) - 1

        for v in [
            (maxpos, b'integer'),
            (-maxpos, b'integer'),
                (-maxpos-1, b'integer')]:
            client.execute_command(
                'JSON.SET', k1, '.', str(v[0]))
            assert v[1] == client.execute_command(
                'JSON.TYPE', k1, '.'), "Value is " + str(v[0])

        for v in [
            (maxpos+1, b'number'),
            (-maxpos-2, b'number')
        ]:

            client.execute_command(
                'JSON.SET', k1, '.', str(v[0]))
            assert v[1] == client.execute_command(
                'JSON.TYPE', k1, '.'), "Value is " + str(v[0])

    def test_json_toggle(self):
        client = self.server.get_new_client()
        # Toggle back and forth
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foobool', 'false')
        assert b'false' == client.execute_command(
            'JSON.GET', wikipedia, '.foobool')

        assert b'true' == client.execute_command(
            'JSON.TOGGLE', wikipedia, '.foobool')
        assert b'true' == client.execute_command(
            'JSON.GET', wikipedia, '.foobool')

        assert b'false' == client.execute_command(
            'JSON.TOGGLE', wikipedia, '.foobool')
        assert b'false' == client.execute_command(
            'JSON.GET', wikipedia, '.foobool')

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.TOGGLE', wikipedia, '.foobool', 'extra')
        assert self.error_class.is_wrong_number_of_arguments_error(
            str(e.value))

        # Wrong types
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foonum', '55')
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.TOGGLE', wikipedia, '.foonum')
        assert self.error_class.is_wrongtype_error(str(e.value))

        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foostr', '"ok"')
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.TOGGLE', wikipedia, '.foostr')
        assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_toggle_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '{"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[true, false, 1, null, "foo", [], {}]')

        for (key, path, exp, exp_new_val) in [
            (k1, '$.*',  [0, 1, None, None, None, None, None],
             '{"a":false,"b":true,"c":1,"d":null,"e":"foo","f":[],"g":{}}'),
            (k1, '$.*',  [1, 0, None, None, None, None, None],
             '{"a":true,"b":false,"c":1,"d":null,"e":"foo","f":[],"g":{}}'),
            (k2, '$[*]', [0, 1, None, None, None, None, None],
             '[false,true,1,null,"foo",[],{}]'),
            (k2, '$[*]', [1, 0, None, None, None, None, None],
             '[true,false,1,null,"foo",[],{}]')
        ]:
            assert exp == client.execute_command(
                'JSON.TOGGLE', key, path)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

    def test_json_numincrby(self):
        client = self.server.get_new_client()
        assert b'28' == client.execute_command(
            'JSON.NUMINCRBY', wikipedia, '.age', '1')
        assert b'38' == client.execute_command(
            'JSON.NUMINCRBY', wikipedia, '.age', '10')
        assert b'33' == client.execute_command(
            'JSON.NUMINCRBY', wikipedia, '.age', '-5')

        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '1')
        assert b'1.5' == client.execute_command(
            'JSON.NUMINCRBY', wikipedia, '.foo', '0.5')
        assert b'2' == client.execute_command(
            'JSON.NUMINCRBY', wikipedia, '.foo', '0.5')

        # error condition: document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.NUMINCRBY', foo, '.age', '2')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.NUMINCRBY', wikipedia, '.bar', '2')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.NUMINCRBY', wikipedia, '.age', '1', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_nummultby(self):
        client = self.server.get_new_client()
        assert b'270' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.age', '10')
        assert b'2700' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.age', '10')
        assert b'27' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.age', '0.01')

        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '1')
        assert b'0.5' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.foo', '0.5')
        assert b'0.25' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.foo', '0.5')
        assert b'1' == client.execute_command(
            'JSON.NUMMULTBY', wikipedia, '.foo', '4')

        # error condition: document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.NUMMULTBY', foo, '.age', '2')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.NUMMULTBY', wikipedia, '.bar', '2')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.NUMMULTBY', wikipedia, '.age', '2', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_simple_operations_remove_double_styling(self):
        # Multiplying by one or adding zero will now remove styling
        # and really change output for negative exponents due to decimals
        client = self.server.get_new_client()
        data = [
            ('2.50000', '2.5'),
            ('2e30', '2e+30'),
            ('2E+30', '2e+30'),
            ('2E30', '2e+30'),
            ('2E-30', '2.0000000000000002e-30'),
            ('2e5', '200000'),
            ('-2.50000', '-2.5'),
            ('-2e30', '-2e+30'),
            ('-2E+30', '-2e+30'),
            ('-2E30', '-2e+30'),
            ('-2E-30', '-2.0000000000000002e-30'),
            ('-2e5', '-200000'),
        ]

        for (initial, unstyled) in data:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', initial)
            assert initial == client.execute_command(
                'JSON.GET', wikipedia, '.foo').decode()
            client.execute_command(
                'JSON.NUMMULTBY', wikipedia, '.foo', '1')
            assert unstyled == client.execute_command(
                'JSON.GET', wikipedia, '.foo').decode()

        for (initial, unstyled) in data:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', initial)
            assert initial == client.execute_command(
                'JSON.GET', wikipedia, '.foo').decode()
            client.execute_command(
                'JSON.NUMINCRBY', wikipedia, '.foo', '0')
            assert unstyled == client.execute_command(
                'JSON.GET', wikipedia, '.foo').decode()

    def test_json_double_operations_wrongtype(self):
        client = self.server.get_new_client()
        # None of these will these will succeed because the doubles are not valid or the field is not a double
        for (cmd, key, field, arg) in [
            ('JSON.NUMMULTBY', wikipedia, '.age', '"2.0"'),
            ('JSON.NUMMULTBY', wikipedia, '.age', '2.'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0.'),
            ('JSON.NUMMULTBY', wikipedia, '.age', '-2.'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '-2.0.'),
            ('JSON.NUMMULTBY', wikipedia, '.age', '+2.'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '+2.0.'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '.2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', 'a2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '-a2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '+a2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', 'e2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '-e2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '+e2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', 'e+2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '-e-2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '+E+2.0'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0e'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0eq'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0e3q'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0e+'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0e+a'),
            ('JSON.NUMINCRBY', wikipedia, '.age', '2.0e+41a'),
            ('JSON.NUMINCRBY', wikipedia, '.firstName', '2'),
        ]:
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    cmd, key, field, arg)
            assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_double_consistency(self):
        '''
        Test that double values remain consistent when going through JSON Engine.
            This tests a tolerance of 2^-50 for a decent number of iterations,
            but is not enough to guarantee that level of presicion to our customers.
        Also verify that regular and pretty print double values have the same output.
        '''
        client = self.server.get_new_client()
        # arbitrarily generated hex to double to string, to send to json
        random.seed(1234)
        data = []
        for i in range(24000):
            potential_double = struct.unpack(
                '<d', bytes.fromhex('%016x' % random.randrange(16**16)))[0]
            # We don't want inf/-inf, NaN, or denormalized numbers
            if not (isinf(potential_double) or isnan(potential_double) or frexp(potential_double)[1] == 0):
                data.append(str(potential_double))

        for val in data:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            v0 = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            vp = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            # verify pretty print content is the same as standard content
            assert v0 is not None and vp is not None and v0 == vp
            # verify content is accurate, worked up to 2^-50 when float was calculated, should work when stored as string
            assert v0.decode() == val

    def test_json_numincrby_large_numbers(self):
        '''
        Test edge cases with large values near the boundaries of int64 and double.
        '''
        # Although the result exceeds the range of int64, it is still a valid double number.
        client = self.server.get_new_client()
        for (val, incr) in [
            ('9223372036854775807', 1),
            ('9223372036854775807', 2),
            ('-9223372036854775808', -1),
            ('-9223372036854775808', -2),
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            assert val.encode() == client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            # verify pretty-print gives us the same result
            assert val.encode() == client.execute_command(
                'JSON.GET', wikipedia, 'SPACE', ' ', 'NEWLINE', '\n', '.foo')

        # The result is still within the range of int64
        data = [
            ('-9223372036854775808', '-9223372036854775808', 0,
             '-9.2233720368547758e+18', '-9223372036854775808'),
            ('-9223372036854775808', '-9223372036854775808', 1,
             '-9.2233720368547758e+18', '-9223372036854775807'),
            ('-9223372036854775808', '-9223372036854775808', 2,
             '-9.2233720368547758e+18', '-9223372036854775806'),
            ('1.79e+308', '1.79e308', 0, '1.79e+308', '1.79e308'),
            ('1.79e+308', '1.79e308', 1, '1.79e+308', '1.79e308'),
            ('1.79e+308', '1.79e308', -1, '1.79e+308', '1.79e308'),
            ('-1.79e+308', '-1.79e308', 0, '-1.79e+308', '-1.79e308'),
            ('-1.79e+308', '-1.79e308', 1, '-1.79e+308', '-1.79e308'),
            ('-1.79e+308', '-1.79e308', -1, '-1.79e+308', '-1.79e308'),
            ('9223372036854775807', '9223372036854775807', 0,
             '9.2233720368547758e+18', '9223372036854775807'),
            ('9223372036854775807', '9223372036854775807', -1,
             '9.2233720368547758e+18', '9223372036854775806'),
            ('9223372036854775807', '9223372036854775807', -2,
             '9.2233720368547758e+18', '9223372036854775805')
        ]
        iteration_tracker = 0
        for (val, val_alt, incr, exp, exp_alt) in data:
            logging.debug("1058: Iteration %d", iteration_tracker)
            iteration_tracker += 1
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            v = client.execute_command(
                'JSON.NUMINCRBY', wikipedia, '.foo', incr)
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt

        # The result is still within the range of double
        for (val, val_alt, incr, exp, exp_alt) in [
            ('1.79e+308', '1.79e308', 0, '1.79e+308', '1.79e308'),
            ('1.79e+308', '1.79e308', 1, '1.79e+308', '1.79e308'),
            ('1.79e+308', '1.79e308', -1, '1.79e+308', '1.79e308'),
            ('-1.79e+308', '-1.79e308', 0, '-1.79e+308', '-1.79e308'),
            ('-1.79e+308', '-1.79e308', 1, '-1.79e+308', '-1.79e308'),
            ('-1.79e+308', '-1.79e308', -1, '-1.79e+308', '-1.79e308')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            v = client.execute_command(
                'JSON.NUMINCRBY', wikipedia, '.foo', incr)
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt

    def test_json_nummultby_large_numbers(self):
        '''
        Test edge cases with large values near the boundaries of int64 and double.
        '''
        client = self.server.get_new_client()

        # Multiplication causes overflow
        for (val, val_alt, mult) in [
            ('9223372036854775807', '9223372036854775807', 1.79e+308),
            ('9223372036854775807', '9223372036854775807', -1.79e+308),
            ('1.79e+308', '1.79e308', 1.79e308),
            ('1.03e+300', '1.03e300', 1.03e300),
            ('1.79e+308', '1.79e308', -1.79e308),
            ('-1.03e+300', '-1.03e300', 1.03e300)
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'SPACE', ' ', 'NEWLINE', '\n', '.foo')
            assert v is not None and v.decode() == val or v.decode() == val_alt
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    'JSON.NUMMULTBY', wikipedia, '.foo', mult)
            assert self.error_class.is_number_overflow_error(str(e.value))

        # The result is still within the range of double
        data = [
            ('9223372036854775807', '9223372036854775807', 0, '0',           '0'),
            ('9223372036854775807', '9223372036854775807', 2,
             '1.8446744073709552e+19', '18446744073709553000.0'),
            ('9223372036854775807', '9223372036854775807', 9223372036854775807,
             '8.5070591730234616e+37', '8.5070591730234616e37'),
            ('9223372036854775807', '9223372036854775807', -9223372036854775807,
             '-8.5070591730234616e+37', '-8.5070591730234616e37'),
            ('1.79e308', '1.79e+308', 0,   '0',          '0'),
            ('1.79e308', '1.79e+308', 1,   '1.79e+308',  '1.79e308'),
            ('-1.79e308', '-1.79e+308', 1, '-1.79e+308', '-1.79e308'),
            ('9223372036854775807', '9223372036854775807', 1,
             '9.2233720368547758e+18', '9223372036854775807')
        ]

        for (val, val_alt, mult, exp, exp_alt) in data:
            assert b'OK' == client.execute_command(
                'JSON.SET', wikipedia, '.foo', val)
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and (
                v.decode() == val or v.decode() == val_alt)
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and (
                v.decode() == val or v.decode() == val_alt)
            v = client.execute_command(
                'JSON.NUMMULTBY', wikipedia, '.foo', mult)
            # print("DEBUG val: %s, mult: %f, v: %s, exp: %s" %(val, mult, v.decode(), exp))
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            v = client.execute_command(
                'JSON.GET', wikipedia, '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt
            # verify pretty-print gives us the same result
            v = client.execute_command(
                'JSON.GET', wikipedia, 'INDENT', '\t', '.foo')
            assert v is not None and v.decode() == exp or v.decode() == exp_alt

    def test_json_strlen_command(self):
        client = self.server.get_new_client()
        assert 2 == client.execute_command(
            'JSON.STRLEN', wikipedia, '.address.state')
        assert 4 == client.execute_command(
            'JSON.STRLEN', wikipedia, '.firstName')

        # edge case: empty string
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '""')
        assert 0 == client.execute_command(
            'JSON.STRLEN', wikipedia, '.foo')

        # return should be null if document key does not exist
        assert None == client.execute_command(
            'JSON.STRLEN', foo, '.firstName')

        # return error if path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.STRLEN', wikipedia, '.bar')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: element is not a string
        for path in ['.address', '.groups', '.age', '.isAlive', '.spouse']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.STRLEN', wikipedia, path)

        # Wrong number of arguments

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.STRLEN', wikipedia, '.firstName', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_strappend_command(self):
        client = self.server.get_new_client()
        for (val, new_len, new_val) in [
            ('"son"',       7,  '"Johnson"'),
            ('" Junior"',   14, '"Johnson Junior"'),
            ('" is"',       17, '"Johnson Junior is"'),
            ('" my"',       20, '"Johnson Junior is my"'),
            ('" friend."',  28, '"Johnson Junior is my friend."'),
            ('""',          28, '"Johnson Junior is my friend."')
        ]:
            assert new_len == client.execute_command(
                'JSON.STRAPPEND', wikipedia, '.firstName', val)
            assert new_val == client.execute_command(
                'JSON.GET', wikipedia, '.firstName').decode('utf-8')

        # edge case: appending to an empty string
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foo', '""')
        client.execute_command(
            'JSON.STRAPPEND', wikipedia, '.foo', '"abc"')
        assert b'"abc"' == client.execute_command(
            'JSON.GET', wikipedia, '.foo')

        # error condition: document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.STRAPPEND', foo, '.firstName', '"abc"')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.STRAPPEND', wikipedia, '.bar', '"abc"')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: attempt to append a non-string value
        for val in ['123', 'true', 'false', 'null', '{}', '[]']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.STRAPPEND', wikipedia, '.firstName', val)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # error condition: attempt to append to a non-string element
        for path in ['.address', '.groups', '.age', '.isAlive', '.spouse']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.STRAPPEND', wikipedia, path, '"12"')
            assert self.error_class.is_wrongtype_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.STRAPPEND', wikipedia, '.firstName', '"abc"', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_strlen_command_legacy_and_jsonpath_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command('JSON.SET', k1, '.',
                               '{"a":{"a":"a"}, "b":{"a":""}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}')

        for (key, path, exp) in [
            (k1, '$.a.a', [1]),
            (k1, '$.a.*', [1]),
            (k1, '$.b.a', [0]),
            (k1, '$.b.*', [0]),
            (k1, '$.c.*', [1, 2]),
            (k1, '$.c.b', [2]),
            (k1, '$.d.*', [None, 1, None]),
            (k1, '.a.a',  1),
            (k1, '.a.*',  1),
            (k1, '.b.a',  0),
            (k1, '.b.*',  0),
            (k1, '.c.*',  1),
            (k1, '.c.b',  2),
        ]:
            assert exp == client.execute_command(
                'JSON.STRLEN', key, path)

        assert 1 == client.execute_command(
            'JSON.STRLEN', k1, '.d.*')

    def test_json_strappend_command_legacy_and_jsonpath_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command('JSON.SET', k1, '.',
                               '{"a":{"a":"a"}, "b":{"a":""}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}')

        # NOTE: The expected result below accounts for the outcome of previous commands.
        for (key, path, append, exp_ret, exp_new_str, exp_whole_json) in [
            (k1, '$.a.a', '"x"', [
             2],   '["ax"]',   '{"a":{"a":"ax"},"b":{"a":""},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.a.a', '""',  [
             2],   '["ax"]',   '{"a":{"a":"ax"},"b":{"a":""},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.a.*', '"yz"', [4],   '["axyz"]',
             '{"a":{"a":"axyz"},"b":{"a":""},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '.a.a',  '"a"', 5,     '"axyza"',
             '{"a":{"a":"axyza"},"b":{"a":""},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '.a.*',  '"a"', 6,     '"axyzaa"',
             '{"a":{"a":"axyzaa"},"b":{"a":""},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.b.a', '"a"', [
             1],   '["a"]',    '{"a":{"a":"axyzaa"},"b":{"a":"a"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.b.*', '""',  [1],   '["a"]',
             '{"a":{"a":"axyzaa"},"b":{"a":"a"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.b.*', '"a"', [2],   '["aa"]',
             '{"a":{"a":"axyzaa"},"b":{"a":"aa"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '.b.a',  '"a"', 3,     '"aaa"',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaa"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '.b.*',  '"a"', 4,     '"aaaa"',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.c.*', '"a"', [2, 3], '["aa","bba"]',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aa","b":"bba"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.c.b', '"a"', [
             4],   '["bbaa"]', '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}}'),

            # The following strappend changes value at $.c value to {"a":"aaa", "b":"bbaaa"}.
            # strappend returns length of the last updated value, which is 5,
            # while 'json.get .c.*' returns the first selected element, which is "aaa".
            (k1, '.c.*',  '"a"', 5,      '"aaa"',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aaa","b":"bbaaa"},"d":{"a":1,"b":"b","c":3}}'),

            (k1, '.c.b',  '"a"', 6,      '"bbaaaa"',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aaa","b":"bbaaaa"},"d":{"a":1,"b":"b","c":3}}'),
            (k1, '$.d.*', '"a"', [None, 2, None], '[1,"ba",3]',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aaa","b":"bbaaaa"},"d":{"a":1,"b":"ba","c":3}}'),

            # strappend returns length of the last updated value, which is 3 ("baa"),
            # while 'json.get .d.*' returns the first selected element, which is 1.
            (k1, '.d.*',  '"a"', 3,      '1',
             '{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aaa","b":"bbaaaa"},"d":{"a":1,"b":"baa","c":3}}')
        ]:
            assert exp_ret == client.execute_command(
                'JSON.STRAPPEND', key, path, append)
            assert exp_new_str == client.execute_command(
                'JSON.GET', key, path).decode()

    def test_json_objectlen_command(self):
        client = self.server.get_new_client()
        assert 4 == client.execute_command(
            'JSON.OBJLEN', wikipedia, '.address')

        # edge case: empty object
        assert 0 == client.execute_command(
            'JSON.OBJLEN', wikipedia, '.groups')

        # return should be null if document key does not exist
        assert None == client.execute_command(
            'JSON.OBJLEN', foo, '.address')

        # return error if path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.OBJLEN', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an object
        for path in ['.children', '.phoneNumbers', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.OBJLEN', wikipedia, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.OBJLEN', wikipedia, '.', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_objlen_command_jsonpath_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command('JSON.SET', k1, '.',
                               '{"a":{}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}')

        test_cases = [
            (k1, '$.a',   [0]),
            (k1, '$.a.*', []),
            (k1, '.a',    0),
            (k1, '$.c',   [2]),
            (k1, '$.c.*', [None, None]),
            (k1, '.c',    2),
            (k1, '$.d',   [3]),
            (k1, '$.d.*', [None, None, 2]),
            (k1, '.d',    3),
            (k1, '$.*',   [0, 2, 3, None]),
            (k1, '.*',    0),
            (k1, '.d.*',  2),
        ]

        for (key, path, exp) in test_cases:
            assert exp == client.execute_command(
                'JSON.OBJLEN', key, path)

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.OBJLEN', k1, '.a.*')
        assert self.error_class.is_nonexistent_error(str(e.value))

        for (key, path) in [
            (k1, '.c.*'),
            (k1, '.e')
        ]:
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    'JSON.OBJLEN', key, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_objectkeys_command(self):
        client = self.server.get_new_client()
        obj_keys = [b'street', b'city', b'state', b'zipcode']
        assert obj_keys == client.execute_command(
            'JSON.OBJKEYS', wikipedia, '.address')

        # edge case: empty object
        assert [] == client.execute_command(
            'JSON.OBJKEYS', wikipedia, '.groups')

        # return should be null if document key does not exist
        assert None == client.execute_command(
            'JSON.OBJKEYS', foo, '.address')

        # return should be null if path does not exist
        assert None == client.execute_command(
            'JSON.OBJKEYS', wikipedia, '.foo')

        # error condition: the element is not an object
        for path in ['.children', '.phoneNumbers', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.OBJKEYS', wikipedia, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.OBJKEYS', wikipedia, '.', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_objkeys_command_jsonpath_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command('JSON.SET', k1, '.',
                               '{"a":{}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}')

        test_cases = [
            (k1, '$.a',   [[]]),
            (k1, '$.a.*', []),
            (k1, '.a',    []),
            (k1, '$.c',   [[b"a", b"b"]]),
            (k1, '.c',    [b"a", b"b"]),
            (k1, '$.d',   [[b"a", b"b", b"c"]]),
            (k1, '$.d.*', [[], [], [b"a", b"b"]]),
            (k1, '.d',    [b"a", b"b", b"c"]),
            (k1, '.d.*',  [b"a", b"b"]),
            (k1, '$.*',   [[], [b"a", b"b"], [b"a", b"b", b"c"], []]),
            (k1, '.*',    [b"a", b"b"]),
            (k1, '$.c.*', [[], []]),
            (k1, '$.c.*', [None, None])
        ]

        for (key, path, exp) in [
        ]:
            assert exp == client.execute_command(
                'JSON.OBJKEYS', key, path)

        for (key, path) in [
            (k1, '.c.*'),
            (k1, '.e')
        ]:
            with pytest.raises(ResponseError) as e:
                assert None == client.execute_command(
                    'JSON.OBJLEN', key, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_arrlen_command(self):
        client = self.server.get_new_client()
        assert 2 == client.execute_command(
            'JSON.ARRLEN', wikipedia, '.phoneNumbers')

        # edge case: empty array
        assert 0 == client.execute_command(
            'JSON.ARRLEN', wikipedia, '.children')

        # return should be null if document key does not exist
        assert None == client.execute_command(
            'JSON.ARRLEN', foo, '.phoneNumbers')

        # return error if path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRLEN', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRLEN', wikipedia, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.ARRLEN', wikipedia, '.phoneNumbers', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_arrlen_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], [\"a\"], [\"a\", \"b\"], [\"a\", \"b\", \"c\"]]')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[[], \"a\", [\"a\", \"b\"], [\"a\", \"b\", \"c\"], 4]')

        for (key, path, exp) in [
            (k1, '$.[*]', [0, 1, 2, 3]),
            (k2, '$.[*]', [0, None, 2, 3, None])
        ]:
            assert exp == client.execute_command(
                'JSON.ARRLEN', key, path)

    def test_json_arrappend_command(self):
        client = self.server.get_new_client()
        # edge case: append to an empty array
        assert 1 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"John"')
        assert b'["John"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')

        # append to non-empty array
        assert 3 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"Mary"', '"Tom"')
        assert b'["John","Mary","Tom"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')
        assert 3 == client.execute_command(
            'JSON.ARRLEN', wikipedia, '.children')

        # return error if document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRAPPEND', foo, '.children', '"Mary"')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRAPPEND', wikipedia, '.foo', '"abc"')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRAPPEND', wikipedia, path, '123')
            assert self.error_class.is_wrongtype_error(str(e.value))

    def test_json_arrappend_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], [\"a\"], [\"a\", \"b\"], [\"a\", \"b\", \"c\"]]')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[[], [\"a\"], [\"a\", \"b\"], [\"a\", \"b\", \"c\"]]')

        for (key, path, val, exp, new_val) in [
            (k1, '$.[*]', '"a"', [1, 2, 3, 4],
             '[[\"a\"],[\"a\",\"a\"],[\"a\",\"b\",\"a\"],[\"a\",\"b\",\"c\",\"a\"]]'),
            (k2, '$.[*]', '""',  [1, 2, 3, 4],
             '[[\"\"],[\"a\",\"\"],[\"a\",\"b\",\"\"],[\"a\",\"b\",\"c\",\"\"]]')
        ]:
            assert exp == client.execute_command(
                'JSON.ARRAPPEND', key, path, val)
            assert new_val == client.execute_command(
                'JSON.GET', key, path).decode()

    def test_json_arrpop_command(self):
        client = self.server.get_new_client()
        # edge case: pop an empty array
        assert None == client.execute_command(
            'JSON.ARRPOP', wikipedia, '.children')

        # populate the array
        assert 3 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"John"', '"Mary"', '"Tom"')

        for (idx, popped_out, new_len, new_val) in [
            (1, '"Mary"', 2, '["John","Tom"]'),
            (-1, '"Tom"', 1, '["John"]'),
            (0, '"John"', 0, '[]')
        ]:
            assert popped_out == client.execute_command(
                'JSON.ARRPOP', wikipedia, '.children', idx).decode('utf-8')
            assert new_len == client.execute_command(
                'JSON.ARRLEN', wikipedia, '.children')
            assert new_val == client.execute_command(
                'JSON.GET', wikipedia, '.children').decode('utf-8')

        # edge case: pop an empty array
        assert None == client.execute_command(
            'JSON.ARRPOP', wikipedia, '.children')

        # return error if document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRPOP', foo, '.children')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRPOP', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRPOP', wikipedia, path)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # test large index: larger than int32
        client.execute_command(
            'JSON.ARRPOP', wikipedia, ".children", 3000000000)

        # Wrong number of arguments
        res = b'{"type":"home","number":"212 555-1234"}'
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.ARRPOP', wikipedia, '.phoneNumbers', 0, 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_arrpop_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], ["a"], ["a", "b"], ["a", "b", "c"]]')

        client.execute_command('COPY', k1, k2)
        client.execute_command('COPY', k1, k3)

        for (key, path, index, exp, exp_new_val) in [
            (k1, '$.[*]', 0,  [None, b'"a"', b'"a"', b'"a"'],
             '[[],[],["b"],["b","c"]]'),
            (k2, '$.[*]', 1,  [None, b'"a"', b'"b"', b'"b"'],
             '[[],[],["a"],["a","c"]]'),
            (k3, '$.[*]', -1, [None, b'"a"', b'"b"', b'"c"'],
             '[[],[],["a"],["a","b"]]')
        ]:
            assert exp == client.execute_command(
                'JSON.ARRPOP', key, path, index)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

    def test_json_arrinsert_command(self):
        client = self.server.get_new_client()
        # edge case: insert into an empty array
        assert 1 == client.execute_command(
            'JSON.ARRINSERT', wikipedia, '.children', 0, '"foo"')
        assert b'["foo"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')
        assert b'"foo"' == client.execute_command(
            'JSON.ARRPOP', wikipedia, '.children')

        # populate the array
        assert 3 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"John"', '"Mary"', '"Tom"')
        assert b'["John","Mary","Tom"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')

        for (idx, val, new_len, new_val) in [
            (0, '"Kathy"', 4, '["Kathy","John","Mary","Tom"]'),
            (2, '"Rose"', 5, '["Kathy","John","Rose","Mary","Tom"]'),
            (3, '"Bob"', 6, '["Kathy","John","Rose","Bob","Mary","Tom"]'),
            (-1, '"Peter"', 7,
             '["Kathy","John","Rose","Bob","Mary","Peter","Tom"]'),
            (-1, '"Jane"', 8,
             '["Kathy","John","Rose","Bob","Mary","Peter","Jane","Tom"]'),
            (8, '"Grace"', 9,
             '["Kathy","John","Rose","Bob","Mary","Peter","Jane","Tom","Grace"]')
        ]:
            assert new_len == client.execute_command(
                'JSON.ARRINSERT', wikipedia, '.children', idx, val)
            assert new_val == client.execute_command(
                'JSON.GET', wikipedia, '.children').decode('utf-8')

        # return error if document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINSERT', foo, '.children', 0, '"abc"')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINSERT', wikipedia, '.foo', 0, '123')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRINSERT', wikipedia, path, 0, '1')
            assert self.error_class.is_wrongtype_error(str(e.value))

        # error condition: index arg is out of array boundaries
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINSERT', wikipedia, ".children", 3000000000, '"a"')
        assert self.error_class.is_outofboundaries_error(str(e.value))

        # error condition: index arg is out of array boundaries
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINSERT', wikipedia, ".children", 31, '"a"')
        assert self.error_class.is_outofboundaries_error(str(e.value))

    def test_json_arrinsert_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], [0], [0, 1], [0, 1, 2]]')
        # COPY NOT SUPPORTED by REJSON
        client.execute_command('JSON.SET', k2, '.',
                               '[[], [0], [0, 1], [0, 1, 2]]')
        client.execute_command('JSON.SET', k3, '.',
                               '[[], [0], [0, 1], [0, 1, 2]]')

        test_cases = [
            (k1, '$.[*]', 0,  '3', [1, 2, 3, 4],
             '[[3],[3,0],[3,0,1],[3,0,1,2]]'),
            (k3, '$.[*]', -1, '3', [1, 2, 3, 4],
             '[[3],[3,0],[0,3,1],[0,1,3,2]]')
        ]

        # Negative paths beyond array length work strangely in ReJSON
        for (key, path, index, val, exp, exp_new_val) in test_cases:
            assert exp == client.execute_command(
                'JSON.ARRINSERT', key, path, index, val)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

        # test index out of bounds error
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINSERT', k2, "$.[*]", 1, '3')
        assert self.error_class.is_outofboundaries_error(str(e.value))

    def test_json_clear_command(self):
        client = self.server.get_new_client()

        # populate the array to be cleared
        assert 3 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"John"', '"Mary"', '"Tom"')
        assert b'["John","Mary","Tom"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')

        # clears and counts 1, 0 remain
        assert 1 == client.execute_command(
            'JSON.CLEAR', wikipedia, '.children')
        assert 0 == client.execute_command(
            'JSON.ARRLEN', wikipedia, '.children')

        # clears empty array
        assert 0 == client.execute_command(
            'JSON.CLEAR', wikipedia, '.children')
        assert 0 == client.execute_command(
            'JSON.ARRLEN', wikipedia, '.children')

        # if path does not exist, the command should return 0
        assert 0 == client.execute_command(
            'JSON.CLEAR', wikipedia, '.foo')

        # if the value at the path is not a container, the command should return 0
        assert b'OK' == client.execute_command(
            'JSON.SET', wikipedia, '.foobool', 'false')
        assert 0 == client.execute_command(
            'JSON.CLEAR', wikipedia, '.foobool')

        # clear the wikipedia object entirely
        wikipedia_objlen = client.execute_command(
            'JSON.OBJLEN', wikipedia)
        assert 0 != wikipedia_objlen
        assert 1 == client.execute_command(
            'JSON.CLEAR', wikipedia)
        assert 0 == client.execute_command(
            'JSON.OBJLEN', wikipedia)

    def test_json_clear_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '{"a":{}, "b":{"a": 1, "b": null, "c": true}, "c":1, "d":true, "e":null, "f":"d"}')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[[], [0], [0,1], [0,1,2], 1, true, null, "d"]')

        test_cases = [
            (k1, '$.*',  4, '{"a":{},"b":{},"c":0,"d":false,"e":null,"f":""}'),
            (k2, '$[*]', 6, '[[],[],[],[],0,false,null,""]')
        ]

        for (key, path, exp, exp_new_val) in test_cases:
            assert exp == client.execute_command(
                'JSON.CLEAR', key, path)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

    def test_json_arrtrim_command(self):
        client = self.server.get_new_client()
        # edge case: empty array
        assert 0 == client.execute_command(
            'JSON.ARRTRIM', wikipedia, '.children', 0, 1)

        # populate the array
        assert 3 == client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.children', '"John"', '"Mary"', '"Tom"')
        assert b'["John","Mary","Tom"]' == client.execute_command(
            'JSON.GET', wikipedia, '.children')

        for (path, start, end, new_len, new_val) in [
            ('.children',       1, 2, 2, '["Mary","Tom"]'),
            ('.children',       0, 0, 1, '["Mary"]'),
            ('.children',      -1, 5, 1, '["Mary"]'),
            ('.phoneNumbers',   2, 0, 0, '[]')
        ]:
            assert new_len == client.execute_command(
                'JSON.ARRTRIM', wikipedia, path, start, end)
            assert new_val == client.execute_command(
                'JSON.GET', wikipedia, path).decode('utf-8')

        # return error if document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRTRIM', foo, '.children', 0, 1)
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRTRIM', wikipedia, '.foo', 0, 3)
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRTRIM', wikipedia, path, 0, 1)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # test large index: larger than int32
        client.execute_command(
            'JSON.ARRTRIM', wikipedia, ".phoneNumbers", 3000000000, 3000000001)

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.ARRTRIM', wikipedia, '.phoneNumbers', 0, 1, 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_arrtrim_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], ["a"], ["a", "b"], ["a", "b", "c"]]')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[[], ["a"], ["a", "b"], ["a", "b", "c"]]')
        assert b'OK' == client.execute_command('JSON.SET', k3, '.',
                                               '[[], [0], [0,1], [0,1,2], [0,1,2,3]]')

        for (key, path, start, stop, exp, exp_new_val) in [
            (k1, '$.[*]', 0, 1, [0, 1, 2, 2],
             '[[],["a"],["a","b"],["a","b"]]'),
            (k2, '$.[*]', 1, 1, [0, 0, 1, 1], '[[],[],["b"],["b"]]'),
            (k3, '$.[*]', 1, 2, [0, 0, 1, 2, 2], '[[],[],[1],[1,2],[1,2]]')
        ]:
            assert exp == client.execute_command(
                'JSON.ARRTRIM', key, path, start, stop)
            assert exp_new_val == client.execute_command(
                'JSON.GET', key, '.').decode()

    def test_json_arrindex_command(self):
        # edge case: empty array
        client = self.server.get_new_client()
        assert -1 == client.execute_command(
            'JSON.ARRINDEX', wikipedia, '.children', '"tom"')

        # populate the array
        assert 5 == client.execute_command('JSON.ARRAPPEND', wikipedia, '.children',
                                           '"John"', '"Mary"', '"Tom"', '"Paul"', '"Peter"')

        for (val, idx) in [
            ('"John"', 0),
            ('"Tom"', 2),
            ('"Peter"', 4),
            ('"Peter2"', -1)
        ]:
            assert idx == client.execute_command(
                'JSON.ARRINDEX', wikipedia, '.children', val)

        for (val, start, stop, idx) in [
            ('"Tom"', 5, 0, -1),
            ('"Paul"', 0, 4, 3),
            ('"Paul"', 0, 0, 3)
        ]:
            assert idx == client.execute_command(
                'JSON.ARRINDEX', wikipedia, '.children', val, start, stop)

        assert b'OK' == client.execute_command(
            'JSON.SET', arr, '.', '[0, 1, 2, 3, 4]')
        assert 1 == client.execute_command(
            'JSON.ARRINDEX', arr, '.', '1')
        assert - \
            1 == client.execute_command(
                'JSON.ARRINDEX', arr, '.', '1', '2')

        # return error if document key does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINDEX', foo, '.children', 0, 5)
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.ARRINDEX', wikipedia, '.foo', 0, 3)
        assert self.error_class.is_nonexistent_error(str(e.value))

        # error condition: the element is not an array
        for path in ['.address', '.groups', '.age', '.weight', '.isAlive', '.spouse', '.firstName']:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.ARRINDEX', wikipedia, path, '1')
            assert self.error_class.is_wrongtype_error(str(e.value))

        # test large index: larger than int32
        client.execute_command(
            'JSON.ARRINDEX', wikipedia, ".phoneNumbers", '1', 3000000000, 0)

    def test_json_arrindex_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '[[], [\"a\"], [\"a\", \"b\"], [\"a\", \"b\", \"c\"]]')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[[], [0], [0,1], [0,1,2]]')
        assert b'OK' == client.execute_command('JSON.SET', k3, '.',
                                               '[[], [0,true], [0,1,false], [0,1,2,null,true]]')

        for (key, path, val, exp) in [
            (k1, '$.[*]', '"a"',  [-1, 0, 0, 0]),
            (k1, '$.[*]', '"b"',  [-1, -1, 1, 1]),
            (k1, '$.[*]', '"c"',  [-1, -1, -1, 2]),
            (k2, '$.[*]', '1',    [-1, -1, 1, 1]),
            (k2, '$.[*]', '2',    [-1, -1, -1, 2]),
            (k2, '$.[*]', 'true', [-1, -1, -1, -1]),
            (k3, '$.[*]', 'true', [-1, 1, -1, 4]),
            (k3, '$.[*]', 'null', [-1, -1, -1, 3])
        ]:
            assert exp == client.execute_command(
                'JSON.ARRINDEX', key, path, val)

    def test_json_arrindex_should_not_limit_to_scalar_value(self):
        client = self.server.get_new_client()
        client.execute_command(
            'JSON.SET', k1, '.', '[5, 6, {"a":"b"}, [99,100]]')
        assert 2 == client.execute_command(
            'JSON.ARRINDEX', k1, '.', '{"a":"b"}', 0, 0)
        assert 3 == client.execute_command(
            'JSON.ARRINDEX', k1, '.', '[99,100]', 0, 0)
        assert 0 == client.execute_command(
            'JSON.ARRINDEX', k1, '.', '5', 0, 0)
        assert 1 == client.execute_command(
            'JSON.ARRINDEX', k1, '.', '6', 0, 0)

        # Wrong number of arguments
        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.ARRINDEX', wikipedia, '.phoneNumbers', '1', 0, 3, 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_arrindex_complex_v2_path(self):
        client = self.server.get_new_client()

        json_string = '{"level0":{"level1_0":{"level2":[1,2,3, [25, [4,5,{"c":"d"}]]]},"level1_1":{"level2":[[{"a":[2,5]},true,null]]}}}'
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', json_string)
        for (path, val, exp) in [
            ("$..level0.level1_0..", b'[4,5,{"c":"d"}]',         [
             None, -1, 1, -1, None]),
            ("$..level0.level1_0..", b'[25,[4,5,{"c":"d"}]]',    [
             None, 3, -1, -1, None]),
            ("$..level0.level1_0..", b'{"c":"d"}',
             [None, -1, -1, 2, None]),
            ("$..level0.level1_1..", b'[{"a":[2,5]},true,null]', [
             None, 0, -1, None, -1]),
            ("$..level0.level1_1..", b'[null,true,{"a":[2,5]}]', [
             None, -1, -1, None, -1]),
            ("$..level0.level1_1..", b'[{"a":[2,5]},true]',      [
             None, -1, -1, None, -1]),
            ("$..level0.level1_0..", b'[4,{"c":"d"}]',           [
             None, -1, -1, -1, None])
        ]:
            assert exp == client.execute_command(
                'JSON.ARRINDEX', k1, path, val)

    def test_json_type_command(self):
        client = self.server.get_new_client()
        for (path, type) in [
            ('.', 'object'),
            ('.groups', 'object'),
            ('.phoneNumbers', 'array'),
            ('.children', 'array'),
            ('.isAlive', 'boolean'),
            ('.spouse', 'null'),
            ('.address.city', 'string'),
            ('.age', 'integer'),
            ('.weight', 'number')
        ]:
            assert type == client.execute_command(
                'JSON.TYPE', wikipedia, path).decode('utf-8')

        # return should be null if document key does not exist
        assert None == client.execute_command('JSON.TYPE', foo)

        # return should be null if path does not exist
        assert None == client.execute_command(
            'JSON.TYPE', wikipedia, '.foo')

        # Wrong number of arguments

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.TYPE', wikipedia, '.phoneNumbers', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_type_command_jsonpath(self):
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '{"a":1, "b":2.3, "c":"foo", "d":true, "e":null, "f":{}, "g":[]}')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '[1, 2.3, "foo", true, null, {}, []]')

        for (key, path, exp) in [
            (k1, '$.*',  [b"integer", b"number", b"string",
             b"boolean", b"null", b"object", b"array"]),
            (k2, '$[*]', [b"integer", b"number", b"string",
             b"boolean", b"null", b"object", b"array"])
        ]:
            assert exp == client.execute_command(
                'JSON.TYPE', key, path)

    def test_json_resp_command(self):
        client = self.server.get_new_client()
        for (path, res) in [
            ('.firstName', b'John'),
            ('.isAlive', b'true'),
            ('.age', 27),
            ('.spouse', None)
        ]:
            assert res == client.execute_command(
                'JSON.RESP', wikipedia, path)

        for (path, res) in [('.weight', '135.17')]:
            assert res == client.execute_command(
                'JSON.RESP', wikipedia, path).decode()

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '.children')
        assert 1 == len(arr)
        assert arr == [b'[']

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '.address')
        assert 5 == len(arr)

        assert arr[0:4] == [b'{', [b'street', b'21 2nd Street'], [
            b'city', b'New York'], [b'state', b'NY']]
        assert b'10021-3100' == arr[4][1]

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '.phoneNumbers')
        assert 3 == len(arr)
        assert b'[' == arr[0]

        assert 3 == len(arr[1])
        assert arr[1] == [b'{', [b'type', b'home'],
                          [b'number', b'212 555-1234']]
        assert 3 == len(arr[2])
        assert arr[2] == [b'{', [b'type', b'office'],
                          [b'number', b'646 555-4567']]

        # return should be null if document key does not exist
        assert None == client.execute_command('JSON.RESP', foo)

        # error condition: path does not exist
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.RESP', wikipedia, '.foo')
        assert self.error_class.is_nonexistent_error(str(e.value))

        # Wrong number of arguments

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.RESP', wikipedia, '.phoneNumbers', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

    def test_json_resp_command_jsonpath(self):
        client = self.server.get_new_client()

        for (path, res) in [
            ('$.firstName',  [b'John']),
            ('$.isAlive',    [b'true']),
            ('$.age',        [27]),
            ('$.spouse',     [None]),
            ('$.foo',        [])
        ]:
            assert res == client.execute_command(
                'JSON.RESP', wikipedia, path)

        for (path, res) in [('$.weight', '135.17')]:
            assert res == client.execute_command(
                'JSON.RESP', wikipedia, path)[0].decode()

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '$.children')
        assert 1 == len(arr)
        assert arr == [[b'[']]

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '$.address.*')
        assert 4 == len(arr)
        assert arr == [b'21 2nd Street', b'New York', b'NY', b'10021-3100']

        arr = client.execute_command(
            'JSON.RESP', wikipedia, '$.phoneNumbers.*')
        assert 2 == len(arr)
        assert arr[0] == [b'{', [b'type', b'home'],
                          [b'number', b'212 555-1234']]
        assert arr[1] == [b'{', [b'type', b'office'],
                          [b'number', b'646 555-4567']]

    def test_json_debug_memory(self):
        # non-existent key
        client = self.server.get_new_client()

        assert None == client.execute_command(
            'JSON.DEBUG MEMORY', nonexistentkey)

        # non-existent path
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.DEBUG MEMORY', wikipedia, nonexistentpath)
        assert self.error_class.is_nonexistent_error(str(e.value))

        # syntax error: key not provided
        with pytest.raises(ResponseError) as e:
            client.execute_command('JSON.DEBUG MEMORY')
        assert str(e.value).startswith('wrong number of arguments')

        # syntax error: wrong subcommand
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.DEBUG MEMORY123', wikipedia)
        assert self.error_class.is_syntax_error(str(e.value))

        with pytest.raises(ResponseError) as e:
            client.execute_command('JSON.DEBUG M', wikipedia)
        assert self.error_class.is_syntax_error(str(e.value))

        with pytest.raises(ResponseError) as e:
            assert None == client.execute_command(
                'JSON.DEBUG MEMORY', wikipedia, '.', 'extra')
        assert str(e.value).find('wrong number of arguments') >= 0

        # Test shared path
        no_shared_mem = client.execute_command(
            'JSON.DEBUG', 'MEMORY', wikipedia)
        with_shared_mem = client.execute_command(
            'JSON.DEBUG', 'MEMORY', wikipedia, '.')
        assert with_shared_mem > no_shared_mem

    def test_json_duplicate_keys(self):
        client = self.server.get_new_client()
        '''Test handling of object with duplicate keys'''
        client.execute_command(
            'JSON.SET', k1, '.', '{"a":0, "a":1}')
        assert b'{"a":1}' == client.execute_command(
            'JSON.GET', k1, '.')
        assert [b"a"] == client.execute_command(
            'JSON.OBJKEYS', k1)
        assert b'1' == client.execute_command(
            'JSON.GET', k1, 'a')
        assert 1 == client.execute_command('JSON.OBJLEN', k1)
        client.execute_command('JSON.SET', k1, 'a', '2')
        assert b'{"a":2}' == client.execute_command(
            'JSON.GET', k1, '.')
        client.execute_command('JSON.NUMINCRBY', k1, 'a', '2')
        assert b'{"a":4}' == client.execute_command(
            'JSON.GET', k1, '.')
        client.execute_command('JSON.NUMMULTBY', k1, 'a', '2')
        assert b'{"a":8}' == client.execute_command(
            'JSON.GET', k1, '.')
        client.execute_command('JSON.DEL', k1, 'a')
        assert b'{}' == client.execute_command(
            'JSON.GET', k1, '.')

    def test_json_set_command_max_depth(self):
        client = self.server.get_new_client()

        def json_with_depth(depth):
            return '{"a":'*depth + '{}' + '}'*depth

        depth_limit = 128
        client.execute_command(
            'config set json.max-path-limit ' + str(depth_limit))

        # json not too deep: ok
        assert b'OK' == client.execute_command(
            'JSON.SET', k, '.', json_with_depth(127))

        # error condition: json is too deep
        with pytest.raises(ResponseError) as e:
            json_deep = json_with_depth(200000)
            client.execute_command(
                'JSON.SET', k, '.', json_deep)
        assert self.error_class.is_limit_exceeded_error(str(e.value))

    def test_json_set_command_max_size(self):
        client = self.server.get_new_client()

        def json_with_size(size):
            return '"' + 'a'*(size-2) + '"'

        MB = 2**20
        assert b'OK' == client.execute_command(
            'JSON.SET', k, '.', json_with_size(33*MB))

        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', k, '.', json_with_size(64*MB))
        assert self.error_class.is_limit_exceeded_error(str(e.value))

    def test_multi_exec(self):
        client = self.server.get_new_client()
        client.execute_command('MULTI')
        client.execute_command(
            'JSON.DEL', wikipedia, '.address.street')
        client.execute_command(
            'JSON.DEL', wikipedia, '.address.zipcode')
        client.execute_command(
            'JSON.SET', wikipedia, '.address.region', '"US East"')
        client.execute_command('EXEC')
        v = client.execute_command(
            'JSON.GET', wikipedia, '.address').decode()
        assert v == '{"city":"New York","state":"NY","region":"US East"}' or \
            v == '{"state":"NY","city":"New York","region":"US East"}'

        client.execute_command('MULTI')
        client.execute_command(
            'JSON.ARRPOP', wikipedia, '.phoneNumbers')
        client.execute_command(
            'JSON.ARRPOP', wikipedia, '.phoneNumbers')
        client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.phoneNumbers', '123')
        client.execute_command(
            'JSON.ARRAPPEND', wikipedia, '.phoneNumbers', '456')
        client.execute_command('EXEC')
        v = client.execute_command(
            'JSON.GET', wikipedia, '.phoneNumbers').decode()
        assert v == '[123,456]'

    def test_escaped_member_names(self):
        '''
        Test accessing member names that contain escaped characters.
        '''
        client = self.server.get_new_client()

        assert b'OK' == client.execute_command('JSON.SET', k1, '.',
                                               '{"a\\\\a":1, "b\\tb":2, "c\\nc":3, "d\\rd":4, "e\\be":5, "f\\"f":6, "":7, "\'":8}')
        assert b'OK' == client.execute_command('JSON.SET', k2, '.',
                                               '{"key\\u0000":"value\\u0000", "key\\u001F":"value\\u001F"}')
        for (key, path, exp) in [
            (k1, '$["a\\\\a"]', '[1]'),
            (k1, "$['a\\a']",   '[1]'),
            (k1, '$["b\\tb"]',  '[2]'),
            (k1, "$['b\\tb']",  '[2]'),
            (k1, '$["c\\nc"]',  '[3]'),
            (k1, "$['c\\nc']",  '[3]'),
            (k1, '$["d\\rd"]',  '[4]'),
            (k1, "$['d\\rd']",  '[4]'),
            (k1, '$["e\\be"]',  '[5]'),
            (k1, "$['e\\be']",  '[5]'),
            (k1, '$["f\\"f"]',  '[6]'),
            (k1, "$['f\"f']",   '[6]'),
            (k1, '$[""]',       '[7]'),
            (k1, "$['']",       '[7]'),
            (k1, '$["\'"]',     '[8]'),
            (k1, "$['\\\'']",   '[8]'),
            (k2, '$["key\\u0000"]', '["value\\u0000"]'),
            (k2, '$["key\\u001F"]', '["value\\u001F"]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path).decode()

    def test_serializing_escaped_quotes_in_member_name(self):
        client = self.server.get_new_client()
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', '{"\\"a":1, "\\"b":2}')
        for (path, space, exp) in [
            ('.', None, '{"\\"a":1,"\\"b":2}'),
            ('.', ' ',  '{"\\"a": 1,"\\"b": 2}')
        ]:
            if space is None:
                assert exp == client.execute_command(
                    'JSON.GET', k1, path).decode()
            else:
                assert exp == client.execute_command(
                    'JSON.GET', k1, 'space', space, path).decode()

    def test_json_numincrby_jsonpath_and_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}')
        client.execute_command(
            'JSON.SET', k3, '.', '{"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}')

        # JSONPath: return an array of values.
        # If a value is not a number, its corresponding returned element is JSON null.
        # NOTE: The expected value has accounted for the outcome of previous commands on the same key.
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMINCRBY', k1, '$.a.*', '1',   '[]'),
            ('JSON.GET',       k1, '$.a.*', None,  '[]'),
            ('JSON.NUMINCRBY', k1, '$.b.*', '1',   '[2]'),
            ('JSON.GET',       k1, '$.b.*', None,  '[2]'),
            ('JSON.NUMINCRBY', k1, '$.b[*]', '1',  '[3]'),
            ('JSON.GET',       k1, '$.b[*]', None, '[3]'),
            ('JSON.NUMINCRBY', k1, '$.d.*', '1',   '[2,3,4]'),
            ('JSON.GET',       k1, '$.d.*', None,  '[2,3,4]'),
            ('JSON.NUMINCRBY', k1, '$.d[*]', '1',  '[3,4,5]'),
            ('JSON.GET',       k1, '$.d[*]', None, '[3,4,5]'),
            ('JSON.NUMINCRBY', k2, '$.a.*', '1',   '[]'),
            ('JSON.GET',       k2, '$.a.*', None,  '[]'),
            ('JSON.NUMINCRBY', k2, '$.b.*', '1',   '[2]'),
            ('JSON.GET',       k2, '$.b.*', None,  '[2]'),
            ('JSON.NUMINCRBY', k2, '$.d.*', '1',   '[2,3,4]'),
            ('JSON.GET',       k2, '$.d.*', None,  '[2,3,4]'),
            ('JSON.NUMINCRBY', k3, '$.a.*', '1',   '[null]'),
            ('JSON.GET',       k3, '$.a.*', None,  '["a"]'),
            ('JSON.NUMINCRBY', k3, '$.b.*', '1',   '[null,2]'),
            ('JSON.GET',       k3, '$.b.*', None,  '["a",2]'),
            ('JSON.NUMINCRBY', k3, '$.c.*', '1',   '[null,null]'),
            ('JSON.GET',       k3, '$.c.*', None,  '["a","b"]'),
            ('JSON.NUMINCRBY', k3, '$.d.*', '1',   '[2,null,4]'),
            ('JSON.GET',       k3, '$.d.*', None,  '[2,"b",4]')
        ]:
            if incr_num is not None:
                assert exp.encode() == client.execute_command(cmd, key, path, incr_num)
            else:
                assert exp.encode() == client.execute_command(cmd, key, path)

    def test_json_numincrby_legacy_path_and_wildcard(self):
        client = self.server.get_new_client()
        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}')
        client.execute_command(
            'JSON.SET', k3, '.', '{"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}')

        # Legacy path: return NONEXISTENT error if no value is selected
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMINCRBY', k1, '.a.*', '1', None),
            ('JSON.NUMINCRBY', k2, '.a.*', '1', None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    cmd, key, path, incr_num)
            assert self.error_class.is_nonexistent_error(str(e.value))
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    'JSON.GET', key, path)
            assert self.error_class.is_nonexistent_error(str(e.value))

        # Legacy path: return WRONGTYPE error if no number value is selected
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMINCRBY', k3, '.a.*', '1', None),
            ('JSON.NUMINCRBY', k3, '.c.*', '1', None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    cmd, key, path, incr_num)
            assert self.error_class.is_wrongtype_error(str(e.value))

        # Legacy path: return a single value, which is the last updated value.
        # NOTE: The expected value has accounted for the outcome of previous commands on the same key.
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMINCRBY', k1, '.b.*', '1',   '2'),
            ('JSON.GET',       k1, '.b.*', None,  '2'),
            ('JSON.NUMINCRBY', k1, '.b[*]', '1',  '3'),
            ('JSON.GET',       k1, '.b[*]', None, '3'),
            ('JSON.NUMINCRBY', k1, '.d.*', '1',   '4'),
            ('JSON.GET',       k1, '.d.*', None,  '2'),
            ('JSON.NUMINCRBY', k1, '.d[*]', '1',  '5'),
            ('JSON.GET',       k1, '.d[*]', None, '3'),
            ('JSON.NUMINCRBY', k2, '.b.*', '1',   '2'),
            ('JSON.GET',       k2, '.b.*', None,  '2'),
            ('JSON.NUMINCRBY', k2, '.d.*', '1',   '4'),
            ('JSON.GET',       k2, '.d.*', None,  '2'),
            ('JSON.NUMINCRBY', k3, '.b.*', '1',   '2'),
            ('JSON.GET',       k3, '.b.*', None,  '"a"'),
            ('JSON.NUMINCRBY', k3, '.d.*', '1',   '4'),
            ('JSON.GET',       k3, '.d.*', None,  '2')
        ]:
            if incr_num is not None:
                assert exp.encode() == client.execute_command(cmd, key, path, incr_num)
            else:
                assert exp.encode() == client.execute_command(cmd, key, path)

    def test_json_nummultby_jsonpath_and_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}')
        client.execute_command(
            'JSON.SET', k3, '.', '{"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}')

        # JSONPath: return an array of values.
        # If a value is not a number, its corresponding returned element is JSON null.
        # NOTE: The expected value has accounted for the outcome of previous commands on the same key.
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMMULTBY', k1, '$.a.*', '2',   '[]'),
            ('JSON.GET',       k1, '$.a.*', None,  '[]'),
            ('JSON.NUMMULTBY', k1, '$.b.*', '2',   '[2]'),
            ('JSON.GET',       k1, '$.b.*', None,  '[2]'),
            ('JSON.NUMMULTBY', k1, '$.b[*]', '2',  '[4]'),
            ('JSON.GET',       k1, '$.b[*]', None, '[4]'),
            ('JSON.NUMMULTBY', k1, '$.d.*', '2',   '[2,4,6]'),
            ('JSON.GET',       k1, '$.d.*', None,  '[2,4,6]'),
            ('JSON.NUMMULTBY', k1, '$.d[*]', '2',  '[4,8,12]'),
            ('JSON.GET',       k1, '$.d[*]', None, '[4,8,12]'),
            ('JSON.NUMMULTBY', k2, '$.a.*', '2',   '[]'),
            ('JSON.GET',       k2, '$.a.*', None,  '[]'),
            ('JSON.NUMMULTBY', k2, '$.b.*', '2',   '[2]'),
            ('JSON.GET',       k2, '$.b.*', None,  '[2]'),
            ('JSON.NUMMULTBY', k2, '$.d.*', '2',   '[2,4,6]'),
            ('JSON.GET',       k2, '$.d.*', None,  '[2,4,6]'),
            ('JSON.NUMMULTBY', k3, '$.a.*', '2',   '[null]'),
            ('JSON.GET',       k3, '$.a.*', None,  '["a"]'),
            ('JSON.NUMMULTBY', k3, '$.b.*', '2',   '[null,2]'),
            ('JSON.GET',       k3, '$.b.*', None,  '["a",2]'),
            ('JSON.NUMMULTBY', k3, '$.c.*', '2',   '[null,null]'),
            ('JSON.GET',       k3, '$.c.*', None,  '["a","b"]'),
            ('JSON.NUMMULTBY', k3, '$.d.*', '2',   '[2,null,6]'),
            ('JSON.GET',       k3, '$.d.*', None,  '[2,"b",6]')
        ]:
            if incr_num is not None:
                assert exp.encode() == client.execute_command(cmd, key, path, incr_num)
            else:
                assert exp.encode() == client.execute_command(cmd, key, path)

    def test_json_nummultby_legacy_path_and_wildcard(self):
        client = self.server.get_new_client()

        client.execute_command(
            'JSON.SET', k1, '.', '{"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}')
        client.execute_command(
            'JSON.SET', k2, '.', '{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}')
        client.execute_command(
            'JSON.SET', k3, '.', '{"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}')

        # Legacy path: return NONEXISTENT error if no value is selected
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMMULTBY', k1, '.a.*', '2', None),
            ('JSON.NUMMULTBY', k2, '.a.*', '2', None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    cmd, key, path, incr_num)
            assert self.error_class.is_nonexistent_error(str(e.value))
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    'JSON.GET', key, path)
            assert self.error_class.is_nonexistent_error(str(e.value))

        # Legacy path: return WRONGTYPE error if no number value is selected
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMMULTBY', k3, '.a.*', '2', None),
            ('JSON.NUMMULTBY', k3, '.c.*', '2', None)
        ]:
            with pytest.raises(ResponseError) as e:
                assert exp == client.execute_command(
                    cmd, key, path, incr_num)

            assert self.error_class.is_wrongtype_error(str(e.value))

        # Legacy path: return a single value, which is the last updated value.
        # NOTE: The expected value has accounted for the outcome of previous commands on the same key.
        for (cmd, key, path, incr_num, exp) in [
            ('JSON.NUMMULTBY', k1, '.b.*', '2',   '2'),
            ('JSON.GET',       k1, '.b.*', None,  '2'),
            ('JSON.NUMMULTBY', k1, '.b[*]', '2',  '4'),
            ('JSON.GET',       k1, '.b[*]', None, '4'),
            ('JSON.NUMMULTBY', k1, '.d.*', '2',   '6'),
            ('JSON.GET',       k1, '.d.*', None,  '2'),
            ('JSON.NUMMULTBY', k1, '.d[*]', '2',  '12'),
            ('JSON.GET',       k1, '.d[*]', None, '4'),
            ('JSON.NUMMULTBY', k2, '.b.*', '2',   '2'),
            ('JSON.GET',       k2, '.b.*', None,  '2'),
            ('JSON.NUMMULTBY', k2, '.d.*', '2',   '6'),
            ('JSON.GET',       k2, '.d.*', None,  '2'),
            ('JSON.NUMMULTBY', k3, '.b.*', '2',   '2'),
            ('JSON.GET',       k3, '.b.*', None,  '"a"'),
            ('JSON.NUMMULTBY', k3, '.d.*', '2',   '6'),
            ('JSON.GET',       k3, '.d.*', None,  '2')
        ]:
            if incr_num is not None:
                assert exp.encode() == client.execute_command(cmd, key, path, incr_num)
            else:
                assert exp.encode() == client.execute_command(cmd, key, path)

    def test_json_digest(self):
        client = self.server.get_new_client()
        orig_digest = client.debug_digest()
        assert orig_digest != 0
        client.execute_command("flushall")
        new_digest = client.debug_digest()
        assert int(new_digest) == 0

    def test_big_dup(self):
        client = self.server.get_new_client()
        # This test is to test memory leak

        for fle in glob.glob("data/*.json"):
            with open(fle, 'r') as file:
                self.data = file.read()
            logging.debug("File %s is size %d" % (fle, len(self.data)))
            b0 = client.info(JSON_INFO_METRICS_SECTION)[
                JSON_INFO_NAMES['total_memory_bytes']]
            try:
                client.execute_command(
                    "json.set x .", self.data)
            except:
                pass
            try:
                client.execute_command("json.del x .")
            except:
                pass
            b2 = client.info(JSON_INFO_METRICS_SECTION)[
                JSON_INFO_NAMES['total_memory_bytes']]
            assert b2 == b0

    def test_jsonpath_filter_expression(self):
        client = self.server.get_new_client()

        store = '''
        {
          "store": {
            "books": [
              {
                "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95
              },
              {
                "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99,
                "movies": [
                  {
                    "title": "Sword of Honour",
                    "realisator": {
                      "first_name": "Bill",
                      "last_name": "Anderson"
                    }
                  }
                ]
              },
              {
                "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 9
              },
              {
                "category": "fiction",
                "author": "J. R. R. Tolkien",
                "title": "The Lord of the Rings",
                "isbn": "0-395-19395-8",
                "price": 22.99
              }
            ],
            "bicycle": {
              "color": "red",
              "price": 19.95
            }
          }
        }
        '''
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', store)
        assert b'OK' == client.execute_command(
            'JSON.SET', k2, '.', '[1,2,3,4,5]')
        assert b'OK' == client.execute_command(
            'JSON.SET', k3, '.', '[true,false,true,false,null,1,2,3,4]')
        assert b'OK' == client.execute_command('JSON.SET', k4, '.',
                                               '{"books": [{"price":5,"sold":true,"in-stock":true,"title":"foo"}, {"price":15,"sold":false,"title":"abc"}]}')
        assert b'OK' == client.execute_command(
            'JSON.SET', k5, '.', '[1,2,3,4,5,6,7,8,9]')

        for (key, path, exp) in [
            (k1, '$.store.books[?(@.isbn)].price',
             b'[9,22.99]'),
            (k1, '$.store.books[?( @.isbn )].price',
             b'[9,22.99]'),
            (k1, '$.store.books[?(@["isbn"])]["price"]',
             b'[9,22.99]'),
            (k1, '$.store.books[?(@[ "isbn" ])][ "price" ]',
             b'[9,22.99]'),
            (k1, '$.store.books[?(@[\'isbn\'])][\'price\']',
             b'[9,22.99]'),
            (k1,
             '$.store.books[?(@.category == "reference")].price',      b'[8.95]'),
            (k1, '$.store.books[?(@.["category"] == "fiction")].price',
             b'[12.99,9,22.99]'),
            (k1, '$.store.books[?(@.price<1.0E1)].price',
             b'[8.95,9]'),
            (k1, '$.store.books[?(@["price"]<1.0E1)]["price"]',
             b'[8.95,9]'),
            (k1, '$.store.books[?(@.["price"]<1.0E1)]["price"]',
             b'[8.95,9]'),
            (k1, '$.store.books[?(@[\'price\']<1.0E1)][\'price\']',
             b'[8.95,9]'),
            (k1,
             '$.store.books[?(@.price>-1.23e1&&@.price<1.0E1)].price', b'[8.95,9]'),
            (k1, '$.store.books[?(@["price"]>-1.23e1&&@["price"]<1.0E1)]["price"]',
             b'[8.95,9]'),
            (k1, '$.store.books[?(@.["price"]>-1.23e1&&@.["price"]<1.0E1)].["price"]',    b'[8.95,9]'),
            (k1, '$.store.books[?(@["price"] > -1.23e1 && @["price"] < 1.0E1)]["price"]', b'[8.95,9]'),
            (k1, '$.store.books[?(@.price==22.99)].title',
             b'["The Lord of the Rings"]'),
            (k1, '$.store.books[?(@["price"]==22.99)].title',
             b'["The Lord of the Rings"]'),
            (k1,
             '$.store.books[?(@.price<10.0&&@.isbn)].price',           b'[9]'),
            (k1, '$.store.books[?(@.price<9||@.price>20)].price',
             b'[8.95,22.99]'),
            # precedence test
            (k1, '$.store.books[?(@.price<9||@.price>10&&@.isbn)].price',
             b'[8.95,22.99]'),
            # precedence test
            (k1,
             '$.store.books[?((@.price<9||@.price>10)&&@.isbn)].price', b'[22.99]'),
            # precedence test
            (k1,
             '$.store.books[?((@.price < 9 || @.price>10) && @.isbn)].price', b'[22.99]'),
            # precedence test
            (k1, '$.store.books[?((@["price"]<9||@["price"]>10)&&@["isbn"])]["price"]', b'[22.99]'),
            # precedence test
            (k1, '$.store.books[?((@["price"] < 9 || @["price"] > 10) && @["isbn"])]["price"]', b'[22.99]'),
            (k2, '$.*.[?(@>2)]',
             b'[3,4,5]'),
            (k2, '$.*.[?(@ > 2)]',
             b'[3,4,5]'),
            (k2, '$.*[?(@>2)]',
             b'[3,4,5]'),
            (k2, '$[*][?(@>2)]',
             b'[3,4,5]'),
            (k2, '$[ * ][?( @ > 2 )]',
             b'[3,4,5]'),
            (k2, '$.*[?(@ == 3)]',
             b'[3]'),
            (k2, '$.*[?(@ != 3)]',
             b'[1,2,4,5]'),
            (k3, '$.*.[?(@==true)]',
             b'[true,true]'),
            (k3, '$[*][?(@==true)]',
             b'[true,true]'),
            (k3, '$[*][?(@ == true)]',
             b'[true,true]'),
            (k3, '$.*.[?(@>1)]',
             b'[2,3,4]'),
            (k3, '$.*.[?( @ > 1 ) ]',
             b'[2,3,4]'),
            (k3, '$[*][?(@>1)]',
             b'[2,3,4]'),
            (k3, '$[ * ][?( @ > 1 ) ]',
             b'[2,3,4]'),
            (k4, '$.books[?(@.price>1&&@.price<20&&@.in-stock)]',
             b'[{"price":5,"sold":true,"in-stock":true,"title":"foo"}]'),
            (k4, '$.books[?(@[\'price\']>1&&@.price<20&&@["in-stock"])]',
             b'[{"price":5,"sold":true,"in-stock":true,"title":"foo"}]'),
            (k4, '$.books[?((@.price>1&&@.price<20)&&(@.sold==false))]',
             b'[{"price":15,"sold":false,"title":"abc"}]'),
            (k4, '$["books"][?((@["price"]>1&&@["price"]<20)&&(@["sold"]==false))]',
             b'[{"price":15,"sold":false,"title":"abc"}]'),
            (k5, '$.*[?(@ > 7 || @ < 3)]',
             b'[8,9,1,2]'),   # order test
            (k5, '$.*[?(@ < 3 || @ > 7)]',
             b'[1,2,8,9]'),   # order test
            (k5, '$.*[?(@ > 3 && @ < 7)]',
             b'[4,5,6]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '$.store.books[?(@.price<10.0)].price', '10.01')
        for (key, path, exp) in [
            (k1, '$.store.books[?(@.price<10.0)]',            b'[]'),
            (k1, '$.store.books[?(@.price<=10.02)].price',
             b'[10.01,10.01]'),
            (k1, '$.store.books[?(@.price <= 10.02)].price',
             b'[10.01,10.01]'),
            (k1, '$.store.books[?(@.price==10.01)].title',
             b'["Sayings of the Century","Moby Dick"]'),
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        for (key, path, new_val, exp) in [
            (k4, '$.books[?((@.price>1&&@.price<20)&&(@.sold==false))].price',
             '13.13', b'[13.13]'),
            (k4, '$.books[?((@.price > 1 && @.price < 20) && (@.sold == false))].price',
             '13.13', b'[13.13]'),
            (k4, '$["books"][?((@["price"]>1&&@["price"]<20)&&(@["sold"]==false))]["price"]',
             '13.13', b'[13.13]'),
            (k4, '$["books"][?((@["price"] > 1 && @["price"] < 20) && (@["sold"] == false))]["price"]', '13.13', b'[13.13]')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, path, new_val)
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        # test delete with filter expression
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', store)
        assert b'OK' == client.execute_command(
            'JSON.SET', k2, '.', store)
        for (key, path, exp) in [
            (k1, '$.store.books[?(@.["category"] == "fiction")].price', 3),
            (k2, '$.store.books[?((@["price"] < 9 || @["price"] > 10) && @["isbn"])]["price"]', 1)
        ]:
            assert exp == client.execute_command(
                'JSON.DEL', key, path)
            assert b'[]' == client.execute_command(
                'JSON.GET', key, path)

    def test_jsonpath_recursive_descent(self):
        client = self.server.get_new_client()

        for (key, val) in [
            (k1, '{"a":{"a":1}}'),
            (k2, '{"a":{"a":{"a":{"a":1}}}}'),
            (k3, '{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}'),
            (k4, '{"a":{"b":{"z":{"y":1}}, "c":{"z":{"y":2}}, "z":{"y":3}}}'),
            (k5, '{"a":1, "b": {"e":[0,1,2]}, "c":{"e":[10,11,12]}}'),
            (k6, '{"a":[1], "b": {"a": [2,3]}, "c": {"a": [4,5,6]}}')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', val)

        for (key, path, exp) in [
            (k1, '$..a',          b'[{"a":1},1]'),
            (k2, '$..a',
             b'[{"a":{"a":{"a":1}}},{"a":{"a":1}},{"a":1},1]'),
            (k2, '$..a..a',       b'[{"a":{"a":1}},{"a":1},1]'),
            (k2, '$..a..a..a',    b'[{"a":1},1]'),
            (k2, '$..a..a..a..a', b'[1]'),
            (k2, '$..a..a..a..a..a', b'[]'),
            (k3, '$..a',          b'["a",""]'),
            (k4, '$.a..z.y',      b'[3,1,2]'),
            (k4, '$.a..z.*',      b'[3,1,2]'),
            (k4, '$.a.*..y',      b'[1,2,3]'),
            (k5, '$..e.[*]',      b'[0,1,2,10,11,12]'),
            (k5, '$..e[1]',       b'[1,11]'),
            (k5, '$..e.[1]',      b'[1,11]'),
            (k5, '$..e.[1]',      b'[1,11]'),
            (k5, '$..e[0:2]',     b'[0,1,10,11]'),
            (k5, '$..["e"][1]',   b'[1,11]'),
            (k5, '$..["e"][1]',   b'[1,11]'),
            (k5, '$..["e"][0:2]', b'[0,1,10,11]'),
            (k5, '$..[ "e" ][ 0 : 2 ]', b'[0,1,10,11]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        # recursive ARRAPPEND
        assert [2, 3, 4] == client.execute_command(
            'JSON.ARRAPPEND', k6, '$..a', 0)
        assert b'{"a":[1,0],"b":{"a":[2,3,0]},"c":{"a":[4,5,6,0]}}' == client.execute_command(
            'JSON.GET', k6)

        # recursive delete
        assert 2 == client.execute_command(
            'JSON.DEL', k3, '$..a')
        assert b'{"x":{},"y":{},"z":{"b":"b"}}' == client.execute_command(
            'JSON.GET', k3)

        # This is the only case that diverges from ReJSON v2. We deleted 4 elements while they deleted 1.
        # The divergence comes from the order of deletion.
        # Note that "JSON.GET k2 $..a" returns 4 elements.
        assert 4 == client.execute_command(
            'JSON.DEL', k2, '$..a')
        assert b'{}' == client.execute_command('JSON.GET', k2)

    def test_jsonpath_recursive_insert_update_delete(self):
        '''
        Test recursive insert, update and delete.
        '''
        client = self.server.get_new_client()
        data_store = '''
        {
          "store": {
            "books": [
              {
                "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95,
                "in-stock": true
              },
              {
                "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99,
                "in-stock": true,
                "movies": [
                  {
                    "title": "Sword of Honour - movie",
                    "realisator": {
                      "first_name": "Bill",
                      "last_name": "Anderson"
                    }
                  }
                ]
              },
              {
                "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 9,
                "in-stock": false
              },
              {
                "category": "fiction",
                "author": "J. R. R. Tolkien",
                "title": "The Lord of the Rings",
                "isbn": "0-115-03266-2",
                "price": 22.99,
                "in-stock": true
              },
              {
                "category": "reference",
                "author": "William Jr. Strunk",
                "title": "The Elements of Style",
                "price": 6.99,
                "in-stock": false
              },
              {
                "category": "fiction",
                "author": "Leo Tolstoy",
                "title": "Anna Karenina",
                "price": 22.99,
                "in-stock": true
              },
              {
                "category": "reference",
                "author": "Sarah Janssen",
                "title": "The World Almanac and Book of Facts 2021",
                "isbn": "0-925-23305-2",
                "price": 10.69,
                "in-stock": false
              },
              {
                "category": "reference",
                "author": "Kate L. Turabian",
                "title": "Manual for Writers of Research Papers",
                "isbn": "0-675-16695-1",
                "price": 8.59,
                "in-stock": true
              }
            ],
            "bicycle": {
              "color": "red",
              "price": 19.64,
              "in-stock": true
            }
          }
        }
        '''
        data_store2 = '''
        {
            "store": {
                "title": "foo",
                "bicycle": {
                    "title": "foo2",
                    "color": "red",
                    "price": 19.64,
                    "in-stock": true
                }
            }
        }
        '''
        for (key, val) in [
            (store, data_store),
            (store2, data_store2)
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', val)

        # recursive delete
        assert 2 == client.execute_command(
            'JSON.DEL', store2, '$..title')
        assert b'[]' == client.execute_command(
            'JSON.GET', store2, '$..title')

        assert b'{"store":{"bicycle":{"color":"red","price":19.64,"in-stock":true}}}' == client.execute_command(
            'JSON.GET', store2)

        # recursive insert, update and delete
        assert b'["Sayings of the Century","Sword of Honour","Sword of Honour - movie","Moby Dick","The Lord of the Rings","The Elements of Style","Anna Karenina","The World Almanac and Book of Facts 2021","Manual for Writers of Research Papers"]'\
               == client.execute_command('JSON.GET', store, '$..title')
        assert b'OK' == client.execute_command(
            'JSON.SET', store, '$..title', '"foo"')
        assert b'["foo","foo","foo","foo","foo","foo","foo","foo","foo"]'\
               == client.execute_command('JSON.GET', store, '$..title')
        for (key, path, exp) in [
            (store, '$.title',                                     b'[]'),
            (store, '$.store.title',                               b'[]'),
            (store, '$.store.bicycle.title',                       b'[]'),
            (store, '$.store.books[1].title',                      b'["foo"]'),
            (store, '$.store.books[1].movies[0].title',            b'["foo"]'),
            (store, '$.store.books[1].movies[0].realisator.title', b'[]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)
        assert 9 == client.execute_command(
            'JSON.DEL', store, '$..title')
        assert b'[]' == client.execute_command(
            'JSON.GET', store, '$..title')
        assert True == client.execute_command('save')

    def test_jsonpath_recursive_insert_update_delete2(self):
        '''
        Test recursive insert, update and delete.
        '''
        client = self.server.get_new_client()
        data_input = '''
        {
            "input": {
                "a": 1, 
                "b": {
                    "e": [0,1,2]
                }, 
                "c": {
                    "e": [10,11,12]
                }
            }
        }
        '''
        data_input2 = '''
        {
            "input": {
                "a": 1, 
                "b": {
                    "e": [0,1,2]
                }, 
                "c": {
                    "e": [10,11,12]
                }
            }
        }
        '''
        for (key, val) in [
            (input, data_input),
            (input2, data_input2)
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', val)

        # recursive delete
        assert 2 == client.execute_command(
            'JSON.DEL', input2, '$..e')
        assert b'[]' == client.execute_command(
            'JSON.GET', input2, '$..e')
        assert b'{"input":{"a":1,"b":{},"c":{}}}' == client.execute_command(
            'JSON.GET', input2)

        # recursive insert, update and delete
        assert b'[0,1,2,10,11,12]' == client.execute_command(
            'JSON.GET', input, '$..e[*]')
        assert b'OK' == client.execute_command(
            'JSON.SET', input, '$..e[*]', '4')
        assert b'[4,4,4,4,4,4]' == client.execute_command(
            'JSON.GET', input, '$..e[*]')
        assert b'[4,4,4]' == client.execute_command(
            'JSON.GET', input, '$.input.b.e[*]')
        for (key, path, exp) in [
            (input, '$.e',            b'[]'),
            (input, '$.input.e',      b'[]'),
            (input, '$.input.a.e',    b'[]'),
            (input, '$.input.b.e[*]', b'[4,4,4]'),
            (input, '$.input.c.e[*]', b'[4,4,4]'),
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)
        assert 2 == client.execute_command(
            'JSON.DEL', input, '$..e')
        assert 0 == client.execute_command(
            'JSON.DEL', input, '$..e')
        assert b'[]' == client.execute_command(
            'JSON.GET', input, '$..e')
        assert True == client.execute_command('save')

    def test_jsonpath_compatibility_invalidArrayIndex(self):
        client = self.server.get_new_client()

        # Array index is not integer
        for (key, path) in [
            (wikipedia, '.phoneNumbers[]'),
            (wikipedia, '.phoneNumbers[x]'),
            (wikipedia, '$.phoneNumbers[]'),
            (wikipedia, '$.phoneNumbers[x]')
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command('JSON.GET', key, path)
            assert self.error_class.is_syntax_error(str(e.value))

    def test_jsonpath_compatibility_unquotedMemberName(self):
        client = self.server.get_new_client()

        # Unquoted member name can contain any symbol except terminator characters
        json = '''{
            "%x22key%x22":1, "+2":2, "-3":3, "/4":4,
            ")5":5, "6)":6, "(7":7, "8(":8,
            "]9":9, "10]":10, "[11":11, "12[":12,
            ">13":13, "14>":14, "<15":15, "16<":16,
            "=17":17, "18=":18, "!19":19, "20!":20,
            "21.21":21
        }'''
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', json)

        test_cases = [
            (k1, '$.%x22key%x22',  b'[1]'),
            (k1, '$.+2',           b'[2]'),
            (k1, '$.-3',           b'[3]'),
            (k1, '$./4',           b'[4]'),
            # The following should return empty array because unquoted member name cannot contain terminator characters.
            (k1, '$.6)',           b'[]'),
            (k1, '$.8(',           b'[]'),
            (k1, '$.10]',          b'[]'),
            # Bracketed/Quoted member name should work
            (k1, '$["6)"]',        b'[6]'),
            (k1, '$["8("]',        b'[8]'),
            (k1, '$["10]"]',       b'[10]'),
            (k1, '$["12["]',       b'[12]'),
            (k1, '$["14>"]',       b'[14]'),
            (k1, '$["16<"]',       b'[16]'),
            (k1, '$["18="]',       b'[18]'),
            (k1, '$["20!"]',       b'[20]'),
            (k1, '$["21.21"]',     b'[21]'),
            (k1, '$.12[',          b'[]'),
            (k1, '$.14>',          b'[]'),
            (k1, '$.16<',          b'[]'),
            (k1, '$.18=',          b'[]'),
            (k1, '$.20!',          b'[]'),
            (k1, '$.21.21',        b'[]'),
        ]

        for (key, path, exp) in test_cases:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        # Unquoted object member cannot start with a character that is a member name terminator.
        for (key, path) in [
            (k1, '$.)5'),
            (k1, '$.]7'),
            (k1, '$.]9'),
            (k1, '$.[11'),
            (k1, '$.>13'),
            (k1, '$.<15'),
            (k1, '$.=17'),
            (k1, '$.!19')
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command('JSON.GET', key, path)
            assert self.error_class.is_syntax_error(str(e.value))

    def test_write_commands_duplicate_values(self):
        '''
        Test all WRITE JSON commands for the situation that if the json path results in multiple values
        with duplicates, the write operation should apply to all unique values once.
        '''
        client = self.server.get_new_client()

        for (key, val) in [
            (k1,  '[0,1,2,3,4,5,6,7,8,9]'),
            (k2,  '[0,1,2,3,4,5,6,7,8,9]'),
            (k3,  '[0,1,2,3,4,5,6,7,8,9]'),
            (k4,  '["0","1","2","3","4","5","6","7","8","9"]'),
            (k5,  '[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]'),
            (k6,  '[true,true,true,true,true]'),
            (k7,  '[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]'),
            (k8,  '[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]'),
            (k9,  '[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]'),
            (k10, '[[0,1,2,3,4],[0,1,2,3,4],[0,1,2,3,4]]'),
            (k11, '[0,1,2,3,4,5,6,7,8,9]'),
            (k12, '[0,1,2,3,4,5,6,7,8,9]')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', val)

        # NUMINCRBY
        assert b'[10]' == client.execute_command(
            'JSON.NUMINCRBY', k1, '$[0,0,0,0,0]', 10)
        assert b'[10,1,2,3,4,5,6,7,8,9]' == client.execute_command(
            'JSON.GET', k1)

        # NUMMULTBY
        assert b'[18]' == client.execute_command(
            'JSON.NUMMULTBY', k2, '$[9,9,9,9,9]', 2)
        assert b'[0,1,2,3,4,5,6,7,8,18]' == client.execute_command(
            'JSON.GET', k2)

        # CLEAR
        assert 1 == client.execute_command(
            'JSON.CLEAR', k5, '$[0,0,0,0,0]')
        assert b'[[],[1],[2],[3],[4],[5],[6],[7],[8],[9]]' == client.execute_command(
            'JSON.GET', k5)

        # TOGGLE
        assert [0] == client.execute_command(
            'JSON.TOGGLE', k6, '$[0,0,0,0]')
        assert b'[false,true,true,true,true]' == client.execute_command(
            'JSON.GET', k6)

        # STRAPPEND
        assert [4] == client.execute_command(
            'JSON.STRAPPEND', k4, '$[0,0,0,0,0]', '"foo"')
        assert b'["0foo","1","2","3","4","5","6","7","8","9"]' == client.execute_command(
            'JSON.GET', k4)

        # ARRAPPEND
        assert [3] == client.execute_command(
            'JSON.ARRAPPEND', k7, '$[0,0,0,0,0]', 8, 9)
        assert b'[[0,8,9],[1],[2],[3],[4],[5],[6],[7],[8],[9]]' == client.execute_command(
            'JSON.GET', k7)

        # ARRINSERT
        assert [2] == client.execute_command(
            'JSON.ARRINSERT', k8, '$[0,0,0,0,0]', 0, 9)
        assert b'[[9,0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]' == client.execute_command(
            'JSON.GET', k8)

        # ARRPOP
        assert [b"0"] == client.execute_command(
            'JSON.ARRPOP', k9, '$[0,0,0,0,0]')
        assert b'[[],[1],[2],[3],[4],[5],[6],[7],[8],[9]]' == client.execute_command(
            'JSON.GET', k9)

        # ARRTRIM
        assert [2] == client.execute_command(
            'JSON.ARRTRIM', k10, '$[0,0,0,0,0]', 0, 1)
        assert b'[[0,1],[0,1,2,3,4],[0,1,2,3,4]]' == client.execute_command(
            'JSON.GET', k10)

        # DEL
        assert 1 == client.execute_command(
            'JSON.DEL', k3, '$[0,0,0,0,0]')
        assert b'[1,2,3,4,5,6,7,8,9]' == client.execute_command(
            'JSON.GET', k3)

        # DEL: delete arbitrary elements with duplicates
        assert 5 == client.execute_command(
            'JSON.DEL', k12, '$[1,4,7,0,0,3,3]')
        assert b'[2,5,6,8,9]' == client.execute_command(
            'JSON.GET', k12)

        # SET
        assert b'OK' == client.execute_command(
            'JSON.SET', k11, '$[0,0,0,0,0]', 9)
        assert b'[9,1,2,3,4,5,6,7,8,9]' == client.execute_command(
            'JSON.GET', k11)

    def test_jsonpath_compatibility_filterOnObject_and_stringComparison(self):
        client = self.server.get_new_client()

        json = '''
            {  
                "key for key"     : "key inside here",
                "an object"       : {
                  "weight"  : 300,
                  "a value" : 300,
                  "my key"  : "key inside here"
                },
                "another object" : {
                  "weight"  : 400,
                  "a value" : 400,
                  "my key"  : "key inside there"
                },
                "objects": [
                    {
                        "weight"  : 100,
                        "a value" : 100,
                        "my key"  : "key inside here"
                    },
                    {
                        "weight"  : 200,
                        "a value" : 200,
                        "my key"  : "key inside there"
                    },
                    {
                        "weight"  : 300,
                        "a value" : 300,
                        "my key"  : "key inside here"
                    },
                    {
                        "weight"  : 400,
                        "a value" : 400,
                        "my key"  : "key inside there"
                    }
                ]
            }
        '''
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '.', json)

        for (key, path, exp) in [
            (k1, '$["an object"].[?(@.weight > 200)].["a value"]',
             b'[300]'),
            (k1, '$["an object"].[?(@.weight == 300)].["a value"]',
             b'[300]'),
            (k1, '$["an object"].[?(@.weight > 300)].["a value"]',
             b'[]'),
            (k1, '$["another object"].[?(@["a value"] > 200)].weight',
             b'[400]'),
            (k1, '$["another object"].[?(@.["a value"] > 200)].weight',
             b'[400]'),
            (k1, '$["another object"].[?(@.["my key"] == "key inside there")].weight', b'[400]'),
            (k1, '$["objects"].[?(@.weight > 200)].["a value"]',
             b'[300,400]'),
            (k1, '$["objects"].[?(@.["my key"] == "key inside there")].weight',
             b'[200,400]'),
            (k1, '$["objects"].[?(@.["my key"] != "key inside there")].weight',
             b'[100,300]'),
            (k1, '$["objects"].[?(@.["my key"] == "key inside here")].weight',
             b'[100,300]'),
            (k1, '$["objects"].[?(@.["my key"] != "key inside here")].weight',
             b'[200,400]'),
            (k1, '$["objects"].[?(@.["my key"] <= "key inside here")].weight',
             b'[100,300]'),
            (k1, '$["objects"].[?(@.["my key"] >= "key inside here")].weight',
             b'[100,200,300,400]'),
            (k1, '$["objects"].[?(@.["my key"] < "key inside herf")].weight',
             b'[100,300]'),
            (k1, '$["objects"].[?(@.["my key"] > "key insidd here")].weight',
             b'[100,200,300,400]'),
            (k1, '$["key for key"].[?(@.["a value"] > 200)]',
             b'[]')
        ]:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

    def test_jsonpath_compatibility_union_of_object_members(self):
        client = self.server.get_new_client()

        test_cases = [
            (wikipedia, '$.["firstName","lastName"]',
             b'["John","Smith"]'),
            (organism,
             '$.animals["2","2"].mammals..weight',                 b'[]'),
            (organism,  '$.animals["2","Junk"]',
             b'[]'),
            (organism,  '$.animals["Junk","Junk"]',
             b'[]'),
            # test unique values in recursive descent
            (organism,  '$..[?(@.weight>400)].name',
             b'["Redwood","Horse"]'),
            (organism,  '$..[?(@.weight>=60&&@.name=="Chimpanzee")].name',
             b'["Chimpanzee"]'),
            (wikipedia, '$.[ "firstName", "lastName" ]',
             b'["John","Smith"]'),
            (wikipedia, '$.address.[ "street", "city", "state", "zipcode" ]',
             b'["21 2nd Street","New York","NY","10021-3100"]'),
        ]

        assert b'OK' == client.execute_command(
            'JSON.SET', organism, '.', DATA_ORGANISM)

        for (key, path, exp) in test_cases:
            assert exp == self.client.execute_command('JSON.GET', key, path)

    def test_jsonpath_malformed_path(self):
        client = self.server.get_new_client()

        for (key, val) in [
            (k1, '{"store":{"book":[{"price":5,"sold":true,"in-stock":true,"title":"foo","author":"me","isbn":"978-3-16-148410-0"}]}}'),
            (k2, '[1,2,3,4,5,6,7,8,9,10]')
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '.', val)

        test_cases = [
            (k1, '$[0:2]$[0:1]$[0:2]$[0:2]$[0<2065>:2]$[0:2]', b'[]'),
            (k1, '$[0,1]',                                     b'[]'),
            (k2, '$.[\"a\"].[\"b\"].[\"c\"]',                  b'[]'),
            (k1, '$a.b.c.d',                                   b'[]'),
            (k2, '$a$b$c$d',                                   b'[]'),
        ]

        for (key, path, exp) in test_cases:
            assert exp == client.execute_command(
                'JSON.GET', key, path)

        for (key, path) in [
            (k1, '.[0:2].[0:1].[0:2].[0:2].[0<2065>:2].[0:2]'),
            (k1, '.[0:2]$[0:1]$[0:2]$[0:2]$[0<2065>:2]$[0:2]')
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command('JSON.GET', key, path)
            assert self.error_class.is_wrongtype_error(
                str(e.value)) or self.error_class.is_nonexistent_error(str(e.value))

        for (key, path) in [
            (k1, '&&$.store..price'),
            (k1, '!$.store..price'),
            (k1, '=$.store..price'),
            (k1, '=.store..price'),
            (k1, '||.store..price')
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command('JSON.GET', key, path)
            assert self.error_class.is_syntax_error(str(e.value))
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'JSON.SET', key, path, '0')
            assert self.error_class.is_syntax_error(str(e.value))

    def test_v2_path_limit_recursive_descent(self):
        client = self.server.get_new_client()

        depth_limit = 10
        client.execute_command(
            'config set json.max-path-limit ' + str(depth_limit))

        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '$', '{"a":0}')

        # repeatedly increasing path depth by 1 till the limit is reached.
        for _ in range(0, depth_limit-1):
            client.execute_command(
                'JSON.SET', k1, '$..a', '{"a":0}')
        # verify the path depth reaches the limit
        assert depth_limit == client.execute_command(
            'JSON.DEBUG', 'DEPTH', k1)

        # one more increase should exceed the path limit
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', k1, '$..a', '{"a":0}')
        assert str(e.value).startswith("LIMIT")

    def test_v2_path_limit_insert_member(self):
        client = self.server.get_new_client()

        depth_limit = 3
        client.config_set('json.max-path-limit', depth_limit)

        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '$', '{"a":{"a":{"a":0}}}')
        assert depth_limit == client.execute_command(
            'JSON.DEBUG DEPTH', k1)

        # insert a nesting object
        with pytest.raises(ResponseError) as e:
            client.execute_command(
                'JSON.SET', k1, '$.a.a.b', '{"b":0}')
        assert str(e.value).startswith("LIMIT")

        # increase limit by 1
        client.config_set(
            'json.max-path-limit', depth_limit + 1)
        assert b'OK' == client.execute_command(
            'JSON.SET', k1, '$.a.a.b', '{"b":0}')

    def test_debug_command_getkeys_api(self):
        client = self.server.get_new_client()

        for (subcmd, res) in [
            ('memory k1', [b'k1']),
            ('fields k1', [b'k1']),
            ('depth k1',  [b'k1']),
            ('memory k1', [b'k1'])
        ]:
            assert res == client.execute_command(
                'command getkeys json.debug ' + subcmd)

        for subcmd in [
            '',
            'memory',
            'mem',
            'memo'
            'fields'
            'depth'
            'help',
            'max-depth-key',
            'max-size-key'
        ]:
            with pytest.raises(ResponseError) as e:
                client.execute_command(
                    'command getkeys json.debug ' + subcmd)
            assert self.error_class.is_wrong_number_of_arguments_error(str(e.value)) or \
                str(e.value).lower().find("invalid command") >= 0 or \
                str(e.value).lower().find(
                    "the command has no key arguments") >= 0

    def test_debug_command_depth(self):
        client = self.server.get_new_client()
        for (key, val, depth) in [
            (k1, '1',                     0),
            (k2, '"a"',                   0),
            (k3, 'null',                  0),
            (k4, 'true',                  0),
            (k5, '{}',                    0),
            (k6, '{"a":0}',               1),
            (k7, '{"a":{"a":0}}',         2),
            (k7, '{"a":{"a":{"a":0}}}',   3),
            (k8, '[]',                    0),
            (k9, '[0]',                   1),
            (k10, '[[0]]',                2),
            (k11, '[[0],[[0]]]',          3),
        ]:
            assert b'OK' == client.execute_command(
                'JSON.SET', key, '$', val)
            assert depth == client.execute_command(
                'JSON.DEBUG DEPTH', key)

        # what if the key does not exist?
        assert None == client.execute_command(
            'JSON.DEBUG DEPTH', 'foobar')

    def test_insert_update_delete_mode(self):
        client = self.server.get_new_client()
        assert b'OK' == client.execute_command(
            'JSON.SET', organism, '.', DATA_ORGANISM)
        assert b'OK' == client.execute_command(
            'JSON.SET', organism2, '.', DATA_ORGANISM)

        # delete
        key = organism
        path = '$.animals[*].mammals[*].primates[*].apes[?(@.weight<400)].weight'
        assert b'[130,300]' == client.execute_command(
            'JSON.GET', key, path)
        assert 2 == client.execute_command(
            'JSON.DEL', key, path)
        assert b'[]' == client.execute_command(
            'JSON.GET', key, path)

        # insert

        assert b'OK' == client.execute_command(
            'JSON.SET', key, path, 25)
        assert b'[]' == client.execute_command(
            'JSON.GET', key, path)

        # update
        key = organism2
        assert b'[130,300]' == client.execute_command(
            'JSON.GET', key, path)
        assert b'OK' == client.execute_command(
            'JSON.SET', key, path, 25)
        assert b'[25,25]' == client.execute_command(
            'JSON.GET', key, path)
        assert 2 == client.execute_command(
            'JSON.DEL', key, path)
        assert b'[]' == client.execute_command(
            'JSON.GET', key, path)

    def test_json_arity_per_command(self):
        client = self.server.get_new_client()

        # These commands should only get the single key
        cmd_arity = [('SET', -4), ('GET', -2), ('MGET', -3), ('DEL', -2), ('FORGET', -2), ('NUMINCRBY', 4),
                     ('NUMMULTBY', 4), ('STRLEN', -2), ('STRAPPEND', -
                                                        3), ('TOGGLE', -2), ('OBJLEN', -2), ('OBJKEYS', -2),
                     ('ARRLEN', -2), ('ARRAPPEND', -4), ('ARRPOP', -
                                                         2), ('ARRINSERT', -5), ('ARRTRIM', 5), ('CLEAR', -2),
                     ('ARRINDEX', -4), ('TYPE', -2), ('RESP', -2), ('DEBUG', -2)]

        for cmd, arity in cmd_arity:
            assert arity == client.execute_command('COMMAND', 'INFO', f'JSON.{cmd}')[
                f'JSON.{cmd}']['arity']

        cmd_arity = [('MEMORY', -3), ('FIELDS', -3), ('DEPTH', 3), ('HELP', 2),
                     ('MAX-DEPTH-KEY', 2), ('MAX-SIZE-KEY',
                                            2), ('KEYTABLE-CHECK', 2), ('KEYTABLE-CORRUPT', 3),
                     ('KEYTABLE-DISTRIBUTION', 3)]
        subcmd_dict = {f'JSON.DEBUG|{cmd}': arity for cmd, arity in cmd_arity}

        output = client.execute_command(
            'COMMAND', 'INFO', f'JSON.DEBUG')[f'JSON.DEBUG']['subcommands']
        assert len(output) == len(subcmd_dict)

        for i in range(len(output)):
            assert subcmd_dict[output[i][0].decode('ascii')] == output[i][1]

    def test_hashtable_insert_and_remove(self):
        client = self.server.get_new_client()

        def make_path(i):
            return '$.' + str(i)

        def make_array(sz, offset):
            data = []
            for i in range(sz):
                data.append(str(i + offset))
            return data

        def make_array_array(p, q):
            data = make_array(p, 0)
            for i in range(p):
                data[i] = make_array(q, i)
            return data

        def make_string(i):
            return f"string value {i}"

        # set json.hash-table-min-size
        client.execute_command(
            'config set json.hash-table-min-size 5')

        for sz in [10, 50, 100]:
            for type in ['array_array', 'array', 'string']:
                client.execute_command(
                    'json.set', k1, '.', '{}')

                # insert object members
                for i in range(sz):
                    if type == 'array_array':
                        v = make_array_array(i, i)
                    elif type == 'array':
                        v = make_array(i, i)
                    else:
                        v = make_string(i)
                    client.execute_command(
                        'json.set', k1, make_path(i), f'{json.dumps(v)}')

                # delete object members
                for i in range(sz):
                    client.execute_command(
                        'json.del', k1, make_path(i))
