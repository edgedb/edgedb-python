#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import pathlib
import unittest

from edgedb import credentials


class TestCredentials(unittest.TestCase):

    def test_credentials_read(self):
        creds = credentials.read_credentials(
            pathlib.Path(__file__).parent / 'credentials1.json')
        self.assertEqual(creds, {
            'database': 'test3n',
            'password': 'lZTBy1RVCfOpBAOwSCwIyBIR',
            'port': 10702,
            'user': 'test3n',
        })

    def test_credentials_empty(self):
        with self.assertRaisesRegex(ValueError, '`user` key is required'):
            credentials.validate_credentials({})

    def test_credentials_port(self):
        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': '1234',
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': 0,
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': -1,
            })

        with self.assertRaisesRegex(ValueError, 'invalid `port` value'):
            credentials.validate_credentials({
                'user': 'u1',
                'port': 65536,
            })

    def test_credentials_extra_key(self):
        creds = credentials.validate_credentials(dict(
            user='user1',
            some_extra_data='test',
        ))
        # extra keys are ignored for forward compatibility
        # but aren't exported through validator
        self.assertEqual(creds, {"user": "user1", "port": 5656})
