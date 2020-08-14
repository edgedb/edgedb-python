import unittest
from edgedb import credentials


class TestCredentials(unittest.TestCase):

    def test_read(self):
        creds = credentials.read_credentials('tests/credentials1.json')
        self.assertEqual(creds, {
            'database': 'test3n',
            'password': 'lZTBy1RVCfOpBAOwSCwIyBIR',
            'port': 10702,
            'user': 'test3n',
        })

    def test_empty(self):
        with self.assertRaisesRegex(ValueError, '`user` key is required'):
            credentials.validate_credentials({})

    def test_port(self):
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

    def test_extra_key(self):
        creds = credentials.validate_credentials(dict(
            user='user1',
            some_extra_data='test',
        ))
        # extra keys are ignored for forward compatibility
        # but aren't exported through validator
        self.assertEqual(creds, {"user": "user1", "port": 5656})
