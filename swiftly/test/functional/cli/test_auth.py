"""
Copyright 2011 Gregory Holt

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest
from StringIO import StringIO
from urlparse import urlparse

from swiftly.cli import CLI
from swiftly.test.functional import AUTH_URL, AUTH_USER, AUTH_KEY, \
                                    SWIFT_PROXY_STORAGE_PATH


class TestAuth(unittest.TestCase):

    def test_direct(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=['-D', SWIFT_PROXY_STORAGE_PATH, 'auth'], stdout=stdout,
                  stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 0)
        stdout = stdout.split()
        self.assertEquals(stdout,
                          ['Direct', 'Storage', 'Path:', '/v1/AUTH_test'])
        self.assertTrue(not stderr)

    def test_client(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(
            args=['-A', AUTH_URL, '-U', AUTH_USER, '-K', AUTH_KEY, 'auth'],
            stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 0)
        stdout = stdout.split()
        self.assertEquals(stdout[:2], ['Storage', 'URL:'])
        urlparse(stdout[2])
        self.assertEquals(stdout[3:-1], ['Auth', 'Token:'])
        self.assertTrue(not stderr)


if __name__ == '__main__':
    unittest.main()
