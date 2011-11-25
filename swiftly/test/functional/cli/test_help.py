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

from swiftly.cli import CLI
from swiftly.test.functional import AUTH_URL, AUTH_USER, AUTH_KEY, \
                                    SWIFT_PROXY_STORAGE_PATH


class TestHelp(unittest.TestCase):

    def test_no_args(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=[], stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 1)
        self.assertTrue('Usage:' in stdout, stdout)
        self.assertTrue('Options:' in stdout, stdout)
        self.assertTrue('Commands:' in stdout, stdout)
        self.assertTrue(not stderr)

    def test_invalid_args(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=['asfsdasdf'], stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 1)
        self.assertTrue('Usage:' in stdout, stdout)
        self.assertTrue('Options:' in stdout, stdout)
        self.assertTrue('Commands:' in stdout, stdout)
        self.assertTrue(not stderr)

    def test_help(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=['help'], stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 1)
        self.assertTrue('Usage:' in stdout, stdout)
        self.assertTrue('Options:' in stdout, stdout)
        self.assertTrue('Commands:' in stdout, stdout)
        self.assertTrue(not stderr)

    def test_help_command(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=['help', 'head'], stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 1)
        self.assertTrue('Usage:' in stdout, stdout)
        self.assertTrue(not stderr)

    def test_help_invalid_command(self):
        stdout = StringIO()
        stderr = StringIO()
        cli = CLI(args=['help', 'slkdfjsdkfj'], stdout=stdout, stderr=stderr)
        rv = cli.main()
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        self.assertEquals(rv, 1)
        self.assertTrue('Usage:' in stdout, stdout)
        self.assertTrue('Options:' in stdout, stdout)
        self.assertTrue('Commands:' in stdout, stdout)
        self.assertTrue(not stderr)


if __name__ == '__main__':
    unittest.main()
