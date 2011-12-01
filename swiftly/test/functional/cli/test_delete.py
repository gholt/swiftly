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
from uuid import uuid4

from swiftly.cli import CLI
from swiftly.test.functional import AUTH_URL, AUTH_USER, AUTH_KEY, \
                                    SWIFT_PROXY_STORAGE_PATH


class TestDelete(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.start_args = ['-A', AUTH_URL, '-U', AUTH_USER, '-K', AUTH_KEY]

    def setUp(self):
        self.uuid = uuid4().hex
        self.container = 'swiftly_test_' + self.uuid
        self.obj = 'swiftly_test_' + self.uuid + '/test'

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['put', self.container])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()

        stdin = StringIO('testvalue')
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['put', self.obj])
        cli = CLI(args=args, stdin=stdin, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)

    def tearDown(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container, '--recursive'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        cli.main()

    def test_delete(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.obj])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

    def test_delete_recursive(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['409', 'Conflict'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container, '--recursive'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

    def test_delete_fails(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container + '/test2'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['404', 'Not', 'Found'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container + '2'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['404', 'Not', 'Found'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['409', 'Conflict'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertTrue('THERE IS NO GOING BACK' in stderr.getvalue())

    def test_delete_ignore_404(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container + '/test2', '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container + '2', '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', self.container, '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['409', 'Conflict'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['delete', '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertTrue('THERE IS NO GOING BACK' in stderr.getvalue())


class TestDirectDelete(TestDelete):

    def __init__(self, *args, **kwargs):
        TestDelete.__init__(self, *args, **kwargs)
        self.start_args = ['-D', SWIFT_PROXY_STORAGE_PATH]


if __name__ == '__main__':
    unittest.main()
