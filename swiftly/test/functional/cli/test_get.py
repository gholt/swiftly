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

try:
    import simplejson as json
except ImportError:
    import json


class TestGet(unittest.TestCase):

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

    def test_get_obj(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.obj])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), 'testvalue')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_obj_ignore_404(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.obj + '2'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['404', 'Not', 'Found'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.obj + '2', '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_container(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.container])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), 'test\n')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_container_full(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.container, '-f'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        stdout = stdout.getvalue().split()
        self.assertEquals(len(stdout), 6)
        self.assertEquals(stdout[0], '9')
        self.assertEquals(stdout[3], 'e9de89b0a5e9ad6efd5e5ab543ec617c')
        self.assertEquals(stdout[4], 'application/octet-stream')
        self.assertEquals(stdout[5], 'test')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_container_raw(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.container, '-r'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        stdout = json.loads(stdout.getvalue())
        self.assertEquals(len(stdout), 1)
        stdout = stdout[0]
        self.assertEquals(stdout['bytes'], 9)
        self.assertEquals(stdout['hash'], 'e9de89b0a5e9ad6efd5e5ab543ec617c')
        self.assertEquals(stdout['content_type'], 'application/octet-stream')
        self.assertEquals(stdout['name'], 'test')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_container_ignore_404(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.container + '2'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 1)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue().split(), ['404', 'Not', 'Found'])

        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', self.container + '2', '--ignore-404'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertEquals(stdout.getvalue(), '')
        self.assertEquals(stderr.getvalue(), '')

    def test_get_account(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        self.assertTrue(self.container in stdout.getvalue())
        self.assertEquals(stderr.getvalue(), '')

    def test_get_account_full(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', '-f'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        match = None
        for line in stdout.getvalue().split('\n'):
            if self.container in line:
                match = line
                break
        self.assertTrue(match)
        stdout = match.split()
        self.assertEquals(len(stdout), 3)
        int(stdout[0])
        int(stdout[1])
        self.assertEquals(stdout[2], self.container)
        self.assertEquals(stderr.getvalue(), '')

    def test_get_account_raw(self):
        stdout = StringIO()
        stderr = StringIO()
        args = list(self.start_args)
        args.extend(['get', '-r'])
        cli = CLI(args=args, stdout=stdout, stderr=stderr)
        rv = cli.main()
        self.assertEquals(rv, 0)
        stdout = json.loads(stdout.getvalue())
        self.assertTrue(len(stdout))
        stdout = stdout[0]
        self.assertEquals(sorted(stdout.keys()), ['bytes', 'count', 'name'])


class TestDirectGet(TestGet):

    def __init__(self, *args, **kwargs):
        TestGet.__init__(self, *args, **kwargs)
        self.start_args = ['-D', SWIFT_PROXY_STORAGE_PATH]


if __name__ == '__main__':
    unittest.main()
