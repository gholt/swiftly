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
from uuid import uuid4

from nose import SkipTest

from swiftly.client import Client
from swiftly.test.functional import AUTH_URL, AUTH_USER, AUTH_KEY, \
                                    SWIFT_PROXY_STORAGE_PATH

try:
    from eventlet.green.httplib import HTTPException
except ImportError:
    from httplib import HTTPException

try:
    from swift.proxy.server import Application
except ImportError:
    Application = None


class TestAccountClient(unittest.TestCase):

    def setUp(self):
        self.client = Client(AUTH_URL, AUTH_USER, AUTH_KEY, retries=1)

    def test_head(self):
        status, reason, headers, contents = self.client.head_account()
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue('x-account-bytes-used' in headers)
        self.assertTrue(isinstance(contents, str))

    def test_get(self):
        uuid = uuid4().hex
        try:
            status, reason, headers, contents = \
                self.client.put_container('swiftly_test_' + uuid)
            self.assertTrue(status // 100 == 2)
            status, reason, headers, contents = \
                self.client.get_account(prefix='swiftly_test_' + uuid)
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(int(headers['x-account-container-count']) > 0)
            self.assertTrue(isinstance(contents, list))
            found = False
            for c in contents:
                if c['name'] == 'swiftly_test_' + uuid:
                    found = True
                    break
            self.assertTrue(found)
        finally:
            self.client.delete_container('swiftly_test_' + uuid)

    def test_post(self):
        uuid = uuid4().hex
        try:
            status, reason, headers, contents = self.client.post_account(
                headers={'x-account-meta-swiftly-test-' + uuid: '123456789'})
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(isinstance(headers, dict))
            self.assertTrue(isinstance(contents, str))
            status, reason, headers, contents = self.client.head_account()
            self.assertTrue(status // 100 == 2)
            self.assertEquals(headers['x-account-meta-swiftly-test-' + uuid],
                              '123456789')
            status, reason, headers, contents = self.client.post_account(
                headers={'x-account-meta-swiftly-test-' + uuid: ''})
            self.assertTrue(status // 100 == 2)
            status, reason, headers, contents = self.client.head_account()
            self.assertTrue(status // 100 == 2)
            self.assertTrue(
                'x-account-meta-swiftly-test-' + uuid not in headers)
        finally:
            status, reason, headers, contents = self.client.post_account(
                headers={'x-account-meta-swiftly-test-' + uuid: ''})


class TestAccountDirect(TestAccountClient):

    def setUp(self):
        if Application is None:
            raise SkipTest
        self.client = Client(retries=1, swift_proxy=True,
                             swift_proxy_storage_path=SWIFT_PROXY_STORAGE_PATH)


class TestAcountClientOnly(unittest.TestCase):

    def setUp(self):
        self.client = Client(AUTH_URL, AUTH_USER, AUTH_KEY, retries=1)

    def test_head_bad_auth(self):
        self.client = Client(AUTH_URL, AUTH_USER, AUTH_KEY + 'invalid',
                             retries=0)
        exc = None
        try:
            self.client.head_account()
        except HTTPException, err:
            exc = err
        self.assertTrue(exc is not None)
        self.assertEquals(exc.args[1], 401)


if __name__ == '__main__':
    unittest.main()
