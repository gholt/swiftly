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


class TestContainerClient(unittest.TestCase):

    def setUp(self):
        self.client = Client(AUTH_URL, AUTH_USER, AUTH_KEY)
        self.uuid = uuid4().hex
        self.container = 'swiftly_test_' + self.uuid
        status = self.client.put_container(self.container)[0]
        self.assertTrue(status // 100 == 2)

    def tearDown(self):
        self.client.delete_container(self.container)

    def test_head(self):
        status, reason, headers, contents = \
            self.client.head_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue('x-container-bytes-used' in headers)
        self.assertTrue(isinstance(contents, str))

    def test_get(self):
        try:
            status, reason, headers, contents = \
                self.client.put_object(self.container, 'object1', 'testvalue')
            self.assertTrue(status // 100 == 2)
            status, reason, headers, contents = \
                self.client.get_container(self.container)
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertEquals(int(headers['x-container-object-count']), 1)
            self.assertTrue(isinstance(contents, list))
            found = False
            for c in contents:
                if c['name'] == 'object1':
                    found = True
                    break
            self.assertTrue(found)
        finally:
            self.client.delete_object(self.container, 'object1')

    def test_put(self):
        status, reason, headers, contents = self.client.put_container(
            self.container,
            headers={'x-container-meta-swiftly-test': '123456789'})
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue(isinstance(headers, dict))
        self.assertTrue(isinstance(contents, str))
        status, reason, headers, contents = \
            self.client.head_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertEquals(
            headers['x-container-meta-swiftly-test'], '123456789')
        status, reason, headers, contents = self.client.put_container(
            self.container, headers={'x-container-meta-swiftly-test': ''})
        self.assertTrue(status // 100 == 2)
        status, reason, headers, contents = \
            self.client.head_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertTrue('x-container-meta-swiftly-test' not in headers)

    def test_post(self):
        status, reason, headers, contents = self.client.post_container(
            self.container,
            headers={'x-container-meta-swiftly-test': '123456789'})
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue(isinstance(headers, dict))
        self.assertTrue(isinstance(contents, str))
        status, reason, headers, contents = \
            self.client.head_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertEquals(
            headers['x-container-meta-swiftly-test'], '123456789')
        status, reason, headers, contents = self.client.post_container(
            self.container, headers={'x-container-meta-swiftly-test': ''})
        self.assertTrue(status // 100 == 2)
        status, reason, headers, contents = \
            self.client.head_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertTrue('x-container-meta-swiftly-test' not in headers)

    def test_delete(self):
        status = self.client.head_container(self.container)[0]
        self.assertTrue(status // 100 == 2)
        status, reason, headers, contents = \
            self.client.delete_container(self.container)
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue(isinstance(headers, dict))
        self.assertTrue(isinstance(contents, str))
        status = self.client.head_container(self.container)[0]
        self.assertEquals(status, 404)


class TestContainerDirect(TestContainerClient):

    def setUp(self):
        if Application is None:
            raise SkipTest
        self.client = Client(retries=1, swift_proxy=True,
                             swift_proxy_storage_path=SWIFT_PROXY_STORAGE_PATH)
        self.uuid = uuid4().hex
        self.container = 'swiftly_test_' + self.uuid
        status = self.client.put_container(self.container)[0]
        self.assertTrue(status // 100 == 2)


if __name__ == '__main__':
    unittest.main()
