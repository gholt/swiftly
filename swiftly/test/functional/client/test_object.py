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

from StringIO import StringIO
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


class TestObjectClient(unittest.TestCase):

    def setUp(self):
        self.client = Client(AUTH_URL, AUTH_USER, AUTH_KEY)
        self.uuid = uuid4().hex
        self.container = 'swiftly_test_' + self.uuid
        status = self.client.put_container(self.container)[0]
        self.assertTrue(status // 100 == 2)
        status = \
            self.client.put_object(self.container, 'object1', '123456789')[0]
        self.assertTrue(status // 100 == 2)

    def tearDown(self):
        self.client.delete_object(self.container, 'object1')
        self.client.delete_container(self.container)

    def test_head(self):
        status, reason, headers, contents = \
            self.client.head_object(self.container, 'object1')
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertEquals(headers['etag'], '25f9e794323b453885f5181f1b624d0b')
        self.assertTrue(isinstance(contents, str))

    def test_get(self):
        status, reason, headers, contents = \
            self.client.get_object(self.container, 'object1')
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertEquals(headers['etag'], '25f9e794323b453885f5181f1b624d0b')
        self.assertTrue(contents.read(), '123456789')

    def test_get_no_stream(self):
        status, reason, headers, contents = \
            self.client.get_object(self.container, 'object1', stream=False)
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertEquals(headers['etag'], '25f9e794323b453885f5181f1b624d0b')
        self.assertTrue(contents, '123456789')

    def test_put(self):
        try:
            status, reason, headers, contents = self.client.put_object(
                self.container, 'object2', 'contentvalue',
                headers={'x-object-meta-swiftly-test': 'somevalue'})
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(isinstance(headers, dict))
            self.assertTrue(isinstance(contents, str))
            status, reason, headers, contents = \
                self.client.get_object(self.container, 'object2')
            self.assertTrue(status // 100 == 2)
            self.assertEquals(
                headers['etag'], '11a49846cb4877d7f9bb9471b646464f')
            self.assertEquals(
                headers['x-object-meta-swiftly-test'], 'somevalue')
            self.assertEquals(contents.read(), 'contentvalue')
        finally:
            self.client.delete_object(self.container, 'object2')

    def test_put_with_etag(self):
        try:
            status, reason, headers, contents = self.client.put_object(
                self.container, 'object2', 'contentvalue',
                headers={'x-object-meta-swiftly-test': 'somevalue',
                         'etag': '11111111111111111111111111111111'})
            self.assertEquals(status, 422)
            status, reason, headers, contents = self.client.put_object(
                self.container, 'object2', 'contentvalue',
                headers={'x-object-meta-swiftly-test': 'somevalue',
                         'etag': '11a49846cb4877d7f9bb9471b646464f'})
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(isinstance(headers, dict))
            self.assertTrue(isinstance(contents, str))
            status, reason, headers, contents = \
                self.client.get_object(self.container, 'object2')
            self.assertTrue(status // 100 == 2)
            self.assertEquals(
                headers['etag'], '11a49846cb4877d7f9bb9471b646464f')
            self.assertEquals(
                headers['x-object-meta-swiftly-test'], 'somevalue')
            self.assertEquals(contents.read(), 'contentvalue')
        finally:
            self.client.delete_object(self.container, 'object2')

    def test_put_as_stream(self):
        try:
            status, reason, headers, contents = self.client.put_object(
                self.container, 'object2', StringIO('contentvalue'),
                headers={'content-length': '12',
                         'x-object-meta-swiftly-test': 'somevalue'})
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(isinstance(headers, dict))
            self.assertTrue(isinstance(contents, str))
            status, reason, headers, contents = \
                self.client.get_object(self.container, 'object2')
            self.assertTrue(status // 100 == 2)
            self.assertEquals(
                headers['etag'], '11a49846cb4877d7f9bb9471b646464f')
            self.assertEquals(
                headers['x-object-meta-swiftly-test'], 'somevalue')
            self.assertEquals(contents.read(), 'contentvalue')
        finally:
            self.client.delete_object(self.container, 'object2')

    def test_put_as_stream_chunked(self):
        try:
            status, reason, headers, contents = self.client.put_object(
                self.container, 'object2', StringIO('contentvalue'),
                headers={'x-object-meta-swiftly-test': 'somevalue'})
            self.assertTrue(status // 100 == 2)
            self.assertTrue(isinstance(reason, str))
            self.assertTrue(isinstance(headers, dict))
            self.assertTrue(isinstance(contents, str))
            status, reason, headers, contents = \
                self.client.get_object(self.container, 'object2')
            self.assertTrue(status // 100 == 2)
            self.assertEquals(
                headers['etag'], '11a49846cb4877d7f9bb9471b646464f')
            self.assertEquals(
                headers['x-object-meta-swiftly-test'], 'somevalue')
            self.assertEquals(contents.read(), 'contentvalue')
        finally:
            self.client.delete_object(self.container, 'object2')

    def test_post(self):
        status, reason, headers, contents = \
            self.client.head_object(self.container, 'object1')
        self.assertTrue(status // 100 == 2)
        self.assertTrue('x-object-meta-swiftly-test' not in headers)
        status, reason, headers, contents = self.client.post_object(
            self.container, 'object1',
            headers={'x-object-meta-swiftly-test': 'testvalue'})
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue(isinstance(headers, dict))
        self.assertTrue(isinstance(contents, str))
        status, reason, headers, contents = \
            self.client.head_object(self.container, 'object1')
        self.assertTrue(status // 100 == 2)
        self.assertEquals(headers['x-object-meta-swiftly-test'], 'testvalue')

    def test_delete(self):
        status = self.client.head_object(self.container, 'object1')[0]
        self.assertTrue(status // 100 == 2)
        status, reason, headers, contents = \
            self.client.delete_object(self.container, 'object1')
        self.assertTrue(status // 100 == 2)
        self.assertTrue(isinstance(reason, str))
        self.assertTrue(isinstance(headers, dict))
        self.assertTrue(isinstance(contents, str))
        status = self.client.head_object(self.container, 'object1')[0]
        self.assertEquals(status, 404)


class TestObjectDirect(TestObjectClient):

    def setUp(self):
        if Application is None:
            raise SkipTest
        self.client = Client(retries=1, swift_proxy=True,
                             swift_proxy_storage_path=SWIFT_PROXY_STORAGE_PATH)
        self.uuid = uuid4().hex
        self.container = 'swiftly_test_' + self.uuid
        status = self.client.put_container(self.container)[0]
        self.assertTrue(status // 100 == 2)
        status = \
            self.client.put_object(self.container, 'object1', '123456789')[0]
        self.assertTrue(status // 100 == 2)


if __name__ == '__main__':
    unittest.main()
