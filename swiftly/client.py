"""
Client API to Swift

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

__all__ = ['VERSION', 'CHUNK_SIZE', 'Client']


from os import umask
from urllib import quote
from urlparse import urlparse, urlunparse

try:
    import simplejson as json
except ImportError:
    import json

try:
    from swift.proxy.server import Application as SwiftProxy
    try:
        from swift.common.swob import Request
    except ImportError:
        from webob import Request
except ImportError:
    SwiftProxy = None

from swiftly import VERSION


CHUNK_SIZE = 65536


def _delayed_imports(eventlet=True):
    BadStatusLine = HTTPConnection = HTTPException = HTTPSConnection = \
        sleep = None
    if eventlet:
        try:
            from eventlet.green.httplib import BadStatusLine, HTTPException, \
                HTTPSConnection
            try:
                from swift.common.bufferedhttp \
                    import BufferedHTTPConnection as HTTPConnection
            except ImportError:
                from eventlet.green.httplib import HTTPConnection
            from eventlet import sleep
        except ImportError:
            pass
    if BadStatusLine is None:
        from httplib import BadStatusLine
    if HTTPConnection is None:
        from httplib import HTTPConnection
    if HTTPException is None:
        from httplib import HTTPException
    if HTTPSConnection is None:
        from httplib import HTTPSConnection
    if sleep is None:
        from time import sleep
    return BadStatusLine, HTTPConnection, HTTPException, HTTPSConnection, sleep


def _quote(value, safe='/:'):
    if isinstance(value, unicode):
        value = value.encode('utf8')
    return quote(value, safe)


class _Node(object):

    def __init__(self, key, val, prv, nxt):
        self.key = key
        self.val = val
        self.prv = prv
        self.nxt = nxt


class _LocalMemcache(object):

    def __init__(self):
        self.cache = {}
        self.first = None
        self.last = None
        self.count = 0
        self.max_count = 1000

    def set(self, key, value, serialize=True, timeout=0):
        self.delete(key)
        self.last = node = _Node(key, value, self.last, None)
        if node.prv:
            node.prv.nxt = node
        elif not self.first:
            self.first = node
        self.cache[key] = node
        self.count += 1
        if self.count > self.max_count:
            self.delete(self.first.key)

    def get(self, key):
        node = self.cache.get(key)
        return node.val if node else None

    def incr(self, key, delta=1, timeout=0):
        node = self.cache.get(key)
        if node:
            node.val += delta
            return node.val
        else:
            self.set(key, delta)
            return delta

    def decr(self, key, delta=1, timeout=0):
        return self.incr(key, delta=-delta, timeout=timeout)

    def delete(self, key):
        node = self.cache.get(key)
        if node:
            del self.cache[key]
            if node.prv:
                node.prv.nxt = node.nxt
            if node.nxt:
                node.nxt.prv = node.prv
            if self.first == node:
                self.first = node.nxt
            if self.last == node:
                self.last = node.prv
            self.count -= 1

    def set_multi(self, mapping, server_key, serialize=True, timeout=0):
        for key, value in mapping.iteritems():
            self.set(key, value)

    def get_multi(self, keys, server_key):
        return [self.get(k) for k in keys]


class _IterReader(object):

    def __init__(self, iterator):
        self.iterator = iter(iterator)

    def read(self, size=-1):
        try:
            return self.iterator.next()
        except StopIteration:
            return ''


class _FileIter(object):

    def __init__(self, fp, chunked=False):
        self.fp = fp
        self.chunked = chunked
        self.final_chunk_sent = False

    def __iter__(self):
        return self

    def next(self):
        chunk = self.fp.read(CHUNK_SIZE)
        if not chunk:
            if self.chunked and not self.final_chunk_sent:
                self.final_chunk_sent = True
                return '0\r\n\r\n'
            raise StopIteration
        if self.chunked:
            return '%x\r\n%s\r\n' % (len(chunk), chunk)
        return chunk


class Client(object):
    """
    Client code to work with Swift.

    :param auth_url: The URL to the auth system.
    :param auth_user: The user to authenticate as.
    :param auth_key: The key to use when authenticating.
    :param proxy: The URL to the proxy to use. Default: None.
    :param snet: Prepends "snet-" to the host name of the storage URL
        given once authenticated. This is usually only useful when
        working with Rackspace Cloud Files and wanting to use
        Rackspace ServiceNet. Default: False.
    :param retries: The number of times to retry requests if a server
        error ocurrs (5xx response). Default: 4 (for a total of 5
        attempts).
    :param swift_proxy: Default: None. If set, the
        swift.proxy.server.Application given will be used instead of
        connecting to an external proxy server. You can also set it to
        True and the Swift proxy will be created with default values.
    :param swift_proxy_storage_path: If swift_proxy is set,
        swift_proxy_storage_path is the path to the Swift account to
        use (example: /v1/AUTH_test).
    :param cache_path: Default: None. If set to a path, the storage URL and
        auth token are cached in the file for reuse. If there is already cached
        values in the file, they are used without authenticating first.
    :param eventlet: Default: True. If true, Eventlet will be used if
        installed.
    """

    def __init__(self, auth_url=None, auth_user=None, auth_key=None,
                 proxy=None, snet=False, retries=4, swift_proxy=None,
                 swift_proxy_storage_path=None, cache_path=None,
                 eventlet=True):
        self.auth_url = auth_url
        self.auth_user = auth_user
        self.auth_key = auth_key
        self.proxy = proxy
        self.snet = snet
        self.attempts = retries + 1
        self.storage_url = None
        self.auth_token = None
        self.storage_conn = None
        self.storage_path = None
        self.swift_proxy = swift_proxy
        if swift_proxy is True:
            self.swift_proxy = SwiftProxy({}, memcache=_LocalMemcache())
        if swift_proxy:
            self.storage_path = swift_proxy_storage_path
        self.cache_path = cache_path
        if self.cache_path:
            try:
                data = open(self.cache_path, 'r').read().decode('base64')
                (auth_url, auth_user, auth_key, self.storage_url,
                 self.auth_token) = [v for v in data.split('\n')]
                if auth_url != self.auth_url or auth_user != self.auth_user \
                        or auth_key != self.auth_key:
                    self.storage_url = None
                    self.auth_token = None
            except Exception:
                pass
        (self.BadStatusLine, self.HTTPConnection, self.HTTPException,
         self.HTTPSConnection, self.sleep) = _delayed_imports(eventlet)

    def _connect(self, url=None):
        if not url:
            if not self.storage_url:
                self._auth()
            url = self.storage_url
        parsed = urlparse(url)
        proxy_parsed = urlparse(self.proxy) if self.proxy else None
        netloc = (proxy_parsed if self.proxy else parsed).netloc
        if parsed.scheme == 'http':
            conn = self.HTTPConnection(netloc)
        elif parsed.scheme == 'https':
            conn = self.HTTPSConnection(netloc)
        else:
            raise self.HTTPException(
                'Cannot handle protocol scheme %s for url %s' %
                (parsed.scheme, repr(url)))
        if self.proxy:
            conn._set_tunnel(parsed.hostname, parsed.port)
        return parsed, conn

    def _response_headers(self, headers):
        hdrs = {}
        for h, v in headers:
            h = h.lower()
            if h in hdrs:
                if isinstance(hdrs[h], list):
                    hdrs[h].append(v)
                else:
                    hdrs[h] = [hdrs[h], v]
            else:
                hdrs[h] = v
        return hdrs

    def _auth(self):
        if not self.auth_url:
            return
        status = 0
        reason = 'Unknown'
        attempt = 0
        while attempt < self.attempts:
            attempt += 1
            parsed, conn = self._connect(self.auth_url)
            conn.request('GET', parsed.path, '',
                {'User-Agent': 'Swiftly v%s' % VERSION,
                 'X-Auth-User': _quote(self.auth_user),
                 'X-Auth-Key': _quote(self.auth_key)})
            try:
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                hdrs = self._response_headers(resp.getheaders())
                resp.read()
                resp.close()
                conn.close()
            except Exception, err:
                status = 0
                reason = str(err)
                hdrs = {}
            if status == 401:
                break
            if status // 100 == 2:
                self.storage_url = hdrs['x-storage-url']
                if self.snet:
                    parsed = list(urlparse(self.storage_url))
                    # Second item in the list is the netloc
                    parsed[1] = 'snet-' + parsed[1]
                    self.storage_url = urlunparse(parsed)
                self.auth_token = hdrs.get('x-auth-token')
                if not self.auth_token:
                    self.auth_token = hdrs.get('x-storage-token')
                    if not self.auth_token:
                        raise KeyError('x-auth-token or x-storage-token')
                if self.cache_path:
                    data = '\n'.join([self.auth_url, self.auth_user,
                        self.auth_key, self.storage_url, self.auth_token])
                    old_umask = umask(0077)
                    open(self.cache_path, 'w').write(data.encode('base64'))
                    umask(old_umask)
                return
            self.sleep(2 ** attempt)
        raise self.HTTPException('Auth GET failed', status, reason)

    def _default_reset_func(self):
        raise self.HTTPException(
            'Failure and no ability to reset contents for reupload.')

    def _request(self, method, path, contents, headers, decode_json=False,
                 stream=False):
        reset_func = self._default_reset_func
        tell = getattr(contents, 'tell', None)
        seek = getattr(contents, 'seek', None)
        if tell and seek:
            try:
                orig_pos = tell()
                reset_func = lambda: seek(orig_pos)
            except Exception:
                tell = seek = None
        elif not contents:
            reset_func = lambda: None
        status = 0
        reason = 'Unknown'
        attempt = 0
        while attempt < self.attempts:
            attempt += 1
            if not self.swift_proxy and not self.storage_conn:
                parsed, self.storage_conn = self._connect()
                self.storage_path = parsed.path
            hdrs = {'User-Agent': 'Swiftly v%s' % VERSION,
                    'X-Auth-Token': self.auth_token}
            if headers:
                hdrs.update(headers)
            resp = None
            if not hasattr(contents, 'read'):
                if self.swift_proxy:
                    req = Request.blank(self.storage_path + path,
                                        environ={'REQUEST_METHOD': method},
                                        headers=hdrs, body=contents)
                    resp = req.get_response(self.swift_proxy)
                else:
                    self.storage_conn.request(
                        method, self.storage_path + path, contents, hdrs)
            else:
                req = None
                if self.swift_proxy:
                    req = Request.blank(self.storage_path + path,
                                        environ={'REQUEST_METHOD': method},
                                        headers=hdrs)
                else:
                    self.storage_conn.putrequest(
                        method, self.storage_path + path)
                content_length = None
                for h, v in hdrs.iteritems():
                    if h.lower() == 'content-length':
                        content_length = int(v)
                    if req:
                        req.headers[h] = v
                    else:
                        self.storage_conn.putheader(h, v)
                if content_length is None:
                    if req:
                        req.headers['Transfer-Encoding'] = 'chunked'
                        req.body_file = contents
                        resp = req.get_response(self.swift_proxy)
                    else:
                        self.storage_conn.putheader(
                            'Transfer-Encoding', 'chunked')
                        self.storage_conn.endheaders()
                        chunk = contents.read(CHUNK_SIZE)
                        while chunk:
                            self.storage_conn.send(
                                '%x\r\n%s\r\n' % (len(chunk), chunk))
                            chunk = contents.read(CHUNK_SIZE)
                        self.storage_conn.send('0\r\n\r\n')
                else:
                    if req:
                        req.body_file = contents
                        req.content_length = content_length
                        resp = req.get_response(self.swift_proxy)
                    else:
                        self.storage_conn.endheaders()
                        left = content_length
                        while left > 0:
                            size = CHUNK_SIZE
                            if size > left:
                                size = left
                            chunk = contents.read(size)
                            self.storage_conn.send(chunk)
                            left -= len(chunk)
            if not resp:
                try:
                    resp = self.storage_conn.getresponse()
                    status = resp.status
                    reason = resp.reason
                    hdrs = self._response_headers(resp.getheaders())
                    if stream:
                        value = resp
                    else:
                        value = resp.read()
                        resp.close()
                except self.BadStatusLine, err:
                    status = 0
                    reason = str(err)
                    hdrs = {}
                    value = None
            else:
                status = resp.status_int
                reason = resp.status.split(' ', 1)[1]
                hdrs = self._response_headers(resp.headers.items())
                if stream:
                    value = _IterReader(resp.app_iter)
                else:
                    value = resp.body
            if status == 401:
                if stream:
                    resp.close()
                self.storage_conn.close()
                self.storage_conn = None
                self._auth()
                attempt -= 1
            elif status and status // 100 != 5:
                if not stream and decode_json and status // 100 == 2:
                    if value:
                        value = json.loads(value)
                    else:
                        value = None
                return (status, reason, hdrs, value)
            if self.storage_conn:
                self.storage_conn.close()
                self.storage_conn = None
            if reset_func:
                reset_func()
            self.sleep(2 ** attempt)
        raise self.HTTPException('%s %s failed' % (method, path),
                                 status, reason)

    def head_account(self, headers=None):
        """
        HEADs the account and returns the results. Useful headers
        returned are:

        =========================== =================================
        x-account-bytes-used        Object storage used for the
                                    account, in bytes.
        x-account-container-count   The number of containers in the
                                    account.
        x-account-object-count      The number of objects in the
                                    account.
        =========================== =================================

        Also, any user headers beginning with x-account-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request('HEAD', '', '', headers)

    def get_account(self, headers=None, prefix=None, delimiter=None,
                    marker=None, end_marker=None, limit=None):
        """
        GETs the account and returns the results. This is done to list
        the containers for the account. Some useful headers are also
        returned:

        =========================== =================================
        x-account-bytes-used        Object storage used for the
                                    account, in bytes.
        x-account-container-count   The number of containers in the
                                    account.
        x-account-object-count      The number of objects in the
                                    account.
        =========================== =================================

        Also, any user headers beginning with x-account-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param headers: Additional headers to send with the request.
        :param prefix: The prefix container names must match to be
                       listed.
        :param delimiter: The delimiter for the listing. Delimiters
                          indicate how far to progress through
                          container names before "rolling them up".
                          For instance, a delimiter='.' query on an
                          account with the containers::

                           one.one
                           one.two
                           two
                           three.one

                          would return the JSON value of::

                           [{'subdir': 'one.'},
                            {'count': 0, 'bytes': 0, 'name': 'two'},
                            {'subdir': 'three.'}]

                          Using this with prefix can allow you to
                          traverse a psuedo hierarchy.
        :param marker: Only container names after this marker will be
                       returned. Swift returns a limited number of
                       containers per request (often 10,000). To get
                       the next batch of names, you issue another
                       query with the marker set to the last name you
                       received. You can continue to issue requests
                       until you receive no more names.
        :param end_marker: Only container names before this marker will be
                           returned.
        :param limit: Limits the size of the list returned per
                      request. The default and maximum depends on the
                      Swift cluster (usually 10,000).
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        query = '?format=json'
        if prefix:
            query += '&prefix=' + _quote(prefix)
        if delimiter:
            query += '&delimiter=' + _quote(delimiter)
        if marker:
            query += '&marker=' + _quote(marker)
        if end_marker:
            query += '&end_marker=' + _quote(end_marker)
        if limit:
            query += '&limit=' + _quote(str(limit))
        return self._request('GET', query, '', headers, decode_json=True)

    def post_account(self, headers=None):
        """
        POSTs the account and returns the results. This is usually
        done to set X-Account-Meta-xxx headers. Note that any existing
        X-Account-Meta-xxx headers will remain untouched. To remove an
        X-Account-Meta-xxx header, send the header with an empty
        string as its value.

        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request('POST', '', '', headers)

    def delete_account(self, headers=None,
                       yes_i_mean_delete_the_account=False):
        """
        DELETEs the account and returns the results. 

        Some Swift clusters do not support this.

        Those that do will mark the account as deleted and immediately begin
        removing the objects from the cluster in the backgound.

        THERE IS NO GOING BACK!

        :param headers: Additional headers to send with the request.
        :param yes_i_mean_delete_the_account: Set to True to verify you really
                                              mean to delete the entire
                                              account.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        if not yes_i_mean_delete_the_account:
            return (0, 'yes_i_mean_delete_the_account was not set to True', {},
                    '')
        return self._request('DELETE', '', '', headers)

    def _container_path(self, container):
        if container.startswith('/'):
            return _quote(container)
        else:
            return '/' + _quote(container)

    def head_container(self, container, headers=None):
        """
        HEADs the container and returns the results. Useful headers
        returned are:

        =========================== =================================
        x-container-bytes-used      Object storage used for the
                                    container, in bytes.
        x-container-object-count    The number of objects in the
                                    container.
        =========================== =================================

        Also, any user headers beginning with x-container-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'HEAD', self._container_path(container), '', headers)

    def get_container(self, container, headers=None, prefix=None,
                      delimiter=None, marker=None, end_marker=None,
                      limit=None):
        """
        GETs the container and returns the results. This is done to
        list the objects for the container. Some useful headers are
        also returned:

        =========================== =================================
        x-container-bytes-used      Object storage used for the
                                    container, in bytes.
        x-container-object-count    The number of objects in the
                                    container.
        =========================== =================================

        Also, any user headers beginning with x-container-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param prefix: The prefix object names must match to be
                       listed.
        :param delimiter: The delimiter for the listing. Delimiters
                          indicate how far to progress through object
                          names before "rolling them up". For
                          instance, a delimiter='/' query on an
                          container with the objects::

                           one/one
                           one/two
                           two
                           three/one

                          would return the JSON value of::

                           [{'subdir': 'one/'},
                            {'count': 0, 'bytes': 0, 'name': 'two'},
                            {'subdir': 'three/'}]

                          Using this with prefix can allow you to
                          traverse a psuedo hierarchy.
        :param marker: Only object names after this marker will be
                       returned. Swift returns a limited number of
                       objects per request (often 10,000). To get the
                       next batch of names, you issue another query
                       with the marker set to the last name you
                       received. You can continue to issue requests
                       until you receive no more names.
        :param end_marker: Only object names before this marker will be
                           returned.
        :param limit: Limits the size of the list returned per
                      request. The default and maximum depends on the
                      Swift cluster (usually 10,000).
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        query = '?format=json'
        if prefix:
            query += '&prefix=' + _quote(prefix)
        if delimiter:
            query += '&delimiter=' + _quote(delimiter)
        if marker:
            query += '&marker=' + _quote(marker)
        if end_marker:
            query += '&end_marker=' + _quote(end_marker)
        if limit:
            query += '&limit=' + _quote(str(limit))
        return self._request('GET', self._container_path(container) + query,
                             '', headers, decode_json=True)

    def put_container(self, container, headers=None):
        """
        PUTs the container and returns the results. This is usually
        done to create new containers and can also be used to set
        X-Container-Meta-xxx headers. Note that if the container
        already exists, any existing X-Container-Meta-xxx headers will
        remain untouched. To remove an X-Container-Meta-xxx header,
        send the header with an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'PUT', self._container_path(container), '', headers)

    def post_container(self, container, headers=None):
        """
        POSTs the container and returns the results. This is usually
        done to set X-Container-Meta-xxx headers. Note that any
        existing X-Container-Meta-xxx headers will remain untouched.
        To remove an X-Container-Meta-xxx header, send the header with
        an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'POST', self._container_path(container), '', headers)

    def delete_container(self, container, headers=None):
        """
        DELETEs the container and returns the results.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'DELETE', self._container_path(container), '', headers)

    def _object_path(self, container, obj):
        container = container.rstrip('/')
        if container.startswith('/'):
            container = _quote(container)
        else:
            container = '/' + _quote(container)
        if obj.startswith('/'):
            return container + _quote(obj)
        else:
            return container + '/' + _quote(obj)

    def head_object(self, container, obj, headers=None):
        """
        HEADs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'HEAD', self._object_path(container, obj), '', headers)

    def get_object(self, container, obj, headers=None, stream=True):
        """
        GETs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param stream: Indicates whether to stream the contents or
                       preread them fully and return them as a str.
                       Default: True to stream the contents.
                       When streaming, contents will have the standard
                       file-like-object read function, which accepts
                       an optional size parameter to limit how much
                       data is read per call.
                       When streaming is on, be certain to fully read
                       the contents before issuing another request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: if *stream* was True, *contents* is a
                file-like-object of the contents of the HTTP body. If
                *stream* was False, *contents* is just a simple str of
                the HTTP body.
        """
        return self._request('GET', self._object_path(container, obj), '',
                             headers, stream=stream)

    def put_object(self, container, obj, contents, headers=None):
        """
        PUTs the object and returns the results. This is used to
        create or overwrite objects. X-Object-Meta-xxx can optionally
        be sent to be stored with the object. Content-Type,
        Content-Encoding and other standard HTTP headers can often
        also be set, depending on the Swift cluster.

        Note that you can set the ETag header to the MD5 sum of the
        contents for extra verifification the object was stored
        correctly.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param contents: The contents of the object to store. This can
                         be a simple str, or a file-like-object with
                         at least a read function. If the
                         file-like-object also has tell and seek
                         functions, the PUT can be reattempted on any
                         server error.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'PUT', self._object_path(container, obj), contents, headers)

    def post_object(self, container, obj, headers=None):
        """
        POSTs the object and returns the results. This is used to
        update the object's header values. Note that all headers must
        be sent with the POST, unlike the account and container POSTs.
        With account and container POSTs, existing headers are
        untouched. But with object POSTs, any existing headers are
        removed. The full list of supported headers depends on the
        Swift cluster, but usually include Content-Type,
        Content-Encoding, and any X-Object-Meta-xxx headers.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'POST', self._object_path(container, obj), '', headers)

    def delete_object(self, container, obj, headers=None):
        """
        DELETEs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'DELETE', self._object_path(container, obj), '', headers)
