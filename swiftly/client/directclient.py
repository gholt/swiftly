"""
Provides a direct client by using loaded Swift Proxy Server code to
work with Swift.
"""
"""
Copyright 2011-2013 Gregory Holt

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
import six
import json
from six.moves import StringIO

from swiftly.client.client import Client
from swiftly.client.utils import quote, headers_to_dict


class DirectClient(Client):
    """
    Direct client by using loaded Swift Proxy Server code to work
    with Swift.

    :param swift_proxy: Default: None. If set, the
        swift.proxy.server.Application given will be used instead of
        creating a default Swift proxy application.
    :param swift_proxy_storage_path: The path to the Swift account to
        use (example: /v1/AUTH_test).
    :param swift_proxy_cdn_path: The path to the Swift account to use
        for CDN management (example: /v1/AUTH_test).
    :param attempts: The number of times to try requests if a server
        error occurs (5xx response). Default: 5
    :param eventlet: Default: None. If True, Eventlet will be used if
        installed. If False, Eventlet will not be used even if
        installed. If None, the default, Eventlet will be used if
        installed and its version is at least 0.11.0 when a CPU usage
        bug was fixed.
    :param chunk_size: Maximum size to read or write at one time.
    :param verbose: Set to a ``func(msg, *args)`` that will be called
        with debug messages. Constructing a string for output can be
        done with msg % args.
    :param verbose_id: Set to a string you wish verbose messages to
        be prepended with; can help in identifying output when
        multiple Clients are in use.
    :param direct_object_ring: The path to custom object ring to used
        by the DirectClient
    """

    def __init__(self, swift_proxy=None, swift_proxy_storage_path=None,
                 swift_proxy_cdn_path=None, attempts=5, eventlet=None,
                 chunk_size=65536, verbose=None, verbose_id='',
                 direct_object_ring=None):
        super(DirectClient, self).__init__()
        self.storage_path = swift_proxy_storage_path
        self.cdn_path = swift_proxy_cdn_path
        self.attempts = attempts
        self.chunk_size = chunk_size
        if verbose:
            self.verbose = lambda m, *a, **k: verbose(
                self._verbose_id + m, *a, **k)
        else:
            self.verbose = lambda *a, **k: None
        self.verbose_id = verbose_id
        self._verbose_id = self.verbose_id
        if self._verbose_id:
            self._verbose_id += ' '
        self.swift_proxy = swift_proxy
        if not swift_proxy:
            self.verbose('Creating default proxy instance.')
            import swift.proxy.server
            from swiftly.client.localmemcache import LocalMemcache
            from swiftly.client.nulllogger import NullLogger
            try:
                import swift.common.swob
                self.Request = swift.common.swob.Request
            except ImportError:
                import webob
                self.Request = webob.Request
            self.swift_proxy = swift.proxy.server.Application(
                {}, memcache=LocalMemcache(), logger=NullLogger())
            self.oring = None
            def get_oring(*args):
                return self.oring

            if direct_object_ring:
                self.oring = swift.common.ring.ring.Ring(direct_object_ring)
                self.swift_proxy.get_object_ring = get_oring

        if eventlet is None:
            try:
                import eventlet
                # Eventlet 0.11.0 fixed the CPU bug
                if eventlet.__version__ >= '0.11.0':
                    eventlet = True
            except ImportError:
                pass
        if eventlet:
            try:
                import eventlet
                self.sleep = eventlet.sleep
            except ImportError:
                import time
                self.sleep = time.sleep
        else:
            import time
            self.sleep = time.sleep

    def _default_reset_func(self):
        raise Exception(
            'Failure and no ability to reset contents for reupload.')

    def request(self, method, path, contents, headers, decode_json=False,
                stream=False, query=None, cdn=False):
        """
        See :py:func:`swiftly.client.client.Client.request`
        """
        if query:
            path += '?' + '&'.join(
                ('%s=%s' % (quote(k), quote(v)) if v else quote(k))
                for k, v in sorted(six.iteritems(query)))
        reset_func = self._default_reset_func
        if isinstance(contents, six.string_types):
            contents = StringIO(contents)
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
            if cdn:
                conn_path = self.cdn_path
            else:
                conn_path = self.storage_path
            titled_headers = dict((k.title(), v) for k, v in six.iteritems({
                'User-Agent': self.user_agent}))
            if headers:
                titled_headers.update(
                    (k.title(), v) for k, v in six.iteritems(headers))
            resp = None
            if not hasattr(contents, 'read'):
                if method not in self.no_content_methods and contents and \
                        'Content-Length' not in titled_headers and \
                        'Transfer-Encoding' not in titled_headers:
                    titled_headers['Content-Length'] = str(
                        len(contents or ''))
                req = self.Request.blank(
                    conn_path + path,
                    environ={'REQUEST_METHOD': method, 'swift_owner': True},
                    headers=titled_headers, body=contents)
                verbose_headers = '  '.join(
                    '%s: %s' % (k, v) for k, v in six.iteritems(titled_headers))
                self.verbose(
                    '> %s %s %s', method, conn_path + path, verbose_headers)
                resp = req.get_response(self.swift_proxy)
            else:
                req = self.Request.blank(
                    conn_path + path,
                    environ={'REQUEST_METHOD': method, 'swift_owner': True},
                    headers=titled_headers)
                content_length = None
                for h, v in six.iteritems(titled_headers):
                    if h.lower() == 'content-length':
                        content_length = int(v)
                    req.headers[h] = v
                if method not in self.no_content_methods and \
                        content_length is None:
                    titled_headers['Transfer-Encoding'] = 'chunked'
                    req.headers['Transfer-Encoding'] = 'chunked'
                else:
                    req.content_length = content_length
                req.body_file = contents
                verbose_headers = '  '.join(
                    '%s: %s' % (k, v) for k, v in six.iteritems(titled_headers))
                self.verbose(
                    '> %s %s %s', method, conn_path + path, verbose_headers)
                resp = req.get_response(self.swift_proxy)
            status = resp.status_int
            reason = resp.status.split(' ', 1)[1]
            hdrs = headers_to_dict(resp.headers.items())
            if stream:
                def iter_reader(size=-1):
                    if size == -1:
                        return ''.join(resp.app_iter)
                    else:
                        try:
                            return next(resp.app_iter)
                        except StopIteration:
                            return ''
                iter_reader.read = iter_reader
                value = iter_reader
            else:
                value = resp.body
            self.verbose('< %s %s', status, reason)
            if status and status // 100 != 5:
                if not stream and decode_json and status // 100 == 2:
                    if value:
                        value = json.loads(value)
                    else:
                        value = None
                return (status, reason, hdrs, value)
            if reset_func:
                reset_func()
            self.sleep(2 ** attempt)
        raise Exception('%s %s failed: %s %s' % (method, path, status, reason))

    def get_account_hash(self):
        """
        See :py:func:`swiftly.client.client.Client.get_account_hash`
        """
        return self.storage_path.rsplit('/', 1)[1]
