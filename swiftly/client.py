"""
Client API to Swift

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

__all__ = ['VERSION', 'CHUNK_SIZE', 'Client']


import hmac
from errno import ENOENT
from hashlib import sha1
from os import umask
from time import time
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


def generate_temp_url(method, url, seconds, key):
    method = method.upper()
    base_url, object_path = url.split('/v1/')
    object_path = '/v1/' + object_path
    expires = int(time() + seconds)
    hmac_body = '%s\n%s\n%s' % (method, expires, object_path)
    sig = hmac.new(key, hmac_body, sha1).hexdigest()
    return '%s%s?temp_url_sig=%s&temp_url_expires=%s' % (
        base_url, object_path, sig, expires)


def _delayed_imports(eventlet=None):
    BadStatusLine = HTTPConnection = HTTPException = HTTPSConnection = \
        sleep = None
    if eventlet is None:
        try:
            from eventlet import __version__
            # Eventlet 0.11.0 fixed the CPU bug
            if __version__ >= '0.11.0':
                eventlet = True
        except ImportError:
            pass
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
    elif not isinstance(value, basestring):
        value = str(value)
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

    def set(self, key, value, serialize=True, timeout=0, time=0):
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

    def incr(self, key, delta=1, timeout=0, time=0):
        node = self.cache.get(key)
        if node:
            node.val += delta
            return node.val
        else:
            self.set(key, delta)
            return delta

    def decr(self, key, delta=1, timeout=0, time=0):
        return self.incr(key, delta=-delta, timeout=timeout, time=time)

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

    def set_multi(self, mapping, server_key, serialize=True, timeout=0,
                  time=0):
        for key, value in mapping.iteritems():
            self.set(key, value)

    def get_multi(self, keys, server_key):
        return [self.get(k) for k in keys]


class _NullLogger(object):

        client_ip = 'client_ip'
        thread_locals = 'thread_locals'
        txn_id = 'txn_id'

        def debug(*args, **kwargs):
                pass

        def error(*args, **kwargs):
                pass

        def exception(*args, **kwargs):
                pass

        def increment(*args, **kwargs):
                pass

        def set_statsd_prefix(*args, **kwargs):
                pass

        def warn(*args, **kwargs):
                pass

        def warning(*args, **kwargs):
                pass


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
    :param cache_path: Default: None. If set to a path, the storage
        URL and auth token are cached in the file for reuse. If there
        is already cached values in the file, they are used without
        authenticating first.
    :param eventlet: Default: None. If True, Eventlet will be used if
        installed. If False, Eventlet will not be used even if
        installed. If None, the default, Eventlet will be used if
        installed and its version is at least 0.11.0 when a CPU usage
        bug was fixed.
    :param swift_proxy_cdn_path: If swift_proxy is set,
        swift_proxy_cdn_path is the path to the Swift account to use
        for CDN management (example: /v1/AUTH_test).
    :param region: The region to access, if supported by auth
        (Example: DFW).
    :param verbose: Set to a ``func(msg, *args)`` that will be called
        with debug messages. Constructing a string for output can be
        done with msg % args.
    :param verbose_id: Set to a string you wish verbose messages to
        be prepended with; can help in identifying output when
        multiple Clients are in use.
    :param auth_tenant: The tenant to authenticate as, if needed.
        Default (if needed): same as auth_user.
    :param auth_methods: Auth methods to use with the auth system,
        example:
        ``auth2key,auth2password,auth2password_force_tenant,auth1`` If
        not specified, the best order will try to be determined; but
        if you notice it keeps making useless auth attempts and that
        drives you crazy, you can override that here. All the
        available auth methods are listed in the example.
    """

    def __init__(self, auth_url=None, auth_user=None, auth_key=None,
                 proxy=None, snet=False, retries=4, swift_proxy=None,
                 swift_proxy_storage_path=None, cache_path=None,
                 eventlet=None, swift_proxy_cdn_path=None, region=None,
                 verbose=None, verbose_id='', auth_tenant=None,
                 auth_methods=None):
        self.auth_url = auth_url
        if self.auth_url:
            self.auth_url = self.auth_url.rstrip('/')
        self.auth_user = auth_user
        self.auth_key = auth_key
        self.auth_tenant = auth_tenant or ''
        self.auth_methods = auth_methods
        self.proxy = proxy
        self.snet = snet
        self.attempts = retries + 1
        self.storage_url = None
        self.storage_conn = None
        self.storage_path = None
        self.cdn_url = None
        self.cdn_conn = None
        self.cdn_path = None
        self.auth_token = None
        self.region = region
        self.regions = []
        self.regions_default = None
        self.verbose = verbose
        self.verbose_id = verbose_id
        self._verbose_id = self.verbose_id
        if self._verbose_id:
            self._verbose_id += ' '
        self.swift_proxy = swift_proxy
        if swift_proxy is True:
            self._verbose('Creating default proxy instance.')
            self.swift_proxy = SwiftProxy({}, memcache=_LocalMemcache(),
                                          logger=_NullLogger())
        if swift_proxy:
            self.storage_path = swift_proxy_storage_path
            self.cdn_path = swift_proxy_cdn_path
        self.cache_path = cache_path
        if self.cache_path:
            try:
                data = open(self.cache_path, 'r').read().decode('base64')
                data = data.split('\n')
                if len(data) == 8:
                    (auth_url, auth_user, auth_key, auth_tenant, region,
                     self.storage_url, self.cdn_url, self.auth_token) = data
                    if auth_url != self.auth_url or \
                            auth_user != self.auth_user or \
                            auth_key != self.auth_key or \
                            auth_tenant != self.auth_tenant or \
                            (self.region and region != self.region):
                        self.storage_url = None
                        self.cdn_url = None
                        self.auth_token = None
                        self._verbose(
                            'Cache %s did not match new settings; discarding.',
                            self.cache_path)
                    else:
                        self._verbose(
                            'Read auth response values from cache %s.',
                            self.cache_path)
                else:
                    self._verbose(
                        'Cache %s was unrecognized format; discarding.',
                        self.cache_path)
            except IOError, err:
                if err.errno == ENOENT:
                    self._verbose('No cached values in %s.', self.cache_path)
                else:
                    self._verbose(
                        'Exception attempting to read auth response values '
                        'from cache %s: %r', self.cache_path, err)
            except Exception, err:
                self._verbose(
                    'Exception attempting to read auth response values from '
                    'cache %s: %r', self.cache_path, err)
        (self.BadStatusLine, self.HTTPConnection, self.HTTPException,
         self.HTTPSConnection, self.sleep) = _delayed_imports(eventlet)

    def _verbose(self, msg, *args):
        if self.verbose:
            self.verbose(self._verbose_id + msg, *args)

    def _connect(self, url=None, cdn=False):
        if not url:
            if cdn:
                if not self.cdn_url:
                    self.auth()
                url = self.cdn_url
            else:
                if not self.storage_url:
                    self.auth()
                url = self.storage_url
        parsed = urlparse(url) if url else None
        proxy_parsed = urlparse(self.proxy) if self.proxy else None
        if not parsed and not proxy_parsed:
            return None, None
        netloc = (proxy_parsed if self.proxy else parsed).netloc
        if parsed.scheme == 'http':
            self._verbose('Establishing HTTP connection to %s', netloc)
            conn = self.HTTPConnection(netloc)
        elif parsed.scheme == 'https':
            self._verbose('Establishing HTTPS connection to %s', netloc)
            conn = self.HTTPSConnection(netloc)
        else:
            raise self.HTTPException(
                'Cannot handle protocol scheme %s for url %s' %
                (parsed.scheme, repr(url)))
        if self.proxy:
            self._verbose(
                'Setting tunnelling to %s:%s', parsed.hostname, parsed.port)
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

    def auth(self):
        """
        Just performs the authentication step without making an
        actual request to the Swift system.
        """
        if not self.auth_url:
            return
        funcs = []
        if self.auth_methods:
            for method in self.auth_methods.split(','):
                funcs.append(getattr(self, '_' + method))
        if not funcs:
            if '1.0' in self.auth_url:
                funcs = [self._auth1, self._auth2key, self._auth2password]
                if not self.auth_tenant:
                    funcs.append(self._auth2password_force_tenant)
            else:
                funcs = [self._auth2key, self._auth2password]
                if not self.auth_tenant:
                    funcs.append(self._auth2password_force_tenant)
                funcs.append(self._auth1)
        info = []
        for func in funcs:
            status, reason = func()
            info.append('%s %s' % (status, reason))
            if status // 100 == 2:
                break
        else:
            raise self.HTTPException('Auth failure %r.' % info)

    def _auth1(self):
        status = 0
        reason = 'Unknown'
        attempt = 0
        while attempt < self.attempts:
            attempt += 1
            self._verbose('Attempting auth v1 with %s', self.auth_url)
            parsed, conn = self._connect(self.auth_url)
            self._verbose('> GET %s', parsed.path)
            conn.request(
                'GET', parsed.path, '',
                {'User-Agent': 'Swiftly v%s' % VERSION,
                 'X-Auth-User': _quote(self.auth_user),
                 'X-Auth-Key': _quote(self.auth_key)})
            try:
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                self._verbose('< %s %s', status, reason)
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
                try:
                    self.storage_url = hdrs['x-storage-url']
                except KeyError:
                    status = 0
                    reason = 'No x-storage-url header'
                    break
                if self.snet:
                    parsed = list(urlparse(self.storage_url))
                    # Second item in the list is the netloc
                    parsed[1] = 'snet-' + parsed[1]
                    self.storage_url = urlunparse(parsed)
                self.cdn_url = hdrs.get('x-cdn-management-url')
                if self.cdn_url and self.snet:
                    parsed = list(urlparse(self.cdn_url))
                    # Second item in the list is the netloc
                    parsed[1] = 'snet-' + parsed[1]
                    self.cdn_url = urlunparse(parsed)
                self.auth_token = hdrs.get('x-auth-token')
                if not self.auth_token:
                    self.auth_token = hdrs.get('x-storage-token')
                    if not self.auth_token:
                        status = 500
                        reason = (
                            'No x-auth-token or x-storage-token header in '
                            'response')
                        break
                if self.cache_path:
                    self._verbose(
                        'Saving auth response values to cache %s.',
                        self.cache_path)
                    data = '\n'.join([
                        self.auth_url, self.auth_user, self.auth_key,
                        self.auth_tenant, self.region, self.storage_url,
                        self.cdn_url or '', self.auth_token])
                    old_umask = umask(0077)
                    open(self.cache_path, 'w').write(data.encode('base64'))
                    umask(old_umask)
                break
            elif status // 100 != 5:
                break
            self.sleep(2 ** attempt)
        return status, reason

    def _auth2key(self):
        return self._auth2('RAX-KSKEY:apiKeyCredentials')

    def _auth2password(self):
        return self._auth2('passwordCredentials')

    def _auth2password_force_tenant(self):
        return self._auth2('passwordCredentials', force_tenant=True)

    def _auth2(self, cred_type, force_tenant=False):
        status = 0
        reason = 'Unknown'
        attempt = 0
        while attempt < self.attempts:
            attempt += 1
            self._verbose(
                'Attempting auth v2 %s with %s', cred_type, self.auth_url)
            parsed, conn = self._connect(self.auth_url)
            data = {'auth': {cred_type: {'username': self.auth_user}}}
            if cred_type == 'RAX-KSKEY:apiKeyCredentials':
                data['auth'][cred_type]['apiKey'] = self.auth_key
            else:
                data['auth'][cred_type]['password'] = self.auth_key
            if self.auth_tenant or force_tenant:
                data['auth']['tenantName'] = self.auth_tenant or self.auth_user
            body = json.dumps(data)
            self._verbose('> POST %s', parsed.path + '/tokens')
            self._verbose('> %s', body)
            conn.request(
                'POST', parsed.path + '/tokens', body,
                {'Content-Type': 'application/json',
                 'User-Agent': 'Swiftly v%s' % VERSION})
            try:
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                self._verbose('< %s %s', status, reason)
                hdrs = self._response_headers(resp.getheaders())
                body = resp.read()
                resp.close()
                conn.close()
            except Exception, err:
                status = 0
                reason = str(err)
                hdrs = {}
            if status == 401:
                break
            if status // 100 == 2:
                try:
                    body = json.loads(body)
                except ValueError, err:
                    status = 500
                    reason = str(err)
                    break
                self.regions = []
                self.regions_default = \
                    body['access']['user']['RAX-AUTH:defaultRegion']
                region = self.region or self.regions_default
                storage_match1 = storage_match2 = storage_match3 = None
                cdn_match1 = cdn_match2 = cdn_match3 = None
                for service in body['access']['serviceCatalog']:
                    if service['type'] == 'object-store':
                        for endpoint in service['endpoints']:
                            if 'region' in endpoint:
                                self.regions.append(endpoint['region'])
                                if endpoint['region'] == region:
                                    storage_match1 = endpoint.get(
                                        'internalURL'
                                        if self.snet else 'publicURL')
                                elif endpoint['region'].lower() == \
                                        region.lower():
                                    storage_match2 = endpoint.get(
                                        'internalURL'
                                        if self.snet else 'publicURL')
                            if not storage_match3:
                                storage_match3 = endpoint.get(
                                    'internalURL'
                                    if self.snet else 'publicURL')
                    elif service['type'] == 'rax:object-cdn':
                        for endpoint in service['endpoints']:
                            if 'region' in endpoint:
                                if endpoint['region'] == region:
                                    cdn_match1 = endpoint.get(
                                        'internalURL'
                                        if self.snet else 'publicURL')
                                elif endpoint['region'].lower() == \
                                        region.lower():
                                    cdn_match2 = endpoint.get(
                                        'internalURL'
                                        if self.snet else 'publicURL')
                            if not cdn_match3:
                                cdn_match3 = endpoint.get(
                                    'internalURL'
                                    if self.snet else 'publicURL')
                self.storage_url = \
                    storage_match1 or storage_match2 or storage_match3
                self.cdn_url = cdn_match1 or cdn_match2 or cdn_match3
                self.auth_token = body['access']['token']['id']
                if not self.storage_url:
                    status = 500
                    reason = (
                        'No storage url resolved from response for region %r '
                        'key %r. Available regions were: %s' %
                        (region, 'internalURL' if self.snet else 'publicURL',
                         ' '.join(self.regions)))
                    break
                if self.cache_path:
                    self._verbose(
                        'Saving auth response values to cache %s.',
                        self.cache_path)
                    data = '\n'.join([
                        self.auth_url, self.auth_user, self.auth_key,
                        self.auth_tenant, self.region, self.storage_url,
                        self.cdn_url or '', self.auth_token])
                    old_umask = umask(0077)
                    open(self.cache_path, 'w').write(data.encode('base64'))
                    umask(old_umask)
                break
            elif status // 100 != 5:
                break
            self.sleep(2 ** attempt)
        return status, reason

    def _default_reset_func(self):
        raise self.HTTPException(
            'Failure and no ability to reset contents for reupload.')

    def _request(self, method, path, contents, headers, decode_json=False,
                 stream=False, query=None, cdn=False):
        if query:
            path += '?' + '&'.join(
                ('%s=%s' % (_quote(k), _quote(v)) if v else _quote(k))
                for k, v in sorted(query.iteritems()))
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
            if cdn:
                conn = self.cdn_conn
                conn_path = self.cdn_path
            else:
                conn = self.storage_conn
                conn_path = self.storage_path
            if not self.swift_proxy and not conn:
                parsed, conn = self._connect(cdn=cdn)
                if conn:
                    if cdn:
                        self.cdn_conn = conn
                        self.cdn_path = conn_path = parsed.path
                    else:
                        self.storage_conn = conn
                        self.storage_path = conn_path = parsed.path
                else:
                    raise self.HTTPException('%s %s failed' % (method, path),
                                             0, 'No connection')
            hdrs = {'User-Agent': 'Swiftly v%s' % VERSION,
                    'X-Auth-Token': self.auth_token}
            if headers:
                hdrs.update(headers)
            resp = None
            if not hasattr(contents, 'read'):
                if self.swift_proxy:
                    req = Request.blank(conn_path + path,
                                        environ={'REQUEST_METHOD': method},
                                        headers=hdrs, body=contents)
                    self._verbose('> %s %s', req.method, req.path)
                    resp = req.get_response(self.swift_proxy)
                else:
                    self._verbose('> %s %s', method, conn_path + path)
                    conn.request(method, conn_path + path, contents, hdrs)
            else:
                req = None
                if self.swift_proxy:
                    req = Request.blank(conn_path + path,
                                        environ={'REQUEST_METHOD': method},
                                        headers=hdrs)
                else:
                    self._verbose('> %s %s', method, conn_path + path)
                    conn.putrequest(method, conn_path + path)
                content_length = None
                for h, v in hdrs.iteritems():
                    if h.lower() == 'content-length':
                        content_length = int(v)
                    if req:
                        req.headers[h] = v
                    else:
                        conn.putheader(h, v)
                if content_length is None:
                    if req:
                        req.headers['Transfer-Encoding'] = 'chunked'
                        req.body_file = contents
                        self._verbose('> %s %s', req.method, req.path)
                        resp = req.get_response(self.swift_proxy)
                    else:
                        conn.putheader('Transfer-Encoding', 'chunked')
                        conn.endheaders()
                        chunk = contents.read(CHUNK_SIZE)
                        while chunk:
                            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
                            chunk = contents.read(CHUNK_SIZE)
                        conn.send('0\r\n\r\n')
                else:
                    if req:
                        req.body_file = contents
                        req.content_length = content_length
                        self._verbose('>: %s %s', req.method, req.path)
                        resp = req.get_response(self.swift_proxy)
                    else:
                        conn.endheaders()
                        left = content_length
                        while left > 0:
                            size = CHUNK_SIZE
                            if size > left:
                                size = left
                            chunk = contents.read(size)
                            conn.send(chunk)
                            left -= len(chunk)
            if not resp:
                try:
                    resp = conn.getresponse()
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
            self._verbose('< %s', resp.status)
            if status == 401:
                if stream:
                    resp.close()
                conn.close()
                conn = None
                self.auth()
                attempt -= 1
            elif status and status // 100 != 5:
                if not stream and decode_json and status // 100 == 2:
                    if value:
                        value = json.loads(value)
                    else:
                        value = None
                return (status, reason, hdrs, value)
            if conn:
                conn.close()
                conn = None
            if reset_func:
                reset_func()
            self.sleep(2 ** attempt)
        raise self.HTTPException('%s %s failed' % (method, path),
                                 status, reason)

    def head_account(self, headers=None, query=None, cdn=False):
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
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request('HEAD', '', '', headers, query=query, cdn=cdn)

    def get_account(self, headers=None, prefix=None, delimiter=None,
                    marker=None, end_marker=None, limit=None, query=None,
                    cdn=False):
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
            indicate how far to progress through container names
            before "rolling them up". For instance, a delimiter='.'
            query on an account with the containers::

                one.one
                one.two
                two
                three.one

            would return the JSON value of::

                [{'subdir': 'one.'},
                 {'count': 0, 'bytes': 0, 'name': 'two'},
                 {'subdir': 'three.'}]

            Using this with prefix can allow you to traverse a psuedo
            hierarchy.
        :param marker: Only container names after this marker will be
            returned. Swift returns a limited number of containers
            per request (often 10,000). To get the next batch of
            names, you issue another query with the marker set to the
            last name you received. You can continue to issue
            requests until you receive no more names.
        :param end_marker: Only container names before this marker will be
            returned.
        :param limit: Limits the size of the list returned per
            request. The default and maximum depends on the Swift
            cluster (usually 10,000).
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        query = dict(query or {})
        query['format'] = 'json'
        if prefix:
            query['prefix'] = prefix
        if delimiter:
            query['delimiter'] = delimiter
        if marker:
            query['marker'] = marker
        if end_marker:
            query['end_marker'] = end_marker
        if limit:
            query['limit'] = limit
        return self._request(
            'GET', '', '', headers, decode_json=True, query=query, cdn=cdn)

    def put_account(self, headers=None, query=None, cdn=False, body=None):
        """
        PUTs the account and returns the results. This is usually
        done with the extract-archive bulk upload request and has no
        other use I know of (but the call is left open in case there
        ever is).

        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some account PUT requests, like the
            extract-archive bulk upload request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'PUT', '', body or '', headers, query=query, cdn=cdn)

    def post_account(self, headers=None, query=None, cdn=False, body=None):
        """
        POSTs the account and returns the results. This is usually
        done to set X-Account-Meta-xxx headers. Note that any existing
        X-Account-Meta-xxx headers will remain untouched. To remove an
        X-Account-Meta-xxx header, send the header with an empty
        string as its value.

        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self._request(
            'POST', '', body or '', headers, query=query, cdn=cdn)

    def delete_account(self, headers=None,
                       yes_i_mean_delete_the_account=False, query=None,
                       cdn=False, body=None):
        """
        Sends a DELETE request to the account and returns the results.

        With ``query['bulk-delete'] = ''`` this might mean a bulk
        delete request where the body of the request is new-line
        separated, url-encoded list of names to delete. Be careful
        with this! One wrong move and you might mark your account for
        deletion of you have the access to do so!

        For a plain DELETE to the account, on clusters that support
        it and, assuming you have permissions to do so, the account
        will be marked as deleted and immediately begin removing the
        objects from the cluster in the backgound.

        THERE IS NO GOING BACK!

        :param headers: Additional headers to send with the request.
        :param yes_i_mean_delete_the_account: Set to True to verify
            you really mean to delete the entire account. This is
            required unless ``body and 'bulk-delete' in query``.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some account DELETE requests, like the bulk
            delete request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be
                a list.
            :contents: is the str for the HTTP body.
        """
        if not yes_i_mean_delete_the_account and (
                not body or not query or 'bulk-delete' not in query):
            return (0, 'yes_i_mean_delete_the_account was not set to True', {},
                    '')
        return self._request(
            'DELETE', '', body or '', headers, query=query, cdn=cdn)

    def _container_path(self, container):
        if container.startswith('/'):
            return _quote(container)
        else:
            return '/' + _quote(container)

    def head_container(self, container, headers=None, query=None, cdn=False):
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
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self._request('HEAD', path, '', headers, query=query, cdn=cdn)

    def get_container(self, container, headers=None, prefix=None,
                      delimiter=None, marker=None, end_marker=None,
                      limit=None, query=None, cdn=False):
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
            indicate how far to progress through object names before
            "rolling them up". For instance, a delimiter='/' query on
            an container with the objects::

                one/one
                one/two
                two
                three/one

            would return the JSON value of::

                [{'subdir': 'one/'},
                 {'count': 0, 'bytes': 0, 'name': 'two'},
                 {'subdir': 'three/'}]

            Using this with prefix can allow you to traverse a psuedo
            hierarchy.
        :param marker: Only object names after this marker will be
            returned. Swift returns a limited number of objects per
            request (often 10,000). To get the next batch of names,
            you issue another query with the marker set to the last
            name you received. You can continue to issue requests
            until you receive no more names.
        :param end_marker: Only object names before this marker will be
            returned.
        :param limit: Limits the size of the list returned per
            request. The default and maximum depends on the Swift
            cluster (usually 10,000).
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        query = dict(query or {})
        query['format'] = 'json'
        if prefix:
            query['prefix'] = prefix
        if delimiter:
            query['delimiter'] = delimiter
        if marker:
            query['marker'] = marker
        if end_marker:
            query['end_marker'] = end_marker
        if limit:
            query['limit'] = limit
        return self._request(
            'GET', self._container_path(container), '', headers,
            decode_json=True, query=query, cdn=cdn)

    def put_container(self, container, headers=None, query=None, cdn=False,
                      body=None):
        """
        PUTs the container and returns the results. This is usually
        done to create new containers and can also be used to set
        X-Container-Meta-xxx headers. Note that if the container
        already exists, any existing X-Container-Meta-xxx headers will
        remain untouched. To remove an X-Container-Meta-xxx header,
        send the header with an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some container PUT requests, like the
            extract-archive bulk upload request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self._request(
            'PUT', path, body or '', headers, query=query, cdn=cdn)

    def post_container(self, container, headers=None, query=None, cdn=False,
                       body=None):
        """
        POSTs the container and returns the results. This is usually
        done to set X-Container-Meta-xxx headers. Note that any
        existing X-Container-Meta-xxx headers will remain untouched.
        To remove an X-Container-Meta-xxx header, send the header with
        an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self._request(
            'POST', path, body or '', headers, query=query, cdn=cdn)

    def delete_container(self, container, headers=None, query=None, cdn=False,
                         body=None):
        """
        DELETEs the container and returns the results.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some container DELETE requests might take a body
            in the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self._request(
            'DELETE', path, body or '', headers, query=query, cdn=cdn)

    def _object_path(self, container, obj):
        container = container.rstrip('/')
        if container.startswith('/'):
            container = _quote(container)
        else:
            container = '/' + _quote(container)
        # Leading/trailing slashes are allowed in object names, so don't strip
        # them.
        return container + '/' + _quote(obj)

    def head_object(self, container, obj, headers=None, query=None, cdn=False):
        """
        HEADs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self._request('HEAD', path, '', headers, query=query, cdn=cdn)

    def get_object(self, container, obj, headers=None, stream=True, query=None,
                   cdn=False):
        """
        GETs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param stream: Indicates whether to stream the contents or
            preread them fully and return them as a str. Default:
            True to stream the contents. When streaming, contents
            will have the standard file-like-object read function,
            which accepts an optional size parameter to limit how
            much data is read per call. When streaming is on, be
            certain to fully read the contents before issuing another
            request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
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
        path = self._object_path(container, obj)
        return self._request(
            'GET', path, '', headers, query=query, stream=stream, cdn=cdn)

    def put_object(self, container, obj, contents, headers=None, query=None,
                   cdn=False):
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
            be a simple str, or a file-like-object with at least a
            read function. If the file-like-object also has tell and
            seek functions, the PUT can be reattempted on any server
            error.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self._request(
            'PUT', path, contents, headers, query=query, cdn=cdn)

    def post_object(self, container, obj, headers=None, query=None, cdn=False,
                    body=None):
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
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self._request(
            'POST', path, body or '', headers, query=query, cdn=cdn)

    def delete_object(self, container, obj, headers=None, query=None,
                      cdn=False, body=None):
        """
        DELETEs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some object DELETE requests might take a body in
            the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self._request(
            'DELETE', path, body or '', headers, query=query, cdn=cdn)
