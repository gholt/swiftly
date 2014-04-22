"""
Provides the standard client for accessing Swift services.
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
import errno
import json
import os
import StringIO
import tempfile
import urlparse
from time import time

from swiftly.client.client import Client
from swiftly.client.utils import quote, headers_to_dict


class StandardClient(Client):
    """
    The standard client for accessing Swift services.

    :param auth_methods: Auth methods to use with the auth system,
        example:
        ``auth2key,auth2password,auth2password_force_tenant,auth1`` If
        not specified, the best order will try to be determined; but
        if you notice it keeps making useless auth attempts and that
        drives you crazy, you can override that here. All the
        available auth methods are listed in the example.
    :param auth_url: The URL to the auth system.
    :param auth_tenant: The tenant to authenticate as, if needed.
        Default (if needed): same as auth_user.
    :param auth_user: The user to authenticate as.
    :param auth_key: The key to use when authenticating.
    :param auth_cache_path: Default: None. If set to a path, the
        storage URL and auth token are cached in the file for reuse.
        If there are already cached values in the file, they are used
        without authenticating first.
    :param region: The region to access, if supported by auth
        (Example: DFW).
    :param snet: Uses the internalURL if Auth v2 is used or prepends
        "snet-" to the host name of the storage URL if Auth v1 is
        used. This is usually only useful when working with Rackspace
        Cloud Files and wanting to use Rackspace ServiceNet. Default:
        False.
    :param attempts: The number of times to try requests if a server
        error occurs (5xx response). Default: 5
    :param eventlet: Default: None. If True, Eventlet will be used if
        installed. If False, Eventlet will not be used even if
        installed. If None, the default, Eventlet will be used if
        installed and its version is at least 0.11.0 when a CPU usage
        bug was fixed.
    :param chunk_size: Maximum size to read or write at one time.
    :param http_proxy: The URL to the tunnelling HTTP proxy to use.
        Default: None.
    :param verbose: Set to a ``func(msg, *args)`` that will be called
        with debug messages. Constructing a string for output can be
        done with msg % args.
    :param verbose_id: Set to a string you wish verbose messages to
        be prepended with; can help in identifying output when
        multiple Clients are in use.
    """

    def __init__(self, auth_methods=None, auth_url=None, auth_tenant=None,
                 auth_user=None, auth_key=None, auth_cache_path=None,
                 region=None, snet=False, attempts=5, eventlet=None,
                 chunk_size=65536, http_proxy=None, verbose=None,
                 verbose_id=''):
        super(StandardClient, self).__init__()
        self.auth_methods = auth_methods
        self.auth_url = auth_url.rstrip('/') if auth_url else None
        self.auth_tenant = auth_tenant or ''
        self.auth_user = auth_user
        self.auth_key = auth_key
        self.auth_cache_path = auth_cache_path
        self.region = region
        self.snet = snet
        self.attempts = attempts
        self.chunk_size = chunk_size
        self.http_proxy = http_proxy
        if verbose:
            self.verbose = lambda m, *a, **k: verbose(
                self._verbose_id + m, *a, **k)
        else:
            self.verbose = lambda *a, **k: None
        self.verbose_id = verbose_id
        self._verbose_id = self.verbose_id
        if self._verbose_id:
            self._verbose_id += ' '
        self.auth_token = None
        self.regions = []
        self.default_region = None
        self.storage_url = None
        self.cdn_url = None
        self.conn_discard = None
        self.storage_conn = None
        self.storage_path = None
        self.cdn_conn = None
        self.cdn_path = None
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
                import eventlet.green.httplib
                self.HTTPConnection = eventlet.green.httplib.HTTPConnection
                self.HTTPSConnection = eventlet.green.httplib.HTTPSConnection
                self.HTTPException = eventlet.green.httplib.HTTPException
                try:
                    import swift.common.bufferedhttp
                    self.HTTPConnection = \
                        swift.common.bufferedhttp.BufferedHTTPConnection
                except ImportError:
                    pass
            except ImportError:
                import httplib
                self.HTTPConnection = httplib.HTTPConnection
                self.HTTPSConnection = httplib.HTTPSConnection
                self.HTTPException = httplib.HTTPException
            try:
                import eventlet
                self.sleep = eventlet.sleep
            except ImportError:
                from time import sleep
                self.sleep = sleep
        else:
            import httplib
            self.HTTPConnection = httplib.HTTPConnection
            self.HTTPSConnection = httplib.HTTPSConnection
            self.HTTPException = httplib.HTTPException
            from time import sleep
            self.sleep = sleep
        self._auth_load_cache()

    def _auth_save_cache(self):
        if self.auth_cache_path:
            self.verbose(
                'Saving auth response values to cache %r.',
                self.auth_cache_path)
            data = '\n'.join([
                self.auth_url, self.auth_user, self.auth_key,
                self.auth_tenant or '', self.region or '', self.storage_url,
                self.cdn_url or '', self.auth_token, str(self.snet)])
            fp, path = tempfile.mkstemp()
            os.write(fp, data.encode('base64'))
            os.close(fp)
            os.rename(path, self.auth_cache_path)

    def _auth_load_cache(self):
        if self.auth_cache_path:
            try:
                data = open(self.auth_cache_path, 'r').read().decode('base64')
                data = data.split('\n')
                if len(data) == 9:
                    (auth_url, auth_user, auth_key, auth_tenant, region,
                     self.storage_url, self.cdn_url, self.auth_token,
                     snet) = data
                    snet = snet == 'True'
                    if auth_url != self.auth_url or \
                            auth_user != self.auth_user or \
                            auth_key != self.auth_key or \
                            auth_tenant != self.auth_tenant or \
                            (self.region and region != self.region) or \
                            snet != self.snet:
                        self.storage_url = None
                        self.cdn_url = None
                        self.auth_token = None
                        self.verbose(
                            'Cache %s did not match new settings; discarding.',
                            self.auth_cache_path)
                    else:
                        self.verbose(
                            'Read auth response values from cache %r.',
                            self.auth_cache_path)
                else:
                    self.verbose(
                        'Cache %r was unrecognized format; discarding.',
                        self.auth_cache_path)
            except IOError as err:
                if err.errno == errno.ENOENT:
                    self.verbose(
                        'No cached values in %r.', self.auth_cache_path)
                else:
                    raise
            except Exception as err:
                self.verbose(
                    'Exception attempting to read auth response values from '
                    'cache %r: %r', self.auth_cache_path, err)

    def auth(self):
        """
        See :py:func:`swiftly.client.client.Client.auth`
        """
        self.reset()
        if not self.auth_url:
            raise ValueError('No Auth URL has been provided.')
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
            self.verbose('Attempting auth v1 with %s', self.auth_url)
            parsed, conn = self._connect(self.auth_url)
            self.verbose('> GET %s', parsed.path)
            conn.request(
                'GET', parsed.path, '',
                {'User-Agent': self.user_agent,
                 'X-Auth-User': quote(self.auth_user),
                 'X-Auth-Key': quote(self.auth_key)})
            try:
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                self.verbose('< %s %s', status, reason)
                hdrs = headers_to_dict(resp.getheaders())
                resp.read()
                resp.close()
                conn.close()
            except Exception as err:
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
                    parsed = list(urlparse.urlparse(self.storage_url))
                    # Second item in the list is the netloc
                    parsed[1] = 'snet-' + parsed[1]
                    self.storage_url = urlparse.urlunparse(parsed)
                self.cdn_url = hdrs.get('x-cdn-management-url')
                self.auth_token = hdrs.get('x-auth-token')
                if not self.auth_token:
                    self.auth_token = hdrs.get('x-storage-token')
                    if not self.auth_token:
                        status = 500
                        reason = (
                            'No x-auth-token or x-storage-token header in '
                            'response')
                        break
                self._auth_save_cache()
                break
            elif status // 100 != 5:
                break
            self.client.sleep(2 ** attempt)
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
            self.verbose(
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
            self.verbose('> POST %s', parsed.path + '/tokens')
            self.verbose('> %s', body)
            conn.request(
                'POST', parsed.path + '/tokens', body,
                {'Content-Type': 'application/json',
                 'User-Agent': self.user_agent})
            try:
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                self.verbose('< %s %s', status, reason)
                body = resp.read()
                resp.close()
                conn.close()
            except Exception as err:
                status = 0
                reason = str(err)
            if status == 401:
                break
            if status // 100 == 2:
                # I leave this commented out normally because the response from
                # auth is so huge.
                # self.verbose('< %s', body)
                try:
                    body = json.loads(body)
                except ValueError as err:
                    status = 500
                    reason = str(err)
                    break
                self.regions = []
                self.default_region = \
                    body['access']['user'].get('RAX-AUTH:defaultRegion')
                region = self.region or self.default_region or ''
                storage_match1 = storage_match2 = storage_match3 = \
                    storage_match4 = None
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
                                elif not (region or storage_match4):
                                    storage_match4 = endpoint.get(
                                        'internalURL'
                                        if self.snet else 'publicURL')
                            elif not storage_match3:
                                storage_match3 = endpoint.get(
                                    'internalURL'
                                    if self.snet else 'publicURL')
                    elif service['type'] == 'rax:object-cdn':
                        for endpoint in service['endpoints']:
                            if 'region' in endpoint:
                                if endpoint['region'] == region:
                                    cdn_match1 = endpoint.get('publicURL')
                                elif endpoint['region'].lower() == \
                                        region.lower():
                                    cdn_match2 = endpoint.get('publicURL')
                            elif not cdn_match3:
                                cdn_match3 = endpoint.get('publicURL')
                self.storage_url = \
                    storage_match1 or storage_match2 or storage_match3 \
                    or storage_match4
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
                self._auth_save_cache()
                break
            elif status // 100 != 5:
                break
            self.sleep(2 ** attempt)
        return status, reason

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
        parsed = urlparse.urlparse(url) if url else None
        http_proxy_parsed = \
            urlparse.urlparse(self.http_proxy) if self.http_proxy else None
        if not parsed and not http_proxy_parsed:
            return None, None
        netloc = (http_proxy_parsed if self.http_proxy else parsed).netloc
        if parsed.scheme == 'http':
            self.verbose('Establishing HTTP connection to %s', netloc)
            conn = self.HTTPConnection(netloc)
        elif parsed.scheme == 'https':
            self.verbose('Establishing HTTPS connection to %s', netloc)
            conn = self.HTTPSConnection(netloc)
        else:
            raise self.HTTPException(
                'Cannot handle protocol scheme %s for url %s' %
                (parsed.scheme, repr(url)))
        if self.http_proxy:
            self.verbose(
                'Setting tunnelling to %s:%s', parsed.hostname, parsed.port)
            conn._set_tunnel(parsed.hostname, parsed.port)
        return parsed, conn

    def _default_reset_func(self):
        raise self.HTTPException(
            'Failure and no ability to reset contents for reupload.')

    def request(self, method, path, contents, headers, decode_json=False,
                stream=False, query=None, cdn=False):
        """
        See :py:func:`swiftly.client.client.Client.request`
        """
        if query:
            path += '?' + '&'.join(
                ('%s=%s' % (quote(k), quote(v)) if v else quote(k))
                for k, v in sorted(query.iteritems()))
        reset_func = self._default_reset_func
        if isinstance(contents, basestring):
            contents = StringIO.StringIO(contents)
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
            if time() >= self.conn_discard:
                self.storage_conn = None
                self.cdn_conn = None
            if cdn:
                conn = self.cdn_conn
                conn_path = self.cdn_path
            else:
                conn = self.storage_conn
                conn_path = self.storage_path
            if not conn:
                parsed, conn = self._connect(cdn=cdn)
                if conn:
                    if cdn:
                        self.cdn_conn = conn
                        self.cdn_path = conn_path = parsed.path
                    else:
                        self.storage_conn = conn
                        self.storage_path = conn_path = parsed.path
                else:
                    raise self.HTTPException(
                        '%s %s failed: No connection' % (method, path))
            self.conn_discard = time() + 4
            titled_headers = dict((k.title(), v) for k, v in {
                'User-Agent': self.user_agent,
                'X-Auth-Token': self.auth_token}.iteritems())
            if headers:
                titled_headers.update(
                    (k.title(), v) for k, v in headers.iteritems())
            try:
                if not hasattr(contents, 'read'):
                    if method not in self.no_content_methods and contents and \
                            'Content-Length' not in titled_headers and \
                            'Transfer-Encoding' not in titled_headers:
                        titled_headers['Content-Length'] = str(
                            len(contents or ''))
                    verbose_headers = '  '.join(
                        '%s: %s' % (k, v)
                        for k, v in sorted(titled_headers.iteritems()))
                    self.verbose(
                        '> %s %s %s', method, conn_path + path,
                        verbose_headers)
                    conn.request(
                        method, conn_path + path, contents, titled_headers)
                else:
                    conn.putrequest(method, conn_path + path)
                    content_length = None
                    for h, v in sorted(titled_headers.iteritems()):
                        if h == 'Content-Length':
                            content_length = int(v)
                        conn.putheader(h, v)
                    if method not in self.no_content_methods and \
                            content_length is None:
                        titled_headers['Transfer-Encoding'] = 'chunked'
                        conn.putheader('Transfer-Encoding', 'chunked')
                    conn.endheaders()
                    verbose_headers = '  '.join(
                        '%s: %s' % (k, v)
                        for k, v in sorted(titled_headers.iteritems()))
                    self.verbose(
                        '> %s %s %s', method, conn_path + path,
                        verbose_headers)
                    if method not in self.no_content_methods and \
                            content_length is None:
                        chunk = contents.read(self.chunk_size)
                        while chunk:
                            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
                            chunk = contents.read(self.chunk_size)
                        conn.send('0\r\n\r\n')
                    else:
                        left = content_length
                        while left > 0:
                            size = self.chunk_size
                            if size > left:
                                size = left
                            chunk = contents.read(size)
                            if not chunk:
                                raise IOError('Early EOF from input')
                            conn.send(chunk)
                            left -= len(chunk)
                resp = conn.getresponse()
                status = resp.status
                reason = resp.reason
                hdrs = headers_to_dict(resp.getheaders())
                if stream:
                    value = resp
                else:
                    value = resp.read()
                    resp.close()
            except Exception as err:
                status = 0
                reason = '%s %s' % (type(err), str(err))
                hdrs = {}
                value = None
            self.verbose('< %s %s', status or '-', reason)
            if status == 401:
                if stream:
                    value.close()
                conn.close()
                self.auth()
                attempt -= 1
            elif status and status // 100 != 5:
                if not stream and decode_json and status // 100 == 2:
                    if value:
                        value = json.loads(value)
                    else:
                        value = None
                self.conn_discard = time() + 4
                return (status, reason, hdrs, value)
            else:
                if stream and value:
                    value.close()
                conn.close()
            if reset_func:
                reset_func()
            self.sleep(2 ** attempt)
        raise self.HTTPException(
            '%s %s failed: %s %s' % (method, path, status, reason))

    def reset(self):
        """
        See :py:func:`swiftly.client.client.Client.reset`
        """
        for conn in (self.storage_conn, self.cdn_conn):
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        self.storage_conn = None
        self.cdn_conn = None

    def get_account_hash(self):
        """
        See :py:func:`swiftly.client.client.Client.get_account_hash`
        """
        return (self.storage_url or self.storage_path).rsplit('/', 1)[1]
