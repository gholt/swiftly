"""A client that uses the local file system pretending to be Swift.
"""
"""
Copyright 2014 Gregory Holt

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
from json import dumps, loads
from os import listdir, mkdir, rmdir, unlink
from os.path import exists, getsize, isdir, join as path_join, sep as path_sep
from StringIO import StringIO

from swiftly.client.client import Client
from swiftly.client.utils import quote


SUBS = [
    ('_', '__'),
    ('.', '_.'),
    ('/', '_s'),
    ('\\', '_b'),
    (':', '_c'),
    ('*', '_S'),
    ("'", '_q'),
    ('"', '_d'),
    ('?', '_Q'),
    ('<', '_l'),
    ('>', '_g'),
    ('|', '_p')]
"""The list of strings in names to substitute for."""


def _encode_name(name):
    for a, b in SUBS:
        name = name.replace(a, b)
    return name


def _decode_name(name):
    for a, b in SUBS:
        name = name.replace(b, a)
    return name


class LocalClient(Client):
    """A client that uses the local file system pretending to be Swift.

    .. note::

        This is a really early implementation and no-ops a lot of stuff.
        With time it will become a more complete representation.

    :param local_path: This is where the fake Swift will store its data.
        Default: Current working directory.
    :param chunk_size: Maximum size to read or write at one time.
    :param verbose: Set to a ``func(msg, *args)`` that will be called
        with debug messages. Constructing a string for output can be
        done with msg % args.
    :param verbose_id: Set to a string you wish verbose messages to
        be prepended with; can help in identifying output when
        multiple Clients are in use.
    """

    def __init__(self, local_path=None, chunk_size=65536, verbose=None,
                 verbose_id=''):
        super(LocalClient, self).__init__()
        self.local_path = local_path.rstrip(path_sep) if local_path else '.'
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

    def request(self, method, path, contents, headers, decode_json=False,
                stream=False, query=None, cdn=False):
        """
        See :py:func:`swiftly.client.client.Client.request`
        """
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        if isinstance(contents, basestring):
            contents = StringIO(contents)
        if not headers:
            headers = {}
        if not query:
            query = {}
        rpath = path.lstrip('/')
        if '/' in rpath:
            container, obj = rpath.split('/', 1)
        else:
            container = rpath
            obj = ''
        if not container:
            status, reason, hdrs, body = self._account(
                method, contents, headers, stream, query, cdn)
        elif not obj:
            status, reason, hdrs, body = self._container(
                method, container, contents, headers, stream, query, cdn)
        else:
            status, reason, hdrs, body = self._object(
                method, container, obj, contents, headers, stream, query, cdn)
        if status and status // 100 != 5:
            if not stream and decode_json and status // 100 == 2:
                if body:
                    body = loads(body)
                else:
                    body = None
            return (status, reason, hdrs, body)
        raise Exception('%s %s failed: %s %s' % (method, path, status, reason))

    def _account(self, method, contents, headers, stream, query, cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            prefix = query.get('prefix')
            delimiter = query.get('delimiter')
            marker = query.get('marker')
            end_marker = query.get('end_marker')
            limit = query.get('limit')
            containers = set()
            for item in listdir(self.local_path):
                local_path = path_join(self.local_path, item)
                if isdir(local_path):
                    container = _decode_name(item)
                    if prefix and not container.startswith(prefix):
                        continue
                    if delimiter:
                        index = container.find(
                            delimiter, len(prefix) + 1 if prefix else 0)
                        if index >= 0:
                            container = container[:index + 1]
                    containers.add(container)
            containers = sorted(containers)
            if marker:
                containers = [c for c in containers if c > marker]
            if end_marker:
                containers = [c for c in containers if c < end_marker]
            if limit:
                containers = containers[:int(limit)]
            status = 200
            reason = 'OK'
            body = dumps([{'name': c} for c in containers])
            hdrs['content-length'] = str(len(body))
            if method == 'HEAD':
                body = ''
        if stream:
            return status, reason, hdrs, StringIO(body)
        else:
            return status, reason, hdrs, body

    def _container(self, method, container, contents, headers, stream, query,
                   cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        container = _encode_name(container)
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            local_path = path_join(self.local_path, container)
            if not isdir(local_path):
                status = 404
                reason = 'Not Found'
                body = ''
                hdrs['content-length'] = str(len(body))
            else:
                prefix = query.get('prefix')
                delimiter = query.get('delimiter')
                marker = query.get('marker')
                end_marker = query.get('end_marker')
                limit = query.get('limit')
                objects = set()
                for item in listdir(local_path):
                    local_path = path_join(self.local_path, item)
                    if not isdir(local_path):
                        obj = _decode_name(item)
                        if prefix and not obj.startswith(prefix):
                            continue
                        if delimiter:
                            index = obj.find(
                                delimiter, len(prefix) + 1 if prefix else 0)
                            if index >= 0:
                                obj = obj[:index + 1]
                        objects.add(obj)
                objects = sorted(objects)
                if marker:
                    objects = [c for c in objects if c > marker]
                if end_marker:
                    objects = [c for c in objects if c < end_marker]
                if limit:
                    objects = objects[:int(limit)]
                status = 200
                reason = 'OK'
                body = dumps([
                    {'subdir' if o[-1] == delimiter else 'name': o}
                    for o in objects])
                hdrs['content-length'] = str(len(body))
            if method == 'HEAD':
                body = ''
        elif method == 'PUT':
            try:
                mkdir(path_join(self.local_path, container))
            except OSError:
                pass
            status = 201
            reason = 'Created'
            body = ''
            hdrs['content-length'] = str(len(body))
        elif method == 'POST':
            status = 204
            reason = 'No Content'
            body = ''
            hdrs['content-length'] = str(len(body))
        elif method == 'DELETE':
            try:
                rmdir(path_join(self.local_path, container))
            except OSError:
                pass
            status = 204
            reason = 'No Content'
            body = ''
            hdrs['content-length'] = str(len(body))
        if stream:
            return status, reason, hdrs, StringIO(body)
        else:
            return status, reason, hdrs, body

    def _object(self, method, container, obj, contents, headers, stream, query,
                cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        container = _encode_name(container)
        obj = _encode_name(obj)
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            local_path = path_join(self.local_path, container, obj)
            if not exists(local_path):
                status = 404
                reason = 'Not Found'
            else:
                content_length = getsize(local_path)
                hdrs['content-length'] = str(content_length)
                status = 200 if content_length else 204
                if method == 'HEAD':
                    body = StringIO() if stream else ''
                else:
                    body = open(local_path, 'rb')
                    if not stream:
                        body = body.read()
        elif method == 'PUT':
            content_length = headers.get('content-length')
            if content_length is not None:
                content_length = int(content_length)
            fp = open(path_join(self.local_path, container, obj), 'wb')
            left = content_length
            while left is None or left > 0:
                if left is not None:
                    chunk = contents.read(max(left, self.chunk_size))
                    left -= len(chunk)
                else:
                    chunk = contents.read(self.chunk_size)
                if not chunk:
                    break
                fp.write(chunk)
            fp.flush()
            fp.close()
            status = 201
            reason = 'Created'
            body = ''
            hdrs['content-length'] = str(len(body))
        elif method == 'DELETE':
            try:
                unlink(path_join(self.local_path, container, obj))
            except OSError:
                pass
            status = 204
            reason = 'No Content'
            body = ''
            hdrs['content-length'] = str(len(body))
        return status, reason, hdrs, body

    def get_account_hash(self):
        """
        See :py:func:`swiftly.client.client.Client.get_account_hash`
        """
        return quote(self.local_path, safe='')
