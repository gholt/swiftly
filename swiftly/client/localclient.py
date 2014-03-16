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
from contextlib import contextmanager
from errno import EAGAIN
from fcntl import flock, LOCK_EX, LOCK_NB
from json import dumps, loads
from os import close as os_close, listdir, mkdir, open as os_open, O_CREAT, \
    O_WRONLY, rename, rmdir, unlink
from os.path import exists, getsize, isdir, isfile, join as path_join, \
    sep as path_sep
from sqlite3 import connect, Row
from StringIO import StringIO
from time import time
from uuid import uuid4

from swiftly.client.client import Client
from swiftly.client.utils import quote

try:
    from eventlet import sleep
except ImportError:
    from time import sleep


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
# Note that _- is reserved for use as the start of internal data file names.


def _encode_name(name):
    for a, b in SUBS:
        name = name.replace(a, b)
    return name


def _decode_name(name):
    for a, b in SUBS:
        name = name.replace(b, a)
    return name


@contextmanager
def lock_dir(path):
    path = path_join(path, '_-lock')
    fd = os_open(path, O_WRONLY | O_CREAT, 0o0600)
    try:
        for x in xrange(100):
            try:
                flock(fd, LOCK_EX | LOCK_NB)
                break
            except IOError as err:
                if err.errno != EAGAIN:
                    raise
            sleep(0.1)
        else:
            raise Exception('Timeout 10s trying to get lock on %r' % path)
        yield True
    finally:
        os_close(fd)


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
            container_name, object_name = rpath.split('/', 1)
        else:
            container_name = rpath
            object_name = ''
        if not container_name:
            status, reason, hdrs, body = self._account(
                method, contents, headers, stream, query, cdn)
        elif not object_name:
            status, reason, hdrs, body = self._container(
                method, container_name, contents, headers, stream, query, cdn)
        else:
            status, reason, hdrs, body = self._object(
                method, container_name, object_name, contents, headers, stream,
                query, cdn)
        if status and status // 100 != 5:
            if not stream and decode_json and status // 100 == 2:
                if body:
                    body = loads(body)
                else:
                    body = None
            return (status, reason, hdrs, body)
        raise Exception('%s %s failed: %s %s' % (method, path, status, reason))

    def _connect(self, db_path):
        db = connect(db_path)
        db.row_factory = Row
        db.text_factory = str
        db.executescript('''
            PRAGMA synchronous = NORMAL;
            PRAGMA count_changes = OFF;
            PRAGMA temp_store = MEMORY;
            PRAGMA journal_mode = DELETE;
        ''')
        return db

    def _get_db(self):
        db_path = path_join(self.local_path, '_-account.db')
        if isfile(db_path):
            return self._connect(db_path)
        with lock_dir(self.local_path):
            if isfile(db_path):
                return self._connect(db_path)
            temp_path = path_join(self.local_path, '_-temp-account.db')
            db = self._connect(temp_path)
            db.executescript('''
                CREATE TABLE account_entry (
                    container_count INTEGER,
                    object_count INTEGER,
                    byte_count INTEGER);

                INSERT INTO account_entry (
                    container_count, object_count, byte_count)
                VALUES (0, 0, 0);

                CREATE TABLE container_entry (
                    container_name TEXT PRIMARY KEY,
                    object_count INTEGER,
                    byte_count INTEGER);

                CREATE TRIGGER container_insert
                AFTER INSERT
                ON container_entry
                BEGIN
                    UPDATE account_entry
                    SET container_count = container_count + 1,
                        object_count = object_count + new.object_count,
                        byte_count = byte_count + new.byte_count;
                END;

                CREATE TRIGGER container_update
                AFTER UPDATE
                ON container_entry
                BEGIN
                    UPDATE account_entry
                    SET object_count = object_count + (
                            new.object_count - old.object_count),
                        byte_count = byte_count + (
                            new.byte_count - old.byte_count);
                END;

                CREATE TRIGGER container_delete
                AFTER DELETE
                ON container_entry
                BEGIN
                    UPDATE account_entry
                    SET container_count = container_count - 1,
                        object_count = object_count - old.object_count,
                        byte_count = byte_count - old.byte_count;
                END;

                CREATE TABLE object_entry (
                    container_name TEXT,
                    object_name TEXT,
                    put_timestamp TEXT,
                    byte_count INTEGER);

                CREATE UNIQUE INDEX object_entry_primary_key
                ON object_entry (container_name, object_name);

                CREATE TRIGGER object_insert
                AFTER INSERT
                ON object_entry
                BEGIN
                    UPDATE container_entry
                    SET object_count = object_count + 1,
                        byte_count = byte_count + new.byte_count
                    WHERE container_name = new.container_name;
                END;

                CREATE TRIGGER object_update
                AFTER UPDATE
                ON object_entry
                BEGIN
                    UPDATE container_entry
                    SET byte_count = byte_count + (
                            new.byte_count - old.byte_count)
                    WHERE container_name = new.container_name;
                END;

                CREATE TRIGGER object_delete
                AFTER DELETE
                ON object_entry
                BEGIN
                    UPDATE container_entry
                    SET object_count = object_count - 1,
                        byte_count = byte_count - old.byte_count
                    WHERE container_name = old.container_name;
                END;
            ''')
            db.commit()
            db.close()
            if isfile(db_path):
                unlink(temp_path)
            else:
                rename(temp_path, db_path)
        return self._connect(db_path)

    def _account(self, method, contents, headers, stream, query, cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            db = self._get_db()
            prefix = query.get('prefix')
            delimiter = query.get('delimiter')
            marker = query.get('marker')
            end_marker = query.get('end_marker')
            limit = int(query.get('limit') or 10000)
            if delimiter and not prefix:
                prefix = ''
            orig_marker = marker
            body = []
            done = False
            while len(body) < limit and not done:
                query = '''
                    SELECT container_name AS name, object_count AS count,
                        byte_count AS bytes
                    FROM container_entry
                '''
                query_args = []
                where = []
                if end_marker:
                    where.append('name < ?')
                    query_args.append(end_marker)
                if marker and marker >= prefix:
                    where.append('name > ?')
                    query_args.append(marker)
                elif prefix:
                    where.append('name >= ?')
                    query_args.append(prefix)
                if where:
                    query += ' WHERE ' + ' AND '.join(where)
                query += ' ORDER BY name LIMIT ? '
                query_args.append(limit - len(body))
                curs = db.execute(query, query_args)
                if prefix is None:
                    body = [dict(r) for r in curs]
                    break
                if not delimiter:
                    if not prefix:
                        body = [dict(r) for r in curs]
                    else:
                        body = [
                            dict(r) for r in curs if r[0].startswith(prefix)]
                    break
                rowcount = 0
                for row in curs:
                    rowcount += 1
                    marker = name = row[0]
                    if len(body) >= limit or not name.startswith(prefix):
                        curs.close()
                        done = True
                        break
                    end = name.find(delimiter, len(prefix))
                    if end > 0:
                        marker = name[:end] + chr(ord(delimiter) + 1)
                        dir_name = name[:end + 1]
                        if dir_name != orig_marker:
                            body.append({'subdir': dir_name})
                        curs.close()
                        break
                    body.append(dict(row))
                if not rowcount:
                    break
            status = 200
            reason = 'OK'
            body = dumps(body)
            hdrs['content-length'] = str(len(body))
            if method == 'HEAD':
                body = ''
            row = db.execute('''
                SELECT container_count, object_count, byte_count
                FROM account_entry
            ''').fetchone()
            hdrs['x-account-container-count'] = row['container_count']
            hdrs['x-account-object-count'] = row['object_count']
            hdrs['x-account-bytes-used'] = row['byte_count']
            db.close()
        if stream:
            return status, reason, hdrs, StringIO(body)
        else:
            return status, reason, hdrs, body

    def _container(self, method, container_name, contents, headers, stream,
                   query, cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        fs_container = _encode_name(container_name)
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            local_path = path_join(self.local_path, fs_container)
            if not isdir(local_path):
                status = 404
                reason = 'Not Found'
                body = ''
                hdrs['content-length'] = str(len(body))
            else:
                object_count = 0
                bytes_used = 0
                prefix = query.get('prefix')
                delimiter = query.get('delimiter')
                marker = query.get('marker')
                end_marker = query.get('end_marker')
                limit = query.get('limit')
                objects = {}
                for item in listdir(local_path):
                    item_local_path = path_join(local_path, item)
                    if isfile(item_local_path):
                        object_name = _decode_name(item)
                        object_count += 1
                        object_size = getsize(item_local_path)
                        bytes_used += object_size
                        if prefix and not object_name.startswith(prefix):
                            continue
                        if delimiter:
                            index = object_name.find(
                                delimiter, len(prefix) + 1 if prefix else 0)
                            if index >= 0:
                                object_name = object_name[:index + 1]
                        objects[object_name] = {
                            'name': object_name, 'bytes': object_size}
                objects = sorted(objects.itervalues(), key=lambda x: x['name'])
                if marker:
                    objects = [o for o in objects if o['name'] > marker]
                if end_marker:
                    objects = [o for o in objects if o['name'] < end_marker]
                if limit:
                    objects = objects[:int(limit)]
                status = 200
                reason = 'OK'
                body = dumps([
                    ({'subdir': o} if o['name'][-1] == delimiter else o)
                    for o in objects])
                hdrs['content-length'] = str(len(body))
                hdrs['x-container-object-count'] = str(object_count)
                hdrs['x-container-bytes-used'] = str(bytes_used)
            if method == 'HEAD':
                body = ''
        elif method == 'PUT':
            fs_container_path = path_join(self.local_path, fs_container)
            if isdir(fs_container_path):
                status = 202
                reason = 'Accepted'
            else:
                db = self._get_db()
                with lock_dir(self.local_path):
                    if isdir(fs_container_path):
                        status = 202
                        reason = 'Accepted'
                    else:
                        mkdir(fs_container_path)
                        db.execute('''
                            INSERT INTO container_entry (
                                container_name, object_count, byte_count)
                            VALUES (?, 0, 0)
                        ''', (container_name,))
                        db.commit()
                        db.close()
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
            fs_container_path = path_join(self.local_path, fs_container)
            if not isdir(fs_container_path):
                status = 404
                reason = 'Not Found'
            else:
                db = self._get_db()
                with lock_dir(self.local_path):
                    if not isdir(fs_container_path):
                        status = 404
                        reason = 'Not Found'
                    else:
                        rmdir(fs_container_path)
                        db.execute('''
                            DELETE FROM container_entry
                            WHERE container_name = ?
                        ''', (container_name,))
                        db.commit()
                        status = 204
                        reason = 'No Content'
            body = ''
            hdrs['content-length'] = str(len(body))
        if stream:
            return status, reason, hdrs, StringIO(body)
        else:
            return status, reason, hdrs, body

    def _object(self, method, container_name, object_name, contents, headers,
                stream, query, cdn):
        if cdn:
            raise Exception('CDN not yet supported with LocalClient')
        fs_container = _encode_name(container_name)
        fs_object = _encode_name(object_name)
        status = 503
        reason = 'Internal Server Error'
        hdrs = {}
        body = ''
        if method in ('GET', 'HEAD'):
            local_path = path_join(self.local_path, fs_container, fs_object)
            if not exists(local_path):
                status = 404
                reason = 'Not Found'
            else:
                content_length = getsize(local_path)
                hdrs['content-length'] = str(content_length)
                status = 200 if content_length else 204
                if method == 'HEAD':
                    body = ''
                else:
                    body = open(local_path, 'rb')
                    if not stream:
                        body = body.read()
        elif method == 'PUT':
            fs_object_path = path_join(
                self.local_path, fs_container, fs_object)
            temp_path = path_join(
                self.local_path, fs_container, '_-temp' + uuid4().hex)
            content_length = headers.get('content-length')
            if content_length is not None:
                content_length = int(content_length)
            fp = open(temp_path, 'wb')
            left = content_length
            written = 0
            while left is None or left > 0:
                if left is not None:
                    chunk = contents.read(max(left, self.chunk_size))
                    left -= len(chunk)
                else:
                    chunk = contents.read(self.chunk_size)
                if not chunk:
                    break
                fp.write(chunk)
                written += len(chunk)
            fp.flush()
            fp.close()
            if content_length is not None and written != content_length:
                unlink(temp_path)
                status = 503
                reason = 'Internal Server Error'
                body = 'Wrote %d bytes when Content-Length was %d' % (
                    written, content_length)
            else:
                db = self._get_db()
                with lock_dir(self.local_path):
                    if isfile(fs_object_path):
                        rename(temp_path, fs_object_path)
                        db.execute('''
                            UPDATE object_entry
                            SET put_timestamp = ?, byte_count = ?
                            WHERE container_name = ? AND object_name = ?
                        ''', (time(), written, container_name, object_name))
                    else:
                        rename(temp_path, fs_object_path)
                        db.execute('''
                            INSERT INTO object_entry (
                                container_name, object_name, put_timestamp,
                                byte_count)
                            VALUES (?, ?, ?, ?)
                        ''', (container_name, object_name, time(), written))
                    db.commit()
                status = 201
                reason = 'Created'
                body = ''
            hdrs['content-length'] = str(len(body))
        elif method == 'DELETE':
            fs_object_path = path_join(
                self.local_path, fs_container, fs_object)
            if not isfile(fs_object_path):
                status = 404
                reason = 'Not Found'
            else:
                db = self._get_db()
                with lock_dir(self.local_path):
                    if not isfile(fs_object_path):
                        status = 404
                        reason = 'Not Found'
                    else:
                        unlink(fs_object_path)
                        db.execute('''
                            DELETE FROM object_entry
                            WHERE container_name = ? AND object_name = ?
                        ''', (container_name, object_name))
                        db.commit()
                        status = 204
                        reason = 'No Content'
            body = ''
            hdrs['content-length'] = str(len(body))
        if stream and not hasattr(body, 'read'):
            body = StringIO(body)
        elif not stream and hasattr(body, 'read'):
            body = body.read()
        return status, reason, hdrs, body

    def get_account_hash(self):
        """
        See :py:func:`swiftly.client.client.Client.get_account_hash`
        """
        return quote(self.local_path, safe='')
