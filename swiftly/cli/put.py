"""
Contains a CLICommand that can issue PUT requests.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

===============  ====================================================
cdn              True if the CDN Management URL should be used
                 instead of the Storage URL.
client_manager   For connecting to Swift.
concurrency      The number of concurrent actions that can be
                 performed.
different        Set to True to check if the local file is different
                 than an existing object before uploading.
empty            Set to True if you wish to send an empty body with
                 the PUT rather than reading from the io_manager's
                 stdin.
headers          A dict of headers to send.
input\_          A string representing where input should be obtained
                 from. If None, the io_manager's stdin will be used.
                 If a directory path is specified, a set of PUTs will
                 be generated for each item in the directory
                 structure. If a file path is specified, that single
                 file will be used as input.
io_manager       For directing output and obtaining input if needed.
newer            Set to True to check if the local file is newer than
                 an existing object before uploading.
query            A dict of query parameters to send.
seek             Where to seek to in the input\_ before uploading;
                 usually just used by recursive calls with segmented
                 objects.
segment_size     The max size of a file before switching to a
                 segmented object and the max size of each object
                 segment.
static_segments  Set to True to use static large object support
                 instead of dynamic large object support.
===============  ====================================================
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
import os
import time

from swiftly.cli.command import CLICommand, ReturnCode
from swiftly.concurrency import Concurrency
from swiftly.dencrypt import AES256CBC, aes_encrypt
from swiftly.filelikeiter import FileLikeIter


def cli_put_directory_structure(context, path):
    """
    Performs PUTs rooted at the path using a directory structure
    pointed to by context.input\_.

    See :py:mod:`swiftly.cli.put` for context usage information.

    See :py:class:`CLIPut` for more information.
    """
    if not context.input_:
        raise ReturnCode(
            'called cli_put_directory_structure without context.input_ set')
    if not os.path.isdir(context.input_):
        raise ReturnCode(
            '%r is not a directory' % context.input_)
    if not path:
        raise ReturnCode(
            'uploading a directory structure requires at least a container '
            'name')
    new_context = context.copy()
    new_context.input_ = None
    container = path.split('/', 1)[0]
    cli_put_container(new_context, container)
    ilen = len(context.input_)
    if not context.input_.endswith(os.sep):
        ilen += 1
    conc = Concurrency(context.concurrency)
    for (dirpath, dirnames, filenames) in os.walk(context.input_):
        if not dirnames and not filenames:
            new_context = context.copy()
            new_context.headers = dict(context.headers)
            new_context.headers['content-type'] = 'text/directory'
            new_context.headers['x-object-meta-mtime'] = \
                '%f' % os.path.getmtime(context.input_)
            new_context.input_ = None
            new_context.empty = True
            new_path = path
            if path[-1] != '/':
                new_path += '/'
            new_path += dirpath[ilen:]
            for (exc_type, exc_value, exc_tb, result) in \
                    six.itervalues(conc.get_results()):
                if exc_value:
                    conc.join()
                    raise exc_value
            conc.spawn(new_path, cli_put_object, new_context, new_path)
        else:
            for fname in filenames:
                new_context = context.copy()
                new_context.input_ = os.path.join(dirpath, fname)
                new_path = path
                if path[-1] != '/':
                    new_path += '/'
                if dirpath[ilen:]:
                    new_path += dirpath[ilen:] + '/'
                new_path += fname
                for (exc_type, exc_value, exc_tb, result) in \
                        six.itervalues(conc.get_results()):
                    if exc_value:
                        conc.join()
                        raise exc_value
                conc.spawn(new_path, cli_put_object, new_context, new_path)
    conc.join()
    for (exc_type, exc_value, exc_tb, result) in \
            six.itervalues(conc.get_results()):
        if exc_value:
            raise exc_value


def cli_put_account(context):
    """
    Performs a PUT on the account.

    See :py:mod:`swiftly.cli.put` for context usage information.

    See :py:class:`CLIPut` for more information.
    """
    body = None
    if context.input_:
        if context.input_ == '-':
            body = context.io_manager.get_stdin()
        else:
            body = open(context.input_, 'rb')
    with context.client_manager.with_client() as client:
        status, reason, headers, contents = client.put_account(
            headers=context.headers, query=context.query, cdn=context.cdn,
            body=body)
        if hasattr(contents, 'read'):
            contents.read()
    if status // 100 != 2:
        raise ReturnCode('putting account: %s %s' % (status, reason))


def cli_put_container(context, path):
    """
    Performs a PUT on the container.

    See :py:mod:`swiftly.cli.put` for context usage information.

    See :py:class:`CLIPut` for more information.
    """
    path = path.rstrip('/')
    if '/' in path:
        raise ReturnCode('called cli_put_container with object %r' % path)
    body = None
    if context.input_:
        if context.input_ == '-':
            body = context.io_manager.get_stdin()
        else:
            body = open(context.input_, 'rb')
    with context.client_manager.with_client() as client:
        status, reason, headers, contents = client.put_container(
            path, headers=context.headers, query=context.query,
            cdn=context.cdn, body=body)
        if hasattr(contents, 'read'):
            contents.read()
    if status // 100 != 2:
        raise ReturnCode(
            'putting container %r: %s %s' % (path, status, reason))


def cli_put_object(context, path):
    """
    Performs a PUT on the object.

    See :py:mod:`swiftly.cli.put` for context usage information.

    See :py:class:`CLIPut` for more information.
    """
    if context.different and context.encrypt:
        raise ReturnCode(
            'context.different will not work properly with context.encrypt '
            'since encryption may change the object size')
    put_headers = dict(context.headers)
    if context.empty:
        body = ''
        put_headers['content-length'] = '0'
    elif not context.input_ or context.input_ == '-':
        stdin = context.io_manager.get_stdin()

        if context.stdin_segmentation:
            def reader():
                while True:
                    chunk = stdin.read(65536)
                    if chunk:
                        yield chunk
                    else:
                        return

            segment_body = FileLikeIter(reader(), context.segment_size)

            prefix = _create_container(context, path, time.time(), 0)
            new_context = context.copy()
            new_context.stdin_segmentation = False
            new_context.stdin = segment_body
            new_context.headers = dict(context.headers)
            segment_n = 0
            path2info = {}
            while not segment_body.is_empty():
                segment_path = _get_segment_path(prefix, segment_n)
                etag = cli_put_object(new_context, segment_path)
                size = segment_body.limit - segment_body.left
                path2info[segment_path] = (size, etag)

                segment_body.reset_limit()
                segment_n += 1
            body = _get_manifest_body(context, prefix, path2info, put_headers)
        else:
            if hasattr(context, 'stdin'):
                body = context.stdin
            else:
                body = stdin
    elif context.seek is not None:
        if context.encrypt:
            raise ReturnCode(
                'putting object %r: Cannot use encryption and context.seek' %
                path)
        body = open(context.input_, 'rb')
        body.seek(context.seek)
    else:
        l_mtime = os.path.getmtime(context.input_)
        l_size = os.path.getsize(context.input_)
        put_headers['content-length'] = str(l_size)
        if context.newer or context.different:
            r_mtime = None
            r_size = None
            with context.client_manager.with_client() as client:
                status, reason, headers, contents = client.head_object(
                    *path.split('/', 1), headers=context.headers,
                    query=context.query, cdn=context.cdn)
                if hasattr(contents, 'read'):
                    contents.read()
            if status // 100 == 2:
                r_mtime = headers.get('x-object-meta-mtime')
                if r_mtime:
                    try:
                        r_mtime = float(r_mtime)
                    except ValueError:
                        r_mtime = None
                r_size = headers.get('content-length')
                if r_size:
                    try:
                        r_size = int(r_size)
                    except ValueError:
                        r_size = None
            elif status != 404:
                raise ReturnCode(
                    'could not head %r for conditional check; skipping put: '
                    '%s %s' % (path, status, reason))
            if context.newer and r_mtime is not None or l_mtime <= r_mtime:
                return
            if context.different and r_mtime is not None and \
                    l_mtime == r_mtime and r_size is not None and \
                    l_size == r_size:
                return
        put_headers['x-object-meta-mtime'] = '%f' % l_mtime
        size = os.path.getsize(context.input_)
        if size > context.segment_size:
            if context.encrypt:
                raise ReturnCode(
                    'putting object %r: Cannot use encryption for objects '
                    'greater than the segment size' % path)
            prefix = _create_container(context, path, l_mtime, size)
            conc = Concurrency(context.concurrency)
            start = 0
            segment = 0
            path2info = {}
            while start < size:
                new_context = context.copy()
                new_context.headers = dict(context.headers)
                new_context.headers['content-length'] = str(min(
                    size - start, context.segment_size))
                new_context.seek = start
                new_path = _get_segment_path(prefix, segment)
                for (ident, (exc_type, exc_value, exc_tb, result)) in \
                        six.iteritems(conc.get_results()):
                    if exc_value:
                        conc.join()
                        raise exc_value
                    path2info[ident] = result
                conc.spawn(
                    new_path, cli_put_object, new_context, new_path)
                segment += 1
                start += context.segment_size
            conc.join()
            for (ident, (exc_type, exc_value, exc_tb, result)) in \
                    six.iteritems(conc.get_results()):
                if exc_value:
                    raise exc_value
                path2info[ident] = result
            body = _get_manifest_body(context, prefix, path2info, put_headers)
        else:
            body = open(context.input_, 'rb')
    with context.client_manager.with_client() as client:
        if context.encrypt:
            content_length = put_headers.get('content-length')
            if content_length:
                content_length = int(content_length)
            if hasattr(body, 'read'):
                body = FileLikeIter(aes_encrypt(
                    context.encrypt, body, preamble=AES256CBC,
                    chunk_size=getattr(client, 'chunk_size', 65536),
                    content_length=content_length))
            else:
                body = FileLikeIter(aes_encrypt(
                    context.encrypt, FileLikeIter([body]), preamble=AES256CBC,
                    chunk_size=getattr(client, 'chunk_size', 65536),
                    content_length=content_length))
            if 'content-length' in put_headers:
                del put_headers['content-length']
        container, obj = path.split('/', 1)
        status, reason, headers, contents = client.put_object(
            container, obj, body, headers=put_headers, query=context.query,
            cdn=context.cdn)
        if hasattr(contents, 'read'):
            contents = contents.read()
    if status // 100 != 2:
        raise ReturnCode(
            'putting object %r: %s %s %r' % (path, status, reason, contents))
    if context.seek is not None:
        content_length = put_headers.get('content-length')
        etag = headers.get('etag')
        if content_length and etag:
            content_length = int(content_length)
        else:
            with context.client_manager.with_client() as client:
                container, obj = path.split('/', 1)
                status, reason, headers, contents = client.head_object(
                    container, obj, cdn=context.cdn)
                if hasattr(contents, 'read'):
                    contents = contents.read()
            if status // 100 != 2:
                raise ReturnCode(
                    'heading object %r: %s %s %r' %
                    (path, status, reason, contents))
            content_length = headers.get('content-length')
            etag = headers.get('etag')
            if content_length:
                content_length = int(content_length)
        return content_length, etag
    if context.stdin is not None:
        return headers.get('etag')


def cli_put(context, path):
    """
    Performs a PUT on the item (account, container, or object).

    See :py:mod:`swiftly.cli.put` for context usage information.

    See :py:class:`CLIPut` for more information.
    """
    path = path.lstrip('/') if path else ''
    if context.input_ and os.path.isdir(context.input_):
        return cli_put_directory_structure(context, path)
    if not path:
        return cli_put_account(context)
    elif '/' not in path.rstrip('/'):
        return cli_put_container(context, path)
    else:
        return cli_put_object(context, path)


def _get_segment_path(prefix, n):
    """
    Returns segment path for nth segment
    """
    return '%s%08d' % (prefix, n)


def _get_manifest_body(context, prefix, path2info, put_headers):
    """
    Returns body for manifest file and modifies put_headers.

    path2info is a dict like {"path": (size, etag)}
    """
    if context.static_segments:
        body = json.dumps([
            {'path': '/' + p, 'size_bytes': s, 'etag': e}
            for p, (s, e) in sorted(six.iteritems(path2info))
        ])
        put_headers['content-length'] = str(len(body))
        context.query['multipart-manifest'] = 'put'
    else:
        body = ''
        put_headers['content-length'] = '0'
        put_headers['x-object-manifest'] = prefix

    return body


def _create_container(context, path, l_mtime, size):
    """
    Creates container for segments of file with `path`
    """
    new_context = context.copy()
    new_context.input_ = None
    new_context.headers = None
    new_context.query = None
    container = path.split('/', 1)[0] + '_segments'
    cli_put_container(new_context, container)
    prefix = container + '/' + path.split('/', 1)[1]
    prefix = '%s/%s/%s/' % (prefix, l_mtime, size)

    return prefix


class CLIPut(CLICommand):
    """
    A CLICommand that can issue PUT requests.

    See the output of ``swiftly help put`` for more information.
    """

    def __init__(self, cli):
        super(CLIPut, self).__init__(
            cli, 'put', max_args=1, usage="""
Usage: %prog [main_options] put [options] [path]

For help on [main_options] run %prog with no args.

Performs a PUT request on the <path> given. If the <path> is an object, the
contents for the object are read from standard input.

Special Note About Segmented Objects:

For object uploads exceeding the -s [size] (default: 5G) the object will be
uploaded in segments. At this time, auto-segmenting only works for objects
uploaded from source files -- objects sourced from standard input cannot exceed
the maximum object size for the cluster.

A segmented object is one that has its contents in several other objects. On
download, these other objects are concatenated into a single object stream.

Segmented objects can be useful to greatly exceed the maximum single object
size, speed up uploading large objects with concurrent segment uploading, and
provide the option to replace, insert, and delete segments within a whole
object without having to alter or reupload any of the other segments.

The main object of a segmented object is called the "manifest object". This
object just has an X-Object-Manifest header that points to another path where
the segments for the object contents are stored. For Swiftly, this header value
is auto-generated as the same name as the manifest object, but with "_segments"
added to the container name. This keeps the segments out of the main container
listing, which is often useful.

By default, Swift's dynamic large object support is used since it was
implemented first. However, if you prefix the [size] with an 's', as in '-s
s1048576' Swiftly will use static large object support. These static large
objects are very similar as described above, except the manifest contains a
static list of the object segments. For more information on the tradeoffs, see
http://greg.brim.net/post/2013/05/16/1834.html""".strip())
        self.option_parser.add_option(
            '-h', '-H', '--header', dest='header', action='append',
            metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hx-object-meta-color:blue -h "Content-Type: text/html"')
        self.option_parser.add_option(
            '-q', '--query', dest='query', action='append',
            metavar='NAME[=VALUE]',
            help='Add a query parameter to the request. This can be used '
                 'multiple times for multiple query parameters. Example: '
                 '-qmultipart-manifest=get')
        self.option_parser.add_option(
            '-i', '--input', dest='input_', metavar='PATH',
            help='Indicates where to read the contents from; default is '
                 'standard input. If the PATH is a directory, all files in '
                 'the directory will be uploaded as similarly named objects '
                 'and empty directories will create text/directory marker '
                 'objects. Use a dash (as in "-i -") to specify standard '
                 'input for account and container PUTs, as those do not '
                 'normally take input. This is useful with '
                 '-qextract-archive=<format> bulk upload requests. For '
                 'example: tar zc . | swiftly put -qextract-archive=tar.gz -i '
                 '- container')
        self.option_parser.add_option(
            '-n', '--newer', dest='newer', action='store_true',
            help='For PUTs with an --input option, first performs a HEAD on '
                 'the object and compares the X-Object-Meta-Mtime header with '
                 'the modified time of the PATH obtained from the --input '
                 'option and then PUTs the object only if the local time is '
                 'newer. When the --input PATH is a directory, this offers an '
                 'easy way to upload only the newer files since the last '
                 'upload (at the expense of HEAD requests). NOTE THAT THIS '
                 'WILL NOT UPLOAD CHANGED FILES THAT DO NOT HAVE A NEWER '
                 'LOCAL MODIFIED TIME! NEWER does not mean DIFFERENT.')
        self.option_parser.add_option(
            '-d', '--different', dest='different', action='store_true',
            help='For PUTs with an --input option, first performs a HEAD on '
                 'the object and compares the X-Object-Meta-Mtime header with '
                 'the modified time of the PATH obtained from the --input '
                 'option and then PUTs the object only if the local time is '
                 'different. It will also check the local and remote sizes '
                 'and PUT if they differ. ETag/MD5sum checking are not done '
                 '(an option may be provided in the future) since this is '
                 'usually much more disk intensive. When the --input PATH is '
                 'a directory, this offers an easy way to upload only the '
                 'differing files since the last upload (at the expense of '
                 'HEAD requests). NOTE THAT THIS CAN UPLOAD OLDER FILES OVER '
                 'NEWER ONES! DIFFERENT does not mean NEWER.')
        self.option_parser.add_option(
            '-e', '--empty', dest='empty', action='store_true',
            help='Indicates a zero-byte object should be PUT.')
        self.option_parser.add_option(
            '-s', '--segment-size', dest='segment_size', metavar='BYTES',
            help='Indicates the maximum size of an object before uploading it '
                 'as a segmented object. See full help text for more '
                 'information.')
        self.option_parser.add_option(
            '--stdin-segmentation', dest='stdin_segmentation', action='store_true',
            help='Separate STDIN data into segments. This will separate data'
            'even if segment size is not exceeded.')
        self.option_parser.add_option(
            '--encrypt', dest='encrypt', metavar='KEY',
            help='Will encrypt the uploaded object data with KEY. This '
                 'currently uses AES 256 in CBC mode but other algorithms may '
                 'be offered in the future. You may specify a single dash "-" '
                 'as the KEY and instead the KEY will be loaded from the '
                 'SWIFTLY_CRYPT_KEY environment variable.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        context.input_ = options.input_
        context.segment_size = options.segment_size
        context.static_segments = False
        if context.segment_size and context.segment_size[0].lower() == 's':
            context.static_segments = True
            context.segment_size = context.segment_size[1:]
        context.segment_size = int(
            context.segment_size or 5 * 1024 * 1024 * 1024)
        if context.segment_size < 1:
            raise ReturnCode('invalid segment size %s' % options.segment_size)
        context.stdin_segmentation = options.stdin_segmentation
        context.empty = options.empty
        context.newer = options.newer
        context.different = options.different
        context.encrypt = options.encrypt
        if context.encrypt == '-':
            context.encrypt = os.environ.get('SWIFTLY_CRYPT_KEY')
            if not context.encrypt:
                raise ReturnCode(
                    'A single dash "-" was given as the encryption key, but '
                    'no key was found in the SWIFTLY_CRYPT_KEY environment '
                    'variable.')
        if context.encrypt and context.different:
            raise ReturnCode(
                '--different will not work properly with --encrypt since '
                'encryption may change the object size')
        path = args.pop(0).lstrip('/') if args else None
        return cli_put(context, path)
