"""
Contains a CLICommand that can issue GET requests.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

=======================  ============================================
all_objects              True if instead of the listing itself you
                         want all objects that the listing references
                         to be output.
cdn                      True if the CDN Management URL should be
                         used instead of the Storage URL.
client_manager           For connecting to Swift.
concurrency              The number of concurrent actions that can be
                         performed.
full                     True if you want a full listing (additional
                         information like object count, bytes used,
                         and upload date) instead of just the item
                         names.
headers                  A dict of headers to send.
ignore_404               True if 404s should be silently ignored.
io_manager               For directing output.
muted_account_headers    The headers to omit when outputting account
                         response headers.
muted_container_headers  The headers to omit when outputting
                         container response headers.
muted_object_headers     The headers to omit when outputting object
                         response headers.
output_headers           True if you want the headers from the
                         response to also be output.
query                    A dict of query parameters to send. Of
                         important use are limit, delimiter, prefix,
                         marker, and end_marker as they are common
                         listing query parameters.
raw                      Normally the result of the GET is translated
                         as a listing and formatted output is
                         generated. Setting this to True will skip
                         any formatting a just output the raw
                         contents from the response. Note that this
                         will also just issue a single request and
                         will not try additional follow-on marker
                         requests.
remove_empty_files       True if files created on disk should be
                         removed if they result in an empty file.
                         This can be useful with sub_commands that
                         only output information for matches.
suppress_container_name  True if files created on disk should have
                         the container name stripped from the file
                         name. When downloading a single container,
                         this is usually desired.
write_headers            A function used to output the response
                         headers if output_headers is set True.
=======================  ============================================
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
import os
import time

from swiftly.cli.command import CLICommand, ReturnCode
from swiftly.concurrency import Concurrency
from swiftly.dencrypt import AES256CBC, aes_decrypt
from swiftly.filelikeiter import FileLikeIter


def cli_get_account_listing(context):
    """
    Performs a GET on the account as a listing request.

    See :py:mod:`swiftly.cli.get` for context usage information.

    See :py:class:`CLIGet` for more information.
    """
    limit = context.query.get('limit')
    delimiter = context.query.get('delimiter')
    prefix = context.query.get('prefix')
    marker = context.query.get('marker')
    end_marker = context.query.get('end_marker')
    if context.raw:
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_account(
                decode_json=False, headers=context.headers, limit=limit,
                marker=marker, end_marker=end_marker, query=context.query,
                cdn=context.cdn)
            if hasattr(contents, 'read'):
                contents = contents.read()
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            raise ReturnCode('listing account: %s %s' % (status, reason))
        with context.io_manager.with_stdout() as fp:
            if context.output_headers:
                context.write_headers(
                    fp, headers, context.muted_account_headers)
            fp.write(contents)
            fp.flush()
        return
    with context.client_manager.with_client() as client:
        status, reason, headers, contents = client.get_account(
            headers=context.headers, limit=limit, marker=marker,
            end_marker=end_marker, query=context.query, cdn=context.cdn)
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            if hasattr(contents, 'read'):
                contents.read()
            raise ReturnCode('listing account: %s %s' % (status, reason))
    if context.output_headers and not context.all_objects:
        with context.io_manager.with_stdout() as fp:
            context.write_headers(
                fp, headers, context.muted_account_headers)
    while contents:
        if context.all_objects:
            new_context = context.copy()
            new_context.query = dict(new_context.query)
            for remove in (
                    'limit', 'delimiter', 'prefix', 'marker', 'end_marker'):
                if remove in new_context.query:
                    del new_context.query[remove]
            for item in contents:
                if 'name' in item:
                    new_path = item['name'].encode('utf8')
                    cli_get_container_listing(new_context, new_path)
        else:
            with context.io_manager.with_stdout() as fp:
                for item in contents:
                    if context.full:
                        fp.write('%13s %13s ' % (
                            item.get('bytes', '-'),
                            item.get('count', '-')))
                    fp.write(item.get(
                        'name', item.get('subdir')).encode('utf8'))
                    fp.write('\n')
                fp.flush()
        if limit:
            break
        marker = contents[-1].get('name', contents[-1].get('subdir', ''))
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_account(
                headers=context.headers, limit=limit, delimiter=delimiter,
                prefix=prefix, end_marker=end_marker, marker=marker,
                query=context.query, cdn=context.cdn)
            if status // 100 != 2:
                if status == 404 and context.ignore_404:
                    return
                if hasattr(contents, 'read'):
                    contents.read()
                raise ReturnCode('listing account: %s %s' % (status, reason))


def cli_get_container_listing(context, path=None):
    """
    Performs a GET on the container as a listing request.

    See :py:mod:`swiftly.cli.get` for context usage information.

    See :py:class:`CLIGet` for more information.
    """
    path = path.strip('/') if path else None
    if not path or '/' in path:
        raise ReturnCode(
            'tried to get a container listing for non-container path %r' %
            path)
    context.suppress_container_name = True
    limit = context.query.get('limit')
    delimiter = context.query.get('delimiter')
    prefix = context.query.get('prefix')
    marker = context.query.get('marker')
    end_marker = context.query.get('end_marker')
    if context.raw:
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_container(
                path, decode_json=False, headers=context.headers, limit=limit,
                marker=marker, end_marker=end_marker, query=context.query,
                cdn=context.cdn)
            if hasattr(contents, 'read'):
                contents = contents.read()
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            raise ReturnCode(
                'listing container %r: %s %s' % (path, status, reason))
        with context.io_manager.with_stdout() as fp:
            if context.output_headers:
                context.write_headers(
                    fp, headers, context.muted_container_headers)
            fp.write(contents)
            fp.flush()
        return
    with context.client_manager.with_client() as client:
        status, reason, headers, contents = client.get_container(
            path, headers=context.headers, limit=limit, delimiter=delimiter,
            prefix=prefix, marker=marker, end_marker=end_marker,
            query=context.query, cdn=context.cdn)
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            if hasattr(contents, 'read'):
                contents.read()
            raise ReturnCode(
                'listing container %r: %s %s' % (path, status, reason))
    if context.output_headers and not context.all_objects:
        with context.io_manager.with_stdout() as fp:
            context.write_headers(
                fp, headers, context.muted_container_headers)
    conc = Concurrency(context.concurrency)
    while contents:
        if context.all_objects:
            new_context = context.copy()
            new_context.query = dict(new_context.query)
            for remove in (
                    'limit', 'delimiter', 'prefix', 'marker', 'end_marker'):
                if remove in new_context.query:
                    del new_context.query[remove]
            for item in contents:
                if 'name' in item:
                    for (exc_type, exc_value, exc_tb, result) in \
                            conc.get_results().itervalues():
                        if exc_value:
                            conc.join()
                            raise exc_value
                    new_path = path + '/' + item['name'].encode('utf8')
                    conc.spawn(new_path, cli_get, new_context, new_path)
        else:
            with context.io_manager.with_stdout() as fp:
                for item in contents:
                    if context.full:
                        fp.write('%13s %22s %32s %25s ' % (
                            item.get('bytes', '-'),
                            item.get('last_modified', '-')[:22].replace(
                                'T', ' '),
                            item.get('hash', '-'),
                            item.get('content_type', '-')))
                    fp.write(item.get(
                        'name', item.get('subdir')).encode('utf8'))
                    fp.write('\n')
                fp.flush()
        if limit:
            break
        marker = contents[-1].get('name', contents[-1].get('subdir', ''))
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_container(
                path, headers=context.headers, limit=limit,
                delimiter=delimiter, prefix=prefix, end_marker=end_marker,
                marker=marker, query=context.query, cdn=context.cdn)
            if status // 100 != 2:
                if status == 404 and context.ignore_404:
                    return
                if hasattr(contents, 'read'):
                    contents.read()
                raise ReturnCode(
                    'listing container %r: %s %s' % (path, status, reason))
    conc.join()
    for (exc_type, exc_value, exc_tb, result) in \
            conc.get_results().itervalues():
        if exc_value:
            raise exc_value


def cli_get(context, path=None):
    """
    Performs a GET on the item (account, container, or object).

    See :py:mod:`swiftly.cli.get` for context usage information.

    See :py:class:`CLIGet` for more information.
    """
    path = path.lstrip('/') if path else None
    if not path:
        return cli_get_account_listing(context)
    elif '/' not in path.rstrip('/'):
        return cli_get_container_listing(context, path)
    status, reason, headers, contents = 0, 'Unknown', {}, ''
    with context.client_manager.with_client() as client:
        status, reason, headers, contents = client.get_object(
            *path.split('/', 1), headers=context.headers, query=context.query,
            cdn=context.cdn)
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            if hasattr(contents, 'read'):
                contents.read()
            raise ReturnCode(
                'getting object %r: %s %s' % (path, status, reason))
        if context.decrypt:
            crypt_type = contents.read(1)
            if crypt_type == AES256CBC:
                contents = FileLikeIter(aes_decrypt(
                    context.decrypt, contents,
                    chunk_size=getattr(client, 'chunk_size', 65536)))
            else:
                raise ReturnCode(
                    'getting object %r: contents encrypted with unsupported '
                    'type %r' % (path, crypt_type))

        def disk_closed_callback(disk_path):
            if context.remove_empty_files and not os.path.getsize(disk_path):
                os.unlink(disk_path)
                if context.io_manager.stdout_root:
                    dirname = os.path.dirname(disk_path)
                    while dirname and dirname.startswith(
                            context.io_manager.stdout_root):
                        try:
                            os.rmdir(dirname)
                        except OSError:
                            pass
                        dirname = os.path.dirname(dirname)
                return
            if (headers.get('content-type') in
                    ['text/directory', 'application/directory'] and
                    headers.get('content-length') == '0'):
                os.unlink(disk_path)
                os.makedirs(disk_path)
            mtime = 0
            if 'x-object-meta-mtime' in headers:
                mtime = float(headers['x-object-meta-mtime'])
            elif 'last-modified' in headers:
                mtime = time.mktime(time.strptime(
                    headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z'))
            if mtime:
                os.utime(disk_path, (mtime, mtime))

        out_path = path
        if context.suppress_container_name:
            out_path = out_path.split('/', 1)[1]
        out_path = context.io_manager.client_path_to_os_path(out_path)
        with context.io_manager.with_stdout(
                out_path, disk_closed_callback=disk_closed_callback) as fp:
            if context.output_headers:
                context.write_headers(
                    fp, headers, context.muted_object_headers)
                fp.write('\n')
            chunk = contents.read(65536)
            while chunk:
                fp.write(chunk)
                chunk = contents.read(65536)
            fp.flush()


class CLIGet(CLICommand):
    """
    A CLICommand that can issue GET requests.

    See the output of ``swiftly help get`` for more information.
    """

    def __init__(self, cli):
        super(CLIGet, self).__init__(
            cli, 'get', max_args=1, usage="""
Usage: %prog [main_options] get [options] [path]

For help on [main_options] run %prog with no args.

Outputs the resulting contents from a GET request of the [path] given. If no
[path] is given, a GET request on the account is performed.""".strip())
        self.option_parser.add_option(
            '--headers', dest='headers', action='store_true',
            help='Output headers as well as the contents.')
        self.option_parser.add_option(
            '-h', '-H', '--header', dest='header', action='append',
            metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hif-match:6f432df40167a4af05ca593acc6b3e4c -h '
                 '"If-Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"')
        self.option_parser.add_option(
            '-q', '--query', dest='query', action='append',
            metavar='NAME[=VALUE]',
            help='Add a query parameter to the request. This can be used '
                 'multiple times for multiple query parameters. Example: '
                 '-qmultipart-manifest=get')
        self.option_parser.add_option(
            '-l', '--limit', dest='limit',
            help='For account and container GETs, this limits the number of '
                 'items returned. Without this option, all items are '
                 'returned, even if it requires several backend requests to '
                 'the gather the information.')
        self.option_parser.add_option(
            '-d', '--delimiter', dest='delimiter',
            help='For account and container GETs, this sets the delimiter for '
                 'the listing retrieved. For example, a container with the '
                 'objects "abc/one", "abc/two", "xyz" and a delimiter of "/" '
                 'would return "abc/" and "xyz". Using the same delimiter, '
                 'but with a prefix of "abc/", would return "abc/one" and '
                 '"abc/two".')
        self.option_parser.add_option(
            '-p', '--prefix', dest='prefix',
            help='For account and container GETs, this sets the prefix for '
                 'the listing retrieved; the items returned will all match '
                 'the PREFIX given.')
        self.option_parser.add_option(
            '-m', '--marker', dest='marker',
            help='For account and container GETs, this sets the marker for '
                 'the listing retrieved; the items returned will begin with '
                 'the item just after the MARKER given (note: the marker does '
                 'not have to actually exist).')
        self.option_parser.add_option(
            '-e', '--end-marker', dest='end_marker', metavar='MARKER',
            help='For account and container GETs, this sets the end-marker '
                 'for the listing retrieved; the items returned will stop '
                 'with the item just before the MARKER given (note: the '
                 'marker does not have to actually exist).')
        self.option_parser.add_option(
            '-f', '--full', dest='full', action="store_true",
            help='For account and container GETs, this will output additional '
                 'information about each item. For an account GET, the items '
                 'output will be bytes-used, object-count, and '
                 'container-name. For a container GET, the items output will '
                 'be bytes-used, last-modified-time, etag, content-type, and '
                 'object-name. Note that this is mostly useless for --cdn '
                 'queries; for those it is best to just use --raw and parse '
                 'the results yourself (perhaps through "python -m '
                 'json.tool").')
        self.option_parser.add_option(
            '-r', '--raw', dest='raw', action="store_true",
            help='For account and container GETs, this will return the raw '
                 'JSON from the request. This will only do one request, even '
                 'if subsequent requests would be needed to return all items. '
                 'Use a subsequent call with --marker set to the last item '
                 'name returned to get the next batch of items, if desired.')
        self.option_parser.add_option(
            '--all-objects', dest='all_objects', action="store_true",
            help='For an account GET, performs a container GET --all-objects '
                 'for every container returned by the original account GET. '
                 'For a container GET, performs a GET for every object '
                 'returned by that original container GET. Any headers set '
                 'with --header options are sent for every GET. Any query '
                 'parameter set with --query is sent for every GET.')
        self.option_parser.add_option(
            '-o', '--output', dest='output', metavar='PATH',
            help='Indicates where to send the output; default is standard '
                 'output. If the PATH ends with a slash "/" and --all-objects '
                 'is used, each object will be placed in a similarly named '
                 'file inside the PATH given.')
        self.option_parser.add_option(
            '--ignore-404', dest='ignore_404', action='store_true',
            help='Ignores 404 Not Found responses. Nothing will be output, '
                 'but the exit code will be 0 instead of 1.')
        self.option_parser.add_option(
            '--sub-command', dest='sub_command', metavar='COMMAND',
            help='Sends the contents of each object downloaded as standard '
                 'input to the COMMAND given and outputs the command\'s '
                 'standard output as if it were the object\'s contents. This '
                 'can be useful in combination with --all-objects to filter '
                 'the objects before writing them to disk; for instance, '
                 'downloading logs, gunzipping them, grepping for a keyword, '
                 'and only storing matching lines locally (--sub-command '
                 '"gunzip | grep keyword" or --sub-command "zgrep keyword" if '
                 'your system has that).')
        self.option_parser.add_option(
            '--remove-empty-files', dest='remove_empty_files',
            action='store_true',
            help='Removes files that result as empty. This can be useful in '
                 'conjunction with --sub-command so you are left only with '
                 'the files that generated output.')
        self.option_parser.add_option(
            '--decrypt', dest='decrypt', metavar='KEY',
            help='Will decrypt the downloaded object data with KEY. This '
                 'currently only supports AES 256 in CBC mode but other '
                 'algorithms may be offered in the future. You may specify a '
                 'single dash "-" as the KEY and instead the KEY will be '
                 'loaded from the SWIFTLY_CRYPT_KEY environment variable.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        if options.output:
            if options.output.endswith(os.path.sep):
                context.io_manager.stdout_root = options.output
            else:
                context.io_manager.stdout = open(options.output, 'wb')
                context.io_manager.stdout_root = None
                context.concurrency = 1
        context.io_manager.stdout_sub_command = options.sub_command
        context.output_headers = options.headers
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        context.raw = options.raw
        context.ignore_404 = options.ignore_404
        context.all_objects = options.all_objects
        context.full = options.full
        context.remove_empty_files = options.remove_empty_files
        if options.limit:
            context.query['limit'] = int(options.limit)
        if options.delimiter:
            context.query['delimiter'] = options.delimiter
        if options.prefix:
            context.query['prefix'] = options.prefix
        if options.marker:
            context.query['marker'] = options.marker
        if options.end_marker:
            context.query['end_marker'] = options.end_marker
        context.decrypt = options.decrypt
        if context.decrypt == '-':
            context.decrypt = os.environ.get('SWIFTLY_CRYPT_KEY')
            if not context.decrypt:
                raise ReturnCode(
                    'A single dash "-" was given as the decryption key, but '
                    'no key was found in the SWIFTLY_CRYPT_KEY environment '
                    'variable.')
        path = args.pop(0).lstrip('/') if args else None
        return cli_get(context, path)
