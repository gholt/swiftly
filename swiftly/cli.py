"""
Command Line Client to Swift

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

__all__ = ['VERSION', 'CLI']

import sys
from optparse import Option, OptionParser
import textwrap
from os import environ, makedirs, unlink, utime, walk
from os.path import dirname, exists, getmtime, getsize, join as pathjoin, isdir
from time import mktime, strptime

from swiftly import VERSION
from swiftly.client import Client, CHUNK_SIZE

try:
    from simplejson import json
except ImportError:
    import json


MUTED_ACCOUNT_HEADERS = ['accept-ranges', 'content-length', 'content-type',
                         'date']
MUTED_CONTAINER_HEADERS = ['accept-ranges', 'content-length', 'content-type',
                           'date']
MUTED_OBJECT_HEADERS = ['accept-ranges', 'date']


def _command(func):
    func.__is_command__ = True
    func.__is_client_command__ = False
    return func


def _client_command(func):
    func.__is_command__ = True
    func.__is_client_command__ = True
    return func


class _OptionParser(OptionParser):

    def __init__(self, usage=None, option_list=None, option_class=Option,
                 version=None, conflict_handler='error', description=None,
                 formatter=None, add_help_option=True, prog=None, epilog=None,
                 stdout=None, stderr=None):
        OptionParser.__init__(self, usage, option_list, option_class, version,
                              conflict_handler, description, formatter,
                              add_help_option, prog, epilog)
        self.remove_option('--version')
        self.remove_option('-h')
        self.stdout = stdout
        if not self.stdout:
            self.stdout = sys.stdout
        self.stderr = stderr
        if not self.stderr:
            self.stderr = sys.stderr
        self.commands = ''
        self.error_encountered = False

    def error(self, msg):
        self.error_encountered = True
        self.stderr.write(msg)
        self.stderr.write('\n')
        self.stderr.flush()

    def exit(self, status=0, msg=None):
        if msg:
            self.error(msg)
        sys.exit(status)

    def print_help(self, file=None):
        if not file:
            file = self.stdout
        OptionParser.print_help(self, file)
        if self.commands:
            file.write(self.commands)
        file.flush()

    def print_usage(self, file=None):
        if not file:
            file = self.stdout
        OptionParser.print_usage(self, file)
        file.flush()

    def print_version(self, file=None):
        if not file:
            file = self.stdout
        OptionParser.print_version(self, file)
        file.flush()


class CLI(object):
    """
    An instance of a command line interface client to Swift. After
    construction, a call to main() will execute the command line given. No args
    will output help information.

    :param args: The command line arguments to process.
    :param stdin: The file-like-object to read input from.
    :param stdout: The file-like-object to send output to.
    :param stderr: The file-like-object to send error output to.
    """

    def __init__(self, args=None, stdin=None, stdout=None, stderr=None):
        self.args = args
        if not self.args:
            self.args = sys.argv[1:]
        self.stdin = stdin
        if not self.stdin:
            self.stdin = sys.stdin
        self.stdout = stdout
        if not self.stdout:
            self.stdout = sys.stdout
        self.stderr = stderr
        if not self.stderr:
            self.stderr = sys.stderr
        self.client = None

        self._help_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] help [command]

For help on [main_options] run %prog with no args.

Outputs help information for the given [command] or general help if no
[command] is given.""".strip(),
            stdout=self.stdout, stderr=self.stderr)

        self._auth_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] auth

For help on [main_options] run %prog with no args.

Outputs auth information.""".strip(),
            stdout=self.stdout, stderr=self.stderr)

        self._head_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] head [options] [path]

For help on [main_options] run %prog with no args.

Outputs the resulting headers from a HEAD request of the [path] given. If no
[path] is given, a HEAD request on the account is performed.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._head_parser.add_option('-h', '--header', dest='header',
            action='append', metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hif-match:6f432df40167a4af05ca593acc6b3e4c -h '
                 '"If-Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"')

        self._get_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] get [options] [path]

For help on [main_options] run %prog with no args.

Outputs the resulting contents from a GET request of the [path] given. If no
[path] is given, a GET request on the account is performed.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._get_parser.add_option('--headers', dest='headers',
            action='store_true',
            help='Output headers as well as the contents.')
        self._get_parser.add_option('-h', '--header', dest='header',
            action='append', metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hif-match:6f432df40167a4af05ca593acc6b3e4c -h '
                 '"If-Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"')
        self._get_parser.add_option('-l', '--limit', dest='limit',
            help='For account and container GETs, this limits the number of '
                 'items returned. Without this option, all items are '
                 'returned, even if it requires several backend requests to '
                 'the gather the information.')
        self._get_parser.add_option('-d', '--delimiter', dest='delimiter',
            help='For account and container GETs, this sets the delimiter for '
                 'the listing retrieved. For example, a container with the '
                 'objects "abc/one", "abc/two", "xyz" and a delimiter of "/" '
                 'would return "abc/" and "xyz". Using the same delimiter, '
                 'but with a prefix of "abc/", would return "abc/one" and '
                 '"abc/two".')
        self._get_parser.add_option('-p', '--prefix', dest='prefix',
            help='For account and container GETs, this sets the prefix for '
                 'the listing retrieved; the items returned will all match '
                 'the PREFIX given.')
        self._get_parser.add_option('-m', '--marker', dest='marker',
            help='For account and container GETs, this sets the marker for '
                 'the listing retrieved; the items returned will begin with '
                 'the item just after the MARKER given (note: the marker does '
                 'not have to actually exist).')
        self._get_parser.add_option('-e', '--end_marker', dest='end_marker',
            metavar='MARKER',
            help='For account and container GETs, this sets the end_marker '
                 'for the listing retrieved; the items returned will stop '
                 'with the item just before the MARKER given (note: the '
                 'marker does not have to actually exist).')
        self._get_parser.add_option('-f', '--full', dest='full',
            action="store_true",
            help='For account and container GETs, this will output additional '
                 'information about each item. For an account GET, the items '
                 'output will be bytes-used, object-count, and '
                 'container-name. For a container GET, the items output will '
                 'be bytes-used, last-modified-time, etag, content-type, and '
                 'object-name.')
        self._get_parser.add_option('-r', '--raw', dest='raw',
            action="store_true",
            help='For account and container GETs, this will return the raw '
                 'JSON from the request. This will only do one request, even '
                 'if subsequent requests would be needed to return all items. '
                 'Use a subsequent call with --marker set to the last item '
                 'name returned to get the next batch of items, if desired.')
        self._get_parser.add_option('--all-objects', dest='all_objects',
            action="store_true",
            help='For a container GET, performs a GET for every object '
                 'returned by the original container GET. Any headers set '
                 'with --header options are also sent for every object GET.')
        self._get_parser.add_option('-o', '--output', dest='output',
            metavar='PATH',
            help='Indicates where to send the output; default is standard '
                 'output. If the PATH ends with a slash "/" and --all-objects '
                 'is used, each object will be placed in a similarly named '
                 'file inside the PATH given.')

        self._put_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] put [options] <path>

For help on [main_options] run %prog with no args.

Performs a PUT request on the <path> given. If the <path> is an object, the
contents for the object are read from standard input.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._put_parser.add_option('-h', '--header', dest='header',
            action='append', metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hx-object-meta-color:blue -h "Content-Type: text/html"')
        self._put_parser.add_option('-i', '--input', dest='input_',
            metavar='PATH',
            help='Indicates where to read the contents from; default is '
                 'standard input. If the PATH is a directory, all files in '
                 'the directory will be uploaded as similarly named objects '
                 'and empty directories will create text/directory marker '
                 'objects.')
        self._put_parser.add_option('-n', '--newer', dest='newer',
            action='store_true',
            help='For PUTs with an --input option, first performs a HEAD on '
                 'the object and compares the X-Object-Meta-Mtime header with '
                 'the modified time of the PATH obtained from the --input '
                 'option and then PUTs the object only if the local time is '
                 'newer. When the --input PATH is a directory, this offers an '
                 'easy way to upload only the newer files since the last '
                 'upload (at the expense of HEAD requests). NOTE THAT THIS '
                 'WILL NOT UPLOAD CHANGED FILES THAT DO NOT HAVE A NEWER '
                 'LOCAL MODIFIED TIME! NEWER does not mean DIFFERENT.')
        self._put_parser.add_option('-d', '--different', dest='different',
            action='store_true',
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
        self._put_parser.add_option('-e', '--empty', dest='empty',
            action='store_true',
            help='Indicates a zero-byte object should be PUT.')

        self._post_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] post [options] [path]

For help on [main_options] run %prog with no args.

Issues a POST request of the [path] given. If no [path] is given, a POST
request on the account is performed.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._post_parser.add_option('-h', '--header', dest='header',
            action='append', metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hx-object-meta-color:blue -h "Content-Type: text/html"')

        self._delete_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] delete [options] <path>

For help on [main_options] run %prog with no args.

Issues a DELETE request of the <path> given.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._delete_parser.add_option('-h', '--header', dest='header',
            action='append', metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hx-some-header:some-value -h "X-Some-Other-Header: Some '
                 'other value"')

        self._main_parser = _OptionParser(version='%prog 1.0',
            usage='Usage: %prog [options] <command> [command_options] [args]',
            stdout=self.stdout, stderr=self.stderr)
        self._main_parser.add_option('-A', '--auth_url', dest='auth_url',
            default=environ.get('SWIFTLY_AUTH_URL', ''),
            help='URL to auth system, example: '
                 'http://127.0.0.1:8080/auth/v1.0 You can also set this with '
                 'the environment variable SWIFTLY_AUTH_URL.')
        self._main_parser.add_option('-U', '--auth_user', dest='auth_user',
            default=environ.get('SWIFTLY_AUTH_USER', ''),
            help='User name for auth system, example: test:tester You can '
                 'also set this with the environment variable '
                 'SWIFTLY_AUTH_USER.')
        self._main_parser.add_option('-K', '--auth_key', dest='auth_key',
            default=environ.get('SWIFTLY_AUTH_KEY', ''),
            help='Key for auth system, example: testing You can also set this '
                 'with the environment variable SWIFTLY_AUTH_KEY.')
        self._main_parser.add_option('-D', '--direct', dest='direct',
            default=environ.get('SWIFTLY_DIRECT', ''),
            help='Uses direct connect method to access Swift. Requires access '
                 'to rings and backend servers. The value is the account '
                 'path, example: /v1/AUTH_test You can also set this with the '
                 'environment variable SWIFTLY_DIRECT.')
        self._main_parser.add_option('-P', '--proxy', dest='proxy',
            default=environ.get('SWIFTLY_PROXY', ''),
            help='Uses the given proxy URL. You can also set this with the '
                 'environment variable SWIFTLY_PROXY.')
        self._main_parser.add_option('-S', '--snet', dest='snet',
            action='store_true',
            default=environ.get('SWIFTLY_SNET', 'false').lower() == 'true',
            help='Prepends the storage URL host name with "snet-". Mostly '
                 'only useful with Rackspace Cloud Files and Rackspace '
                 'ServiceNet. You can also set this with the environment '
                 'variable SWIFTLY_SNET (set to "true" or "false").')
        self._main_parser.add_option('-R', '--retries', dest='retries',
            default=int(environ.get('SWIFTLY_RETRIES', 4)),
            help='Indicates how many times to retry the request on a server '
                 'error. Default: 4. You can also set this with the '
                 'environment variable SWIFTLY_RETRIES.')
        self._main_parser.add_option('-C', '--cache-auth', dest='cache_auth',
            action='store_true',
            default=
                environ.get('SWIFTLY_CACHE_AUTH', 'false').lower() == 'true',
            help='If set true, the storage URL and auth token are cached in '
                 '/tmp/<user>.swiftly for reuse. If there are already cached '
                 'values, they are used without authenticating first. You can '
                 'also set this with the environment variable '
                 'SWIFTLY_CACHE_AUTH (set to "true" or "false").')
        self._main_parser.commands = 'Commands:\n'
        for key in sorted(dir(self)):
            attr = getattr(self, key)
            if getattr(attr, '__is_command__', False):
                lines = getattr(self, key + '_parser').get_usage().split('\n')
                main_line = '  ' + lines[0].split(']', 1)[1].strip()
                for x in xrange(4):
                    lines.pop(0)
                if len(main_line) < 24:
                    initial_indent = main_line + ' ' * (24 - len(main_line))
                else:
                    self._main_parser.commands += main_line + '\n'
                    initial_indent = ' ' * 24
                self._main_parser.commands += textwrap.fill(' '.join(lines),
                    width=79, initial_indent=initial_indent,
                    subsequent_indent=' ' * 24) + '\n'

    def main(self):
        """
        Process the command line given in the constructor.
        """
        self._main_parser.disable_interspersed_args()
        options, args = self._main_parser.parse_args(self.args)
        self._main_parser.enable_interspersed_args()
        if self._main_parser.error_encountered:
            return 1
        if not args:
            self._main_parser.print_help()
            return 1
        func = getattr(self, '_' + args[0], None)
        if not func or not getattr(func, '__is_command__', False):
            self._main_parser.print_help()
            return 1
        if not getattr(func, '__is_client_command__', False):
            return func(options, args[1:])
        self.client = None
        if options.direct:
            self.client = Client(swift_proxy=True,
                swift_proxy_storage_path=options.direct,
                retries=int(options.retries))
        elif all([options.auth_url, options.auth_user, options.auth_key]):
            cache_path = None
            if options.cache_auth:
                cache_path = '/tmp/%s.swiftly' % environ.get('USER', 'user')
            self.client = Client(auth_url=options.auth_url,
                auth_user=options.auth_user, auth_key=options.auth_key,
                proxy=options.proxy, snet=options.snet,
                retries=int(options.retries), cache_path=cache_path)
        else:
            self._main_parser.print_help()
            return 1
        return func(options, args[1:])

    def _command_line_headers(self, options_list):
        headers = {}
        if options_list:
            for h in options_list:
                v = ''
                if ':' in h:
                    h, v = h.split(':', 1)
                headers[h.strip().lower()] = v.strip()
        return headers

    def _output_headers(self, headers, mute=None, stdout=None):
        if headers:
            if not mute:
                mute = []
            if not stdout:
                stdout = self.stdout
            fmt = '%%-%ds %%s\n' % (min(25, max(len(k) for k in headers)) + 1)
            for key in sorted(headers):
                if key in mute:
                    continue
                stdout.write(fmt % (key.title() + ':', headers[key]))
            stdout.flush()

    @_command
    def _help(self, main_options, args):
        options, args = self._help_parser.parse_args(args)
        if self._help_parser.error_encountered:
            return 1
        if not args:
            self._main_parser.print_help()
            return 1
        func = getattr(self, '_' + args[0], None)
        if not func or not getattr(func, '__is_command__', False):
            self._main_parser.print_help()
            return 1
        getattr(self, '_' + args[0] + '_parser').print_help()
        return 1

    @_client_command
    def _auth(self, main_options, args):
        options, args = self._auth_parser.parse_args(args)
        if self._auth_parser.error_encountered:
            return 1
        if args:
            self._auth_parser.print_help()
            return 1
        if main_options.direct:
            self.stdout.write('Direct Storage Path: ')
            self.stdout.write(self.client.storage_path)
            self.stdout.write('\n')
            self.stdout.flush()
            return 0
        status, reason, headers, contents = self.client.head_account()
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        self.stdout.write('Storage URL: ')
        self.stdout.write(self.client.storage_url)
        self.stdout.write('\n')
        self.stdout.write('Auth Token:  ')
        self.stdout.write(self.client.auth_token)
        self.stdout.write('\n')
        self.stdout.flush()
        return 0

    @_client_command
    def _head(self, main_options, args):
        options, args = self._head_parser.parse_args(args)
        if self._head_parser.error_encountered:
            return 1
        hdrs = self._command_line_headers(options.header)
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        mute = []
        if not args:
            status, reason, headers, contents = \
                self.client.head_account(headers=hdrs)
            mute.extend(MUTED_ACCOUNT_HEADERS)
        elif len(args) == 1:
            path = args[0].lstrip('/')
            if '/' not in path.rstrip('/'):
                status, reason, headers, contents = \
                    self.client.head_container(path.rstrip('/'), headers=hdrs)
                mute.extend(MUTED_CONTAINER_HEADERS)
            else:
                status, reason, headers, contents = \
                    self.client.head_object(*path.split('/', 1), headers=hdrs)
                mute.extend(MUTED_OBJECT_HEADERS)
        else:
            self._head_parser.print_help()
            return 1
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        self._output_headers(headers, mute)
        return 0

    @_client_command
    def _get(self, main_options, args):
        options, args = self._get_parser.parse_args(args)
        if self._get_parser.error_encountered:
            return 1
        if len(args) > 1:
            self._get_parser.print_help()
            return 1
        hdrs = self._command_line_headers(options.header)
        stdout = self.stdout
        if options.output:
            if options.output.endswith('/'):
                if not exists(options.output):
                    makedirs(options.output)
            else:
                stdout = open(options.output, 'wb')
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        path = ''
        if args:
            path = args[0].lstrip('/')
        if not path or '/' not in path.rstrip('/'):
            path = path.rstrip('/')
            func = self.client.get_account
            mute = MUTED_ACCOUNT_HEADERS
            if path:
                func = self.client.get_container
                mute = MUTED_CONTAINER_HEADERS
            limit = options.limit or 10000
            delimiter = options.delimiter
            prefix = options.prefix
            marker = options.marker
            end_marker = options.end_marker
            if path:
                status, reason, headers, contents = func(
                    path, headers=hdrs, limit=limit, delimiter=delimiter,
                    prefix=prefix, marker=marker, end_marker=end_marker)
            else:
                status, reason, headers, contents = func(
                    headers=hdrs, limit=limit, marker=marker,
                    end_marker=end_marker)
            if status // 100 != 2:
                self.stderr.write('%s %s\n' % (status, reason))
                self.stderr.flush()
                if hasattr(contents, 'read'):
                    contents.read()
                return 1
            if options.headers and not options.all_objects:
                self._output_headers(headers, mute, stdout=stdout)
                stdout.write('\n')
                stdout.flush()
            while contents:
                if options.all_objects:
                    for item in contents:
                        if 'name' in item:
                            subargs = \
                                [path + '/' + item['name'].encode('utf8')]
                            if options.header:
                                for h in options.header:
                                    subargs.append('-h')
                                    subargs.append(h)
                            if options.output and options.output.endswith('/'):
                                outpath = \
                                   options.output + item['name'].encode('utf8')
                                dirpath = dirname(outpath)
                                if not exists(dirpath):
                                    makedirs(dirpath)
                                elif not isdir(dirpath):
                                    self.stderr.write('%s conflict; file and '
                                        'directory\n' % dirpath)
                                    self.stderr.flush()
                                    return 1
                                subargs.append('-o')
                                subargs.append(outpath)
                            rv = self._get(main_options, subargs)
                            if rv == 404:
                                self.stderr.write('skipping %s; not found')
                                self.stderr.flush()
                            elif rv:
                                self.stderr.write('aborting after error with '
                                    '%s\n' % subargs[0])
                                self.stderr.flush()
                                return rv
                elif options.raw:
                    json.dump(contents, stdout)
                    stdout.write('\n')
                    stdout.flush()
                    break
                else:
                    for item in contents:
                        if options.full:
                            if not path:
                                stdout.write('%13s %13s ' % (
                                    item.get('bytes', '-'),
                                    item.get('count', '-')))
                            else:
                                stdout.write('%13s %22s %32s %25s ' % (
                                    item.get('bytes', '-'),
                                    item.get('last_modified',
                                             '-')[:22].replace('T', ' '),
                                    item.get('hash', '-'),
                                    item.get('content_type', '-')))
                        stdout.write(
                           item.get('name', item.get('subdir')).encode('utf8'))
                        stdout.write('\n')
                    stdout.flush()
                if options.limit:
                    break
                if path:
                    status, reason, headers, contents = func(
                        path, headers=hdrs, limit=limit, delimiter=delimiter,
                        prefix=prefix, end_marker=end_marker,
                        marker=contents[-1].get('name',
                                               contents[-1].get('subdir', '')))
                else:
                    status, reason, headers, contents = func(
                        headers=hdrs, limit=limit, delimiter=delimiter,
                        prefix=prefix, end_marker=end_marker,
                        marker=contents[-1].get('name',
                                               contents[-1].get('subdir', '')))
                if status // 100 != 2:
                    self.stderr.write('%s %s\n' % (status, reason))
                    self.stderr.flush()
                    if hasattr(contents, 'read'):
                        contents.read()
                    return 1
            return 0
        status, reason, headers, contents = \
            self.client.get_object(*path.split('/', 1), headers=hdrs)
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            if hasattr(contents, 'read'):
                contents.read()
            return 1
        if options.headers:
            self._output_headers(headers, MUTED_OBJECT_HEADERS, stdout=stdout)
            stdout.write('\n')
        if headers.get('content-type') == 'text/directory' and \
                headers.get('content-length') == '0':
            contents.read()
            if options.output and not options.output.endswith('/'):
                stdout.close()
                unlink(options.output)
                makedirs(options.output)
        else:
            chunk = contents.read(CHUNK_SIZE)
            while chunk:
                stdout.write(chunk)
                chunk = contents.read(CHUNK_SIZE)
            stdout.flush()
        if options.output and not options.output.endswith('/'):
            stdout.close()
            mtime = 0
            if 'x-object-meta-mtime' in headers:
                mtime = float(headers['x-object-meta-mtime'])
            elif 'last-modified' in headers:
                mtime = mktime(strptime(headers['last-modified'],
                                        '%a, %d %b %Y %H:%M:%S %Z'))
            if mtime:
                utime(options.output, (mtime, mtime))
        return 0

    @_client_command
    def _put(self, main_options, args):
        options, args = self._put_parser.parse_args(args)
        if self._put_parser.error_encountered:
            return 1
        if not args or len(args) != 1:
            self._put_parser.print_help()
            return 1
        if options.input_ and isdir(options.input_):
            subargs = [args[0].split('/', 1)[0]]
            if options.header:
                for h in options.header:
                    subargs.append('-h')
                    subargs.append(h)
            rv = self._put(main_options, subargs)
            if rv:
                self.stderr.write(
                    'aborting after error with %s\n' % subargs[0])
                self.stderr.flush()
                return rv
            ilen = len(options.input_)
            if not options.input_.endswith('/'):
                ilen += 1
            for (dirpath, dirnames, filenames) in walk(options.input_):
                if not dirnames and not filenames:
                    subargs = [args[0] + '/' + dirpath[ilen:]]
                    if options.header:
                        for h in options.header:
                            subargs.append('-h')
                            subargs.append(h)
                    subargs.append('-h')
                    subargs.append('content-type:text/directory')
                    subargs.append('-h')
                    subargs.append(
                        'x-object-meta-mtime:%d' % getmtime(options.input_))
                    subargs.append('-e')
                    rv = self._put(main_options, subargs)
                    if rv:
                        self.stderr.write(
                            'aborting after error with %s\n' % subargs[0])
                        self.stderr.flush()
                        return rv
                else:
                    for fname in filenames:
                        subargs = \
                            ['%s/%s/%s' % (args[0], dirpath[ilen:], fname)]
                        if options.header:
                            for h in options.header:
                                subargs.append('-h')
                                subargs.append(h)
                        subargs.append('-i')
                        subargs.append(pathjoin(dirpath, fname))
                        if options.newer:
                            subargs.append('-n')
                        if options.different:
                            subargs.append('-d')
                        rv = self._put(main_options, subargs)
                        if rv:
                            self.stderr.write(
                                'aborting after error with %s\n' % subargs[0])
                            self.stderr.flush()
                            return rv
            return 0
        path = args[0].lstrip('/')
        hdrs = {}
        stdin = self.stdin
        if options.empty:
            hdrs['content-length'] = '0'
            stdin = ''
        elif options.input_ and not isdir(options.input_):
            l_mtime = getmtime(options.input_)
            if (options.newer or options.different) and \
                    '/' in path.rstrip('/'):
                r_mtime = 0
                r_size = -1
                status, reason, headers, contents = \
                    self.client.head_object(*path.split('/', 1))
                if status // 100 == 2:
                    try:
                        r_mtime = int(headers.get('x-object-meta-mtime', 0))
                    except ValueError:
                        pass
                    try:
                        r_size = int(headers.get('content-length', -1))
                    except ValueError:
                        pass
                if options.newer and l_mtime <= r_mtime:
                    return 0
                if options.different and l_mtime == r_mtime and \
                        getsize(options.input_) == r_size:
                    return 0
            hdrs['x-object-meta-mtime'] = '%d' % l_mtime
            stdin = open(options.input_, 'rb')
        hdrs.update(self._command_line_headers(options.header))
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        if '/' not in path.rstrip('/'):
            status, reason, headers, contents = \
                self.client.put_container(path.rstrip('/'), headers=hdrs)
        else:
            c, o = path.split('/', 1)
            status, reason, headers, contents = \
                self.client.put_object(c, o, stdin, headers=hdrs)
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        return 0

    @_client_command
    def _post(self, main_options, args):
        options, args = self._post_parser.parse_args(args)
        if self._post_parser.error_encountered:
            return 1
        hdrs = self._command_line_headers(options.header)
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        if not args:
            status, reason, headers, contents = \
                self.client.post_account(headers=hdrs)
        elif len(args) == 1:
            path = args[0].lstrip('/')
            if '/' not in path.rstrip('/'):
                status, reason, headers, contents = \
                    self.client.post_container(path.rstrip('/'), headers=hdrs)
            else:
                status, reason, headers, contents = \
                    self.client.post_object(*path.split('/', 1), headers=hdrs)
        else:
            self._post_parser.print_help()
            return 1
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        return 0

    @_client_command
    def _delete(self, main_options, args):
        options, args = self._delete_parser.parse_args(args)
        if self._delete_parser.error_encountered:
            return 1
        hdrs = self._command_line_headers(options.header)
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        if len(args) == 1:
            path = args[0].lstrip('/')
            if '/' not in path.rstrip('/'):
                status, reason, headers, contents = \
                    self.client.delete_container(path.rstrip('/'),
                                                 headers=hdrs)
            else:
                status, reason, headers, contents = \
                    self.client.delete_object(*path.split('/', 1),
                                              headers=hdrs)
        else:
            self._delete_parser.print_help()
            return 1
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        return 0
