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

from swiftly import VERSION
from swiftly.client import Client, CHUNK_SIZE


MUTED_ACCOUNT_HEADERS = ['accept-ranges', 'content-length', 'content-type']
MUTED_CONTAINER_HEADERS = ['accept-ranges', 'content-length', 'content-type']
MUTED_OBJECT_HEADERS = ['accept-ranges']


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

    def error(self, msg):
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
    :param stdout: The file-like-object to send output to.
    :param stderr: The file-like-object to send error output to.
    """

    def __init__(self, args=None, stdout=None, stderr=None):
        self.args = args
        if not self.args:
            self.args = sys.argv[1:]
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

Prints help information for the given [command] or general help if no [command]
is given.""".strip(),
            stdout=self.stdout, stderr=self.stderr)

        self._head_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] head [path]

For help on [main_options] run %prog with no args.

Prints the resulting headers from a HEAD request of the [path] given. If no
[path] is given, a HEAD request on the account is performed.""".strip(),
            stdout=self.stdout, stderr=self.stderr)

        self._get_parser = _OptionParser(version='%prog 1.0', usage="""
Usage: %prog [main_options] get [options] [path]

For help on [main_options] run %prog with no args.

Prints the resulting contents from a GET request of the [path] given. If no
[path] is given, a GET request on the account is performed.""".strip(),
            stdout=self.stdout, stderr=self.stderr)
        self._get_parser.add_option('--headers', dest='headers',
            action='store_true',
            help='Output headers as well as the contents.')

        self.main_parser = _OptionParser(version='%prog 1.0',
            usage='Usage: %prog [options] <command> [command_options] [args]',
            stdout=self.stdout, stderr=self.stderr)
        self.main_parser.add_option('-A', '--auth_url', dest='auth_url',
           help='URL to auth system, example: http://127.0.0.1:8080/auth/v1.0')
        self.main_parser.add_option('-U', '--auth_user', dest='auth_user',
            help='User name for auth system, example: test:tester')
        self.main_parser.add_option('-K', '--auth_key', dest='auth_key',
            help='Key for auth system, example: testing')
        self.main_parser.add_option('-D', '--direct', dest='direct',
            help='Uses direct connect method to access Swift. Requires access '
                 'to rings and backend servers. The value is the account '
                 'path, example: /v1/AUTH_test')
        self.main_parser.add_option('-P', '--proxy', dest='proxy',
            help='Uses the given proxy URL.')
        self.main_parser.add_option('-S', '--snet', dest='snet',
            action='store_true',
            help='Prepends the storage URL host name with "snet-". Mostly '
                 'only useful with Rackspace Cloud Files and Rackspace '
                 'ServiceNet.')
        self.main_parser.add_option('-R', '--retries', dest='retries',
            default=4,
            help='Indicates how many times to retry the request on a server '
                 'error. Default: 4.')
        self.main_parser.commands = 'Commands:\n'
        for key in sorted(dir(self)):
            attr = getattr(self, key)
            if getattr(attr, '__is_command__', False):
                lines = getattr(self, key + '_parser').get_usage().split('\n')
                main_line = '  ' + lines[0].split(']', 1)[1].strip()
                for x in xrange(4):
                    lines.pop(0)
                if len(main_line) < 23:
                    initial_indent = main_line + ' ' * (24 - len(main_line))
                else:
                    self.main_parser.commands += main_line + '\n'
                    initial_indent = ' ' * 24
                self.main_parser.commands += textwrap.fill(' '.join(lines),
                    width=79, initial_indent=initial_indent,
                    subsequent_indent=' ' * 24) + '\n'

    def main(self):
        """
        Process the command line given in the constructor.
        """
        self.main_parser.disable_interspersed_args()
        options, args = self.main_parser.parse_args(self.args)
        self.main_parser.enable_interspersed_args()
        if not args:
            self.main_parser.print_help()
            return 1
        func = getattr(self, '_' + args[0], None)
        if not func or not getattr(func, '__is_command__', False):
            self.main_parser.print_help()
            return 1
        if not getattr(func, '__is_client_command__', False):
            return func(options, args[1:])
        self.client = None
        if all([options.auth_url, options.auth_user, options.auth_key]):
            self.client = Client(auth_url=options.auth_url,
                auth_user=options.auth_user, auth_key=options.auth_key,
                proxy=options.proxy, snet=options.snet,
                retries=int(options.retries))
        elif options.direct:
            self.client = Client(swift_proxy=True,
                swift_proxy_storage_path=options.direct,
                retries=int(options.retries))
        else:
            self.main_parser.print_help()
            return 1
        return func(options, args[1:])

    def _print_headers(self, headers, mute=None):
        if headers:
            if not mute:
                mute = []
            fmt = '%%-%ds %%s\n' % (min(25, max(len(k) for k in headers)) + 1)
            for key in sorted(headers):
                if key in mute:
                    continue
                self.stdout.write(fmt % (key.title() + ':', headers[key]))
            self.stdout.flush()

    @_command
    def _help(self, main_options, args):
        if not args:
            self.main_parser.print_help()
            return 1
        func = getattr(self, '_' + args[0], None)
        if not func or not getattr(func, '__is_command__', False):
            self.main_parser.print_help()
            return 1
        getattr(self, '_' + args[0] + '_parser').print_help()
        return 1

    @_client_command
    def _head(self, main_options, args):
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        mute = []
        if not args:
            status, reason, headers, contents = self.client.head_account()
            mute.extend(MUTED_ACCOUNT_HEADERS)
        elif len(args) == 1:
            path = args[0].lstrip('/')
            if '/' not in path.rstrip('/'):
                status, reason, headers, contents = \
                    self.client.head_container(path.rstrip('/'))
                mute.extend(MUTED_CONTAINER_HEADERS)
            else:
                status, reason, headers, contents = \
                    self.client.head_object(*path.split('/', 1))
                mute.extend(MUTED_OBJECT_HEADERS)
        else:
            self._head_parser.print_help()
            return 1
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        self._print_headers(headers, mute)
        return 0

    @_client_command
    def _get(self, main_options, args):
        options, args = self._get_parser.parse_args(args)
        status, reason, headers, contents = 0, 'Unknown', {}, ''
        mute = []
        if not args:
            status, reason, headers, contents = self.client.get_account()
            mute.extend(MUTED_ACCOUNT_HEADERS)
        elif len(args) == 1:
            path = args[0].lstrip('/')
            if '/' not in path.rstrip('/'):
                status, reason, headers, contents = \
                    self.client.get_container(path.rstrip('/'))
                mute.extend(MUTED_CONTAINER_HEADERS)
            else:
                status, reason, headers, contents = \
                    self.client.get_object(*path.split('/', 1))
                mute.extend(MUTED_OBJECT_HEADERS)
        else:
            self._get_parser.print_help()
            return 1
        if status // 100 != 2:
            self.stderr.write('%s %s\n' % (status, reason))
            self.stderr.flush()
            return 1
        if options.headers:
            self._print_headers(headers, mute)
            self.stdout.write('\n')
        if hasattr(contents, 'read'):
            chunk = contents.read(CHUNK_SIZE)
            while chunk:
                self.stdout.write(chunk)
                chunk = contents.read(CHUNK_SIZE)
        else:
            for item in contents:
                self.stdout.write(
                    item.get('name', item.get('subdir')).encode('utf8'))
                self.stdout.write('\n')
        self.stdout.flush()
        return 0
