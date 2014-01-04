"""
Contains a CLICommand that can issue HEAD requests.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

=======================  ============================================
cdn                      True if the CDN Management URL should be
                         used instead of the Storage URL.
client_manager           For connecting to Swift.
headers                  A dict of headers to send.
ignore_404               True if 404s should be silently ignored.
io_manager               For directing output.
muted_account_headers    The headers to omit when outputting account
                         response headers.
muted_container_headers  The headers to omit when outputting
                         container response headers.
muted_object_headers     The headers to omit when outputting object
                         response headers.
query                    A dict of query parameters to send.
write_headers            A function used to output the response
                         headers.
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
from swiftly.cli.command import CLICommand, ReturnCode


def cli_head(context, path=None):
    """
    Performs a HEAD on the item (account, container, or object).

    See :py:mod:`swiftly.cli.head` for context usage information.

    See :py:class:`CLIHead` for more information.
    """
    path = path.lstrip('/') if path else None
    with context.client_manager.with_client() as client:
        if not path:
            status, reason, headers, contents = client.head_account(
                headers=context.headers, query=context.query, cdn=context.cdn)
            mute = context.muted_account_headers
        elif '/' not in path.rstrip('/'):
            path = path.rstrip('/')
            status, reason, headers, contents = client.head_container(
                path, headers=context.headers, query=context.query,
                cdn=context.cdn)
            mute = context.muted_container_headers
        else:
            status, reason, headers, contents = client.head_object(
                *path.split('/', 1), headers=context.headers,
                query=context.query, cdn=context.cdn)
            mute = context.muted_object_headers
        if hasattr(contents, 'read'):
            contents = contents.read()
    if status // 100 != 2:
        if status == 404 and context.ignore_404:
            return
        if not path:
            raise ReturnCode('heading account: %s %s' % (status, reason))
        elif '/' not in path:
            raise ReturnCode(
                'heading container %r: %s %s' % (path, status, reason))
        else:
            raise ReturnCode(
                'heading object %r: %s %s' % (path, status, reason))
    else:
        with context.io_manager.with_stdout() as fp:
            context.write_headers(fp, headers, mute)


class CLIHead(CLICommand):
    """
    A CLICommand that can issue HEAD requests.

    See the output of ``swiftly help head`` for more information.
    """

    def __init__(self, cli):
        super(CLIHead, self).__init__(
            cli, 'head', max_args=1, usage="""
Usage: %prog [main_options] head [options] [path]

For help on [main_options] run %prog with no args.

Outputs the resulting headers from a HEAD request of the [path] given. If no
[path] is given, a HEAD request on the account is performed.""".strip())
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
            '--ignore-404', dest='ignore_404', action='store_true',
            help='Ignores 404 Not Found responses. Nothing will be output, '
                 'but the exit code will be 0 instead of 1.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        context.ignore_404 = options.ignore_404
        path = args.pop(0).lstrip('/') if args else None
        return cli_head(context, path)
