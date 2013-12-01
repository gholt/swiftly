"""
Contains a CLICommand that can issue POST requests.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

==============  ============================================
cdn             True if the CDN Management URL should be used instead
                of the Storage URL.
client_manager  For connecting to Swift.
headers         A dict of headers to send.
io_manager      For directing output.
query           A dict of query parameters to send.
==============  ============================================
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


def cli_post(context, path, body=None):
    """
    Performs a POST on the item (account, container, or object).

    See :py:mod:`swiftly.cli.post` for context usage information.

    See :py:class:`CLIPost` for more information.

    :param context: The :py:class:`swiftly.cli.context.CLIContext` to
        use.
    :param path: The path to the item to issue a POST for.
    :param body: The body send along with the POST.
    """
    path = path.lstrip('/') if path else ''
    status, reason, headers, contents = 0, 'Unknown', {}, ''
    with context.client_manager.with_client() as client:
        if not path:
            status, reason, headers, contents = client.post_account(
                headers=context.headers, query=context.query, cdn=context.cdn,
                body=body)
            if status // 100 != 2:
                raise ReturnCode('posting account: %s %s' % (status, reason))
        elif '/' not in path.rstrip('/'):
            path = path.rstrip('/')
            status, reason, headers, contents = client.post_container(
                path, headers=context.headers, query=context.query,
                cdn=context.cdn, body=body)
            if status // 100 != 2:
                raise ReturnCode(
                    'posting container %r: %s %s' % (path, status, reason))
        else:
            status, reason, headers, contents = client.post_object(
                *path.split('/', 1), headers=context.headers,
                query=context.query, cdn=context.cdn, body=body)
            if status // 100 != 2:
                raise ReturnCode(
                    'posting object %r: %s %s' % (path, status, reason))


class CLIPost(CLICommand):
    """
    A CLICommand that can issue POST requests.

    See the output of ``swiftly help post`` for more information.
    """

    def __init__(self, cli):
        super(CLIPost, self).__init__(
            cli, 'post', max_args=1, usage="""
Usage: %prog [main_options] post [options] [path]

For help on [main_options] run %prog with no args.

Issues a POST request of the [path] given. If no [path] is given, a POST
request on the account is performed.""".strip())
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
            help='Indicates where to read the POST request body from; '
                 'use a dash (as in "-i -") to specify standard input since '
                 'POSTs to Swift do not normally take input.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        path = args.pop(0).lstrip('/') if args else None
        body = None
        if options.input_:
            if options.input_ == '-':
                body = self.cli.context.io_manager.get_stdin()
            else:
                body = open(options.input_, 'rb')
        return cli_post(context, path, body=body)
