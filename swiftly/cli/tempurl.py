"""
Contains a CLICommand for generating TempURLs.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

==============  ========================
io_manager      For directing output.
client_manager  For connecting to Swift.
==============  ========================
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
import contextlib

from swiftly.client import generate_temp_url
from swiftly.cli.command import CLICommand, ReturnCode


def cli_tempurl(context, method, path, seconds=None, use_container=False):
    """
    Generates a TempURL and sends that to the context.io_manager's
    stdout.

    See :py:mod:`swiftly.cli.tempurl` for context usage information.

    See :py:class:`CLITempURL` for more information.

    :param context: The :py:class:`swiftly.cli.context.CLIContext` to
        use.
    :param method: The method for the TempURL (GET, PUT, etc.)
    :param path: The path the TempURL should direct to.
    :param seconds: The number of seconds the TempURL should be good
        for. Default: 3600
    :param use_container: If True, will create a container level TempURL
        useing X-Container-Meta-Temp-Url-Key instead of
        X-Account-Meta-Temp-Url-Key.
    """
    with contextlib.nested(
            context.io_manager.with_stdout(),
            context.client_manager.with_client()) as (fp, client):
        method = method.upper()
        path = path.lstrip('/')
        seconds = seconds if seconds is not None else 3600
        if '/' not in path:
            raise ReturnCode(
                'invalid tempurl path %r; should have a / within it' % path)
        if use_container:
            key_type = 'container'
            container = path.split('/', 1)[0]
            status, reason, headers, contents = \
                client.head_container(container)
        else:
            key_type = 'account'
            status, reason, headers, contents = \
                client.head_account()
        if status // 100 != 2:
            raise ReturnCode(
                'obtaining X-%s-Meta-Temp-Url-Key: %s %s' %
                (key_type.title(), status, reason))
        key = headers.get('x-%s-meta-temp-url-key' % key_type)
        if not key:
            raise ReturnCode(
                'there is no X-%s-Meta-Temp-Url-Key set for this %s' %
                (key_type.title(), key_type))
        url = client.storage_url + '/' + path
        fp.write(generate_temp_url(method, url, seconds, key))
        fp.write('\n')
        fp.flush()


class CLITempURL(CLICommand):
    """
    A CLICommand for generating TempURLs.

    See the output of ``swiftly help tempurl`` for more information.
    """

    def __init__(self, cli):
        super(CLITempURL, self).__init__(
            cli, 'tempurl', min_args=2, max_args=3, usage="""
Usage: %prog [main_options] tempurl [options] <method> <path> [seconds]

For help on [main_options] run %prog with no args.

Outputs a TempURL using the information given.
The <path> should be to an object or object-prefix.
[seconds] defaults to 3600""".strip())
        self.option_parser.add_option(
            '--use_container', '-c', action='store_true', default=False,
            help='Create TempURL using container key.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        method = args.pop(0)
        path = args.pop(0)
        seconds = int(args.pop(0)) if args else None
        return cli_tempurl(context, method, path, seconds,
                           use_container=options.use_container)
