"""
Contains a CLICommand for decrypting stdin to stdout.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

==========  =====================
io_manager  For directing output.
==========  =====================
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

from swiftly.cli.command import CLICommand, ReturnCode
from swiftly.dencrypt import AES256CBC, aes_decrypt


def cli_decrypt(context, key):
    """
    Decrypts context.io_manager's stdin and sends that to
    context.io_manager's stdout.

    See :py:mod:`swiftly.cli.decrypt` for context usage information.

    See :py:class:`CLIDecrypt` for more information.
    """
    with context.io_manager.with_stdout() as stdout:
        with context.io_manager.with_stdin() as stdin:
            crypt_type = stdin.read(1)
            if crypt_type == AES256CBC:
                for chunk in aes_decrypt(key, stdin):
                    stdout.write(chunk)
                stdout.flush()
            else:
                raise ReturnCode(
                    'contents encrypted with unsupported type %r' % crypt_type)


class CLIDecrypt(CLICommand):
    """
    A CLICommand for decrypting stdin and sending that to stdout.

    See the output of ``swiftly help decrypt`` for more information.
    """

    def __init__(self, cli):
        super(CLIDecrypt, self).__init__(
            cli, 'decrypt', max_args=1, usage="""
Usage: %prog [main_options] decrypt [key]

For help on [main_options] run %prog with no args.

Decrypts standard input using the given [key] and sends that to standard
output. If the key is not provided on the command line or is a single dash "-",
it must be provided via a SWIFTLY_CRYPT_KEY environment variable.

This currently only supports AES 256 in CBC mode but other algorithms may be
offered in the future.
""".strip())

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        key = args.pop(0) if args else None
        if not key or key == '-':
            key = os.environ.get('SWIFTLY_CRYPT_KEY')
        if not key:
            raise ReturnCode(
                'No key provided and no SWIFTLY_CRYPT_KEY in the environment.')
        return cli_decrypt(context, key)
