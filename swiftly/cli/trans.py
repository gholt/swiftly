"""
Contains a CLICommand for translating transaction identifiers.

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
import time

from swiftly.client import get_trans_id_time
from swiftly.cli.command import CLICommand


def cli_trans(context, x_trans_id):
    """
    Translates any information that can be determined from the
    x_trans_id and sends that to the context.io_manager's stdout.

    See :py:mod:`swiftly.cli.trans` for context usage information.

    See :py:class:`CLITrans` for more information.
    """
    with context.io_manager.with_stdout() as fp:
        trans_time = get_trans_id_time(x_trans_id)
        trans_info = x_trans_id[34:]
        msg = 'X-Trans-Id:      ' + x_trans_id + '\n'
        if not trans_time:
            msg += 'Time Stamp:      None, old style id with no time ' \
                'embedded\nUTC Time:        None, old style id with no time ' \
                'embedded\n'
        else:
            msg += 'Time Stamp:      %s\nUTC Time:        %s\n' % (
                trans_time,
                time.strftime(
                    '%a %Y-%m-%d %H:%M:%S UTC', time.gmtime(trans_time)))
        msg += 'Additional Info: ' + trans_info + '\n'
        fp.write(msg)
        fp.flush()


class CLITrans(CLICommand):
    """
    A CLICommand for translating transaction identifiers.

    See the output of ``swiftly help trans`` for more information.
    """

    def __init__(self, cli):
        super(CLITrans, self).__init__(
            cli, 'trans', min_args=1, max_args=1, usage="""
Usage: %prog [main_options] trans <x-trans-id-value>

For help on [main_options] run %prog with no args.

Outputs information about the <x-trans-id-value> given, such as the embedded
transaction time.""".strip())

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        x_trans_id = args.pop(0)
        return cli_trans(context, x_trans_id)
