"""
Contains a CLICommand that outputs help information.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

=======================  ============================================
io_manager               For directing output.
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


def cli_help(context, command_name, general_parser, command_parsers):
    """
    Outputs help information.

    See :py:mod:`swiftly.cli.help` for context usage information.

    See :py:class:`CLIHelp` for more information.

    :param context: The :py:class:`swiftly.cli.context.CLIContext` to
        use.
    :param command_name: The command_name to output help information
        for, or set to None or an empty string to output the general
        help information.
    :param general_parser: The
        :py:class:`swiftly.cli.optionparser.OptionParser` for general
        usage.
    :param command_parsers: A dict of (name, :py:class:`CLICommand`)
        for specific command usage.
    """
    if command_name == 'for':
        command_name = 'fordo'
    with context.io_manager.with_stdout() as stdout:
        if not command_name:
            general_parser.print_help(stdout)
        elif command_name in command_parsers:
            command_parsers[command_name].option_parser.print_help(stdout)
        else:
            raise ReturnCode('unknown command %r' % command_name)


class CLIHelp(CLICommand):
    """
    A CLICommand that outputs help information.

    See the output of ``swiftly help help`` for more information.
    """

    def __init__(self, cli):
        super(CLIHelp, self).__init__(
            cli, 'help', max_args=1, usage="""
Usage: %prog [main_options] help [command]

For help on [main_options] run %prog with no args.

Outputs help information for the given [command] or general help if no
[command] is given.""".strip())

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        command_name = args.pop(0) if args else None
        general_parser = self.cli.option_parser
        command_parsers = self.cli.commands
        return cli_help(context, command_name, general_parser, command_parsers)
