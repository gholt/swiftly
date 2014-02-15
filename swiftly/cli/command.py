"""
Contains the CLICommand class that can be subclassed to create new
Swiftly commands.
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
from swiftly.cli.optionparser import OptionParser


class ReturnCode(Exception):
    """
    Raise this to indicate the desire to exit.

    :param text: The text to report as the reason for exiting.
        Default: None
    :param code: The return code to give back to the shell.
        Default: 1
    """

    def __init__(self, text=None, code=1):
        Exception.__init__(self, text)
        self.text = text
        self.code = code


class CLICommand(object):
    """
    Subclass this to create new Swiftly commands.

    Don't forget to add your new class to
    :py:attr:`swiftly.cli.cli.COMMANDS`.

    Your subclass will be created by :py:class:`swiftly.cli.cli.CLI`
    with the CLI instance as the only parameter. You should then call
    this superclass with that CLI instance, your command's name, and
    any other options desired.

    See the implemention for other commands like
    :py:class:`swiftly.cli.auth.CLIAuth` for good starting points.

    :param cli: The :py:class:`swiftly.cli.cli.CLI` instance.
    :param name: The name of the command.
    :param min_args: The minimum number of arguments required.
    :param max_args: The maximum number of arguments allowed.
    :param usage: The usage string for
        :py:class:`OptionParser`.
    """

    def __init__(self, cli, name, min_args=None, max_args=None, usage=None):
        self.cli = cli
        self.name = name
        self.min_args = min_args
        self.max_args = max_args
        if usage is None:
            usage = """
Usage: %%prog [main_options] %s

For help on [main_options] run %%prog with no args.

Executes the %s command.""".strip() % (name, name)
        self.option_parser = OptionParser(
            usage=usage, io_manager=self.cli.context.io_manager,
            error_prefix=name + ' command: ')

    def parse_args_and_create_context(self, args):
        """
        Helper method that will parse the args into options and
        remaining args as well as create an initial
        :py:class:`swiftly.cli.context.CLIContext`.

        The new context will be a copy of
        :py:attr:`swiftly.cli.cli.CLI.context` with the following
        attributes added:

        =======================  ===================================
        muted_account_headers    The headers to omit when outputting
                                 account headers.
        muted_container_headers  The headers to omit when outputting
                                 container headers.
        muted_object_headers     The headers to omit when outputting
                                 object headers.
        =======================  ===================================

        :returns: options, args, context
        """
        original_args = args
        try:
            options, args = self.option_parser.parse_args(args)
        except UnboundLocalError:
            # Happens sometimes with an error handler that doesn't raise its
            # own exception. We'll catch the error below with
            # error_encountered.
            pass
        if self.option_parser.error_encountered:
            if '-?' in original_args or '-h' in original_args or \
                    '--help' in original_args:
                self.option_parser.print_help()
            raise ReturnCode()
        if options.help:
            self.option_parser.print_help()
            raise ReturnCode()
        if self.min_args is not None and len(args) < self.min_args:
            raise ReturnCode(
                'requires at least %s args.' % self.min_args)
        if self.max_args is not None and len(args) > self.max_args:
            raise ReturnCode(
                'requires no more than %s args.' % self.max_args)
        context = self.cli.context.copy()
        context.muted_account_headers = [
            'accept-ranges', 'content-length', 'content-type', 'date']
        context.muted_container_headers = [
            'accept-ranges', 'content-length', 'content-type', 'date']
        context.muted_object_headers = ['accept-ranges', 'date']
        return options, args, context

    def options_list_to_lowered_dict(self, options_list):
        """
        Helper function that will convert an options list into a dict
        of key/values.

        This is used for the quite common -hheader:value and
        -qparameter=value command line options, like this::

            context.headers = self.options_list_to_lowered_dict(options.header)
            context.query = self.options_list_to_lowered_dict(options.query)

        For a full example, see
        :py:func:`swiftly.cli.get.CLIGet.__call__`.
        """
        result = {}
        if options_list:
            for key in options_list:
                key = key.lstrip()
                colon = key.find(':')
                if colon < 1:
                    colon = None
                equal = key.find('=')
                if equal < 1:
                    equal = None
                if colon and (not equal or colon < equal):
                    key, value = key.split(':', 1)
                elif equal:
                    key, value = key.split('=', 1)
                else:
                    value = ''
                result[key.lower()] = value.lstrip()
        return result
