"""
Contains an extended optparse.OptionParser that works better with
command line applications that may call other command line
applications or use multiple parsers.
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
import sys
import optparse


def _stdout_filed(func):
    """
    Instance method decorator to convert an optional file keyword
    argument into an actual value, whether it be a passed value, a
    value obtained from an io_manager, or sys.stdout.
    """
    def wrapper(self, file=None):
        if file:
            return func(self, file=file)
        elif self.io_manager:
            with self.io_manager.with_stdout() as stdout:
                return func(self, file=stdout)
        else:
            return func(self, file=sys.stdout)
    wrapper.__doc__ = func.__doc__
    return wrapper


def _stderr_filed(func):
    """
    Instance method decorator to convert an optional file keyword
    argument into an actual value, whether it be a passed value, a
    value obtained from an io_manager, or sys.stderr.
    """
    def wrapper(self, msg, file=None):
        if file:
            return func(self, msg, file=file)
        elif self.io_manager:
            with self.io_manager.with_stderr() as stderr:
                return func(self, msg, file=stderr)
        else:
            return func(self, msg, file=sys.stderr)
    wrapper.__doc__ = func.__doc__
    return wrapper


class OptionParser(optparse.OptionParser, object):
    """
    Extended optparse.OptionParser that supports an io_manager,
    raw_epilog output, and an error prefix. It also does not exit
    automatically on error and instead sets an instance bool of
    error_encountered.
    """

    def __init__(self, usage=None, option_list=None,
                 option_class=optparse.Option, version=None,
                 conflict_handler='error', description=None, formatter=None,
                 add_help_option=True, prog=None, epilog=None,
                 io_manager=None, raw_epilog='', error_prefix=''):
        super(OptionParser, self).__init__(
            usage, option_list, option_class, version, conflict_handler,
            description, formatter, False, prog, epilog)
        if add_help_option:
            self.add_option(
                '-?', '--help', dest='help', action='store_true',
                help='Shows this help text.')
        if version:
            self.remove_option('--version')
            self.add_option(
                '--version', dest='version', action='store_true',
                help='Shows the version of this tool.')
        self.io_manager = io_manager
        #: Output just after the standard print_help output, in it's raw form.
        #: This is different than epilog in that epilog is reformatted.
        self.raw_epilog = raw_epilog
        #: Output just before any error. This can be useful in identification
        #: when multiple parsers are in use.
        self.error_prefix = error_prefix
        #: True if an error was encountered while parsing.
        self.error_encountered = False

    @_stderr_filed
    def error(self, msg, file=None):
        """
        Outputs the error msg to the file if specified, or to the
        io_manager's stderr if available, or to sys.stderr.
        """
        self.error_encountered = True
        file.write(self.error_prefix)
        file.write(msg)
        file.write('\n')
        file.flush()

    def exit(self, status=0, msg=None):
        """
        Immediately exits Python with the given status (or 0) as the
        exit code and optionally outputs the msg using self.error.
        """
        if msg:
            self.error(msg)
        sys.exit(status)

    @_stdout_filed
    def print_help(self, file=None):
        """
        Outputs help information to the file if specified, or to the
        io_manager's stdout if available, or to sys.stdout.
        """
        optparse.OptionParser.print_help(self, file)
        if self.raw_epilog:
            file.write(self.raw_epilog)
        file.flush()

    @_stdout_filed
    def print_usage(self, file=None):
        """
        Outputs usage information to the file if specified, or to the
        io_manager's stdout if available, or to sys.stdout.
        """
        optparse.OptionParser.print_usage(self, file)
        file.flush()

    @_stdout_filed
    def print_version(self, file=None):
        """
        Outputs version information to the file if specified, or to
        the io_manager's stdout if available, or to sys.stdout.
        """
        optparse.OptionParser.print_version(self, file)
        file.flush()
