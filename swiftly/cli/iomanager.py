"""
Contains IOManager for managing access to input, output, error, and
debug file-like objects.
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
import errno
import os
import sys


class IOManager(object):
    """
    Manages access to IO in ways that are mostly specific to
    :py:mod:`swiftly.cli` but might be generally useful.

    :param stdin: The file-like object to use for default stdin or
        sys.stdin if None.
    :param stdout: The file-like object to use for default stdout or
        sys.stdout if None.
    :param stderr: The file-like object to use for default stderr or
        sys.stderr if None.
    :param debug: The file-like object to use for default debug
        output or sys.stderr if None.
    :param stdin_root: The root path to use for requests for pathed
        stdin file-like objects.
    :param stdout_root: The root path to use for requests for pathed
        stdout file-like objects.
    :param stderr_root: The root path to use for requests for pathed
        stderr file-like objects.
    :param debug_root: The root path to use for requests for pathed
        debug file-like objects.
    :param stdin_sub_command: A shell line to pipe any stdin
        file-like objects through.
    :param stdout_sub_command: A shell line to pipe any stdout
        file-like objects through.
    :param stderr_sub_command: A shell line to pipe any stderr
        file-like objects through.
    :param debug_sub_command: A shell line to pipe any debug
        file-like objects through.
    :param subprocess_module: The subprocess module to use; for
        instance, you might use eventlet.green.subprocess instead of
        the standard subprocess module.
    :param verbose: A function to call with ``(msg)`` when waiting
        for subcommands to complete and for logging the subcommands'
        return codes.
    """

    def __init__(self, stdin=None, stdout=None, stderr=None, debug=None,
                 stdin_root=None, stdout_root=None, stderr_root=None,
                 debug_root=None, stdin_sub_command=None,
                 stdout_sub_command=None, stderr_sub_command=None,
                 debug_sub_command=None, subprocess_module=None, verbose=None):
        self.stdin = stdin or sys.stdin
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self.debug = debug or sys.stderr
        self.stdin_root = stdin_root
        self.stdout_root = stdout_root
        self.stderr_root = stderr_root
        self.debug_root = debug_root
        self.stdin_sub_command = stdin_sub_command
        self.stdout_sub_command = stdout_sub_command
        self.stderr_sub_command = stderr_sub_command
        self.debug_sub_command = debug_sub_command
        self.subprocess_module = subprocess_module
        if not self.subprocess_module:
            import subprocess
            self.subprocess_module = subprocess
        self.verbose = verbose

    def client_path_to_os_path(self, client_path):
        """
        Converts a client path into the operating system's path by
        replacing instances of '/' with os.path.sep.

        Note: If the client path contains any instances of
        os.path.sep already, they will be replaced with '-'.
        """
        if os.path.sep == '/':
            return client_path
        return client_path.replace(os.path.sep, '-').replace('/', os.path.sep)

    def os_path_to_client_path(self, os_path):
        """
        Converts an operating system path into a client path by
        replacing instances of os.path.sep with '/'.

        Note: If the client path contains any instances of '/'
        already, they will be replaced with '-'.
        """
        if os.path.sep == '/':
            return os_path
        return os_path.replace('/', '-').replace(os.path.sep, '/')

    def _get_path(self, root, os_path):
        path = None
        if root:
            if root.endswith(os.path.sep):
                if os_path:
                    path = os.path.join(root, os_path)
            else:
                path = root
        return path

    def _get_in_and_path(self, default, root, sub_command, os_path):
        inn = default
        path = self._get_path(root, os_path)
        if path:
            inn = open(path, 'rb')
        if sub_command:
            inn = self.subprocess_module.Popen(
                sub_command, shell=True, stdin=inn,
                stdout=self.subprocess_module.PIPE)
        return inn, path

    def _get_out_and_path(self, default, root, sub_command, os_path):
        out = default
        path = self._get_path(root, os_path)
        if path:
            dirname = os.path.dirname(path)
            if dirname:
                try:
                    os.makedirs(dirname)
                except OSError as err:
                    if err.errno != errno.EEXIST:
                        raise
            out = open(path, 'wb')
        if sub_command:
            out = self.subprocess_module.Popen(
                sub_command, shell=True, stdin=self.subprocess_module.PIPE,
                stdout=out)
        return out, path

    def get_stdin(self, os_path=None, skip_sub_command=False):
        """
        Returns a stdin-suitable file-like object based on the
        optional os_path and optionally skipping any configured
        sub-command.
        """
        sub_command = None if skip_sub_command else self.stdin_sub_command
        inn, path = self._get_in_and_path(
            self.stdin, self.stdin_root, sub_command, os_path)
        if hasattr(inn, 'stdout'):
            return inn.stdout
        return inn

    def get_stdout(self, os_path=None, skip_sub_command=False):
        """
        Returns a stdout-suitable file-like object based on the
        optional os_path and optionally skipping any configured
        sub-command.
        """
        sub_command = None if skip_sub_command else self.stdout_sub_command
        out, path = self._get_out_and_path(
            self.stdout, self.stdout_root, sub_command, os_path)
        if hasattr(out, 'stdin'):
            return out.stdin
        return out

    def get_stderr(self, os_path=None, skip_sub_command=False):
        """
        Returns a stderr-suitable file-like object based on the
        optional os_path and optionally skipping any configured
        sub-command.
        """
        sub_command = None if skip_sub_command else self.stderr_sub_command
        out, path = self._get_out_and_path(
            self.stderr, self.stderr_root, sub_command, os_path)
        if hasattr(out, 'stdin'):
            return out.stdin
        return out

    def get_debug(self, os_path=None, skip_sub_command=False):
        """
        Returns a debug-output-suitable file-like object based on the
        optional os_path and optionally skipping any configured
        sub-command.
        """
        sub_command = None if skip_sub_command else self.debug_sub_command
        out, path = self._get_out_and_path(
            self.debug, self.debug_root, sub_command, os_path)
        if hasattr(out, 'stdin'):
            return out.stdin
        return out

    def _wait(self, item, path):
        if hasattr(item, 'wait'):
            if self.verbose:
                msg = 'Waiting on sub-command'
                if path:
                    msg += ' to close %s' % path
                self.verbose(msg)
            exit_code = item.wait()
            if self.verbose:
                msg = 'Sub-command exited with %s' % exit_code
                if path:
                    msg += ' and closed %s' % path
                self.verbose(msg)

    def _close(self, item):
        if item not in (self.stdin, self.stdout, self.stderr, self.debug):
            if hasattr(item, 'close'):
                item.close()

    @contextlib.contextmanager
    def with_stdin(self, os_path=None, skip_sub_command=False,
                   disk_closed_callback=None):
        """
        A context manager yielding a stdin-suitable file-like object
        based on the optional os_path and optionally skipping any
        configured sub-command.

        :param os_path: Optional path to base the file-like object
            on.
        :param skip_sub_command: Set True to skip any configured
            sub-command filter.
        :param disk_closed_callback: If the backing of the file-like
            object is an actual file that will be closed,
            disk_closed_callback (if set) will be called with the
            on-disk path just after closing it.
        """
        sub_command = None if skip_sub_command else self.stdin_sub_command
        inn, path = self._get_in_and_path(
            self.stdin, self.stdin_root, sub_command, os_path)
        try:
            if hasattr(inn, 'stdout'):
                yield inn.stdout
            else:
                yield inn
        finally:
            if hasattr(inn, 'stdout'):
                self._close(inn.stdout)
            self._wait(inn, path)
            self._close(inn)
            if disk_closed_callback and path:
                disk_closed_callback(path)

    @contextlib.contextmanager
    def with_stdout(self, os_path=None, skip_sub_command=False,
                    disk_closed_callback=None):
        """
        A context manager yielding a stdout-suitable file-like object
        based on the optional os_path and optionally skipping any
        configured sub-command.

        :param os_path: Optional path to base the file-like object
            on.
        :param skip_sub_command: Set True to skip any configured
            sub-command filter.
        :param disk_closed_callback: If the backing of the file-like
            object is an actual file that will be closed,
            disk_closed_callback (if set) will be called with the
            on-disk path just after closing it.
        """
        sub_command = None if skip_sub_command else self.stdout_sub_command
        out, path = self._get_out_and_path(
            self.stdout, self.stdout_root, sub_command, os_path)
        try:
            if hasattr(out, 'stdin'):
                yield out.stdin
            else:
                yield out
        finally:
            if hasattr(out, 'stdin'):
                self._close(out.stdin)
            self._wait(out, path)
            self._close(out)
            if disk_closed_callback and path:
                disk_closed_callback(path)

    @contextlib.contextmanager
    def with_stderr(self, os_path=None, skip_sub_command=False,
                    disk_closed_callback=None):
        """
        A context manager yielding a stderr-suitable file-like object
        based on the optional os_path and optionally skipping any
        configured sub-command.

        :param os_path: Optional path to base the file-like object
            on.
        :param skip_sub_command: Set True to skip any configured
            sub-command filter.
        :param disk_closed_callback: If the backing of the file-like
            object is an actual file that will be closed,
            disk_closed_callback (if set) will be called with the
            on-disk path just after closing it.
        """
        sub_command = None if skip_sub_command else self.stderr_sub_command
        out, path = self._get_out_and_path(
            self.stderr, self.stderr_root, sub_command, os_path)
        try:
            if hasattr(out, 'stdin'):
                yield out.stdin
            else:
                yield out
        finally:
            if hasattr(out, 'stdin'):
                self._close(out.stdin)
            self._wait(out, path)
            self._close(out)
            if disk_closed_callback and path:
                disk_closed_callback(path)

    @contextlib.contextmanager
    def with_debug(self, os_path=None, skip_sub_command=False,
                   disk_closed_callback=None):
        """
        A context manager yielding a debug-output-suitable file-like
        object based on the optional os_path and optionally skipping
        any configured sub-command.

        :param os_path: Optional path to base the file-like object
            on.
        :param skip_sub_command: Set True to skip any configured
            sub-command filter.
        :param disk_closed_callback: If the backing of the file-like
            object is an actual file that will be closed,
            disk_closed_callback (if set) will be called with the
            on-disk path just after closing it.
        """
        sub_command = None if skip_sub_command else self.debug_sub_command
        out, path = self._get_out_and_path(
            self.debug, self.debug_root, sub_command, os_path)
        try:
            if hasattr(out, 'stdin'):
                yield out.stdin
            else:
                yield out
        finally:
            if hasattr(out, 'stdin'):
                self._close(out.stdin)
            self._wait(out, path)
            self._close(out)
            if disk_closed_callback and path:
                disk_closed_callback(path)
