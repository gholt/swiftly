"""
Wraps an iterable to behave as a file-like object.

Copyright (c) 2010-2012 OpenStack Foundation

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


class FileLikeIter(object):
    """
    Wraps an iterable to behave as a file-like object.

    Taken from work I did for OpenStack Swift
    swift.common.utils.FileLikeIter, Copyright (c) 2010-2012
    OpenStack Foundation.
    """

    def __init__(self, iterable, limit=None):
        self.iterator = iter(iterable)
        self.limit = limit
        self.left = limit
        self.buf = None
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        """
        x.__next__() -> the next value, or raise StopIteration
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.buf:
            rv = self.buf
            self.buf = None
            return rv
        else:
            return next(self.iterator)

    def reset_limit(self):
        """
        Resets the limit.
        """
        self.left = self.limit

    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
        if self.left is not None:
            size = min(size, self.left)
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if size < 0:
            return ''.join(self)
        elif not size:
            chunk = ''
        elif self.buf:
            chunk = self.buf
            self.buf = None
        else:
            try:
                chunk = next(self.iterator)
            except StopIteration:
                return ''
        if len(chunk) > size:
            self.buf = chunk[size:]
            chunk = chunk[:size]
        if self.left is not None:
            self.left -= len(chunk)
        return chunk

    def readline(self, size=-1):
        """
        readline([size]) -> next line from the file, as a string.

        Retain newline.  A non-negative size argument limits the maximum
        number of bytes to return (an incomplete line may be returned then).
        Return an empty string at EOF.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        data = ''
        while '\n' not in data and (size < 0 or len(data) < size):
            if size < 0:
                chunk = self.read(1024)
            else:
                chunk = self.read(size - len(data))
            if not chunk:
                break
            data += chunk
        if '\n' in data:
            data, sep, rest = data.partition('\n')
            data += sep
            if self.buf:
                self.buf = rest + self.buf
            else:
                self.buf = rest
        return data

    def readlines(self, sizehint=-1):
        """
        readlines([size]) -> list of strings, each a line from the file.

        Call readline() repeatedly and return a list of the lines so read.
        The optional size argument, if given, is an approximate bound on the
        total number of bytes in the lines returned.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        lines = []
        while True:
            line = self.readline(sizehint)
            if not line:
                break
            lines.append(line)
            if sizehint >= 0:
                sizehint -= len(line)
                if sizehint <= 0:
                    break
        return lines

    def is_empty(self):
        """
        Check whether the "file" is empty reading the single byte.
        """
        something = self.read(1)
        if something:
            if self.buf:
                self.buf = something + self.buf
            else:
                self.buf = something
            return False
        else:
            return True

    def close(self):
        """
        close() -> None or (perhaps) an integer.  Close the file.

        Sets data attribute .closed to True.  A closed file cannot be used for
        further I/O operations.  close() may be called more than once without
        error.  Some kinds of file objects (for example, opened by popen())
        may return an exit status upon closing.
        """
        self.iterator = None
        self.closed = True
