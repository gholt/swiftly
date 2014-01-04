"""
Contains the CLIContext class used to pass contextual information to
CLI functions.
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


class CLIContext(object):
    """
    Used to pass contextual information to CLI functions.

    An instance of this class will allow almost any attribute access,
    even if that attribute did not previously exist. It offers an
    easy way to set new attributes on the fly for different use cases
    without having to create a whole new subclasses.
    """

    def __getattr__(self, name):
        try:
            return getattr(super(CLIContext, self), name)
        except AttributeError:
            if name == 'verbose':
                return lambda *a, **k: None
            return None

    def __repr__(self):
        result = super(CLIContext, self).__repr__()
        for item in dir(self):
            if item[0] != '_' and item != 'copy':
                result += '\n    %s = %s' % (item, getattr(self, item))
        return result

    def copy(self):
        """
        Returns a new CLIContext instance that is a shallow copy of
        the original, much like dict's copy method.
        """
        context = CLIContext()
        for item in dir(self):
            if item[0] != '_' and item not in ('copy', 'write_headers'):
                setattr(context, item, getattr(self, item))
        return context

    def write_headers(self, fp, headers, mute=None):
        """
        Convenience function to output headers in a formatted fashion
        to a file-like fp, optionally muting any headers in the mute
        list.
        """
        if headers:
            if not mute:
                mute = []
            fmt = '%%-%ds %%s\n' % (max(len(k) for k in headers) + 1)
            for key in sorted(headers):
                if key in mute:
                    continue
                fp.write(fmt % (key.title() + ':', headers[key]))
            fp.flush()
