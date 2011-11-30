"""
Concurrency API for Swiftly.

Copyright 2011 Gregory Holt

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

__all__ = ['Concurrency']

from Queue import Empty, Queue

try:
    from eventlet import GreenPool
except ImportError:
    GreenPool = None


class Concurrency(object):
    """
    Convenience class to support concurrency, if Eventlet is available;
    otherwise it just performs at single concurrency.

    :param concurrency: The level of concurrency desired. Default: 10
    """

    def __init__(self, concurrency=10):
        if concurrency and GreenPool:
            self._pool = GreenPool(concurrency)
        else:
            self._pool = None
        self._queue = Queue()
        self._results = {}

    def _spawner(self, ident, func, *args, **kwargs):
        self._queue.put((ident, func(*args, **kwargs)))

    def spawn(self, ident, func, *args, **kwargs):
        """
        Returns immediately to the caller and begins executing the func in
        the background. Use get_results and the ident given to retrieve the
        results of the func.

        :param ident: An identifier to find the results of the func from
                      get_results. This identifier can be anything unique to
                      the Concurrency instance.
        :param func: The function to execute in the concurrently.
        :param args: The args to give the func.
        :param kwargs: The keyword args to the give the func.
        :returns: None
        """
        if self._pool:
            self._pool.spawn_n(self._spawner, ident, func, *args, **kwargs)
        else:
            self._spawner(ident, func, *args, **kwargs)

    def get_results(self):
        """
        Returns a dict of the results currently available. The keys are the
        ident values given with the calls to spawn. The values are the return
        values of the funcs.
        """
        try:
            while True:
                ident, value = self._queue.get(block=False)
                self._results[ident] = value
        except Empty:
            pass
        return self._results

    def join(self):
        """
        Blocks until all currently pending functions have finished.
        """
        if self._pool:
            self._pool.waitall()
