"""
Contains the ClientManager class that can be used to manage a set of
clients.
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
from six.moves import queue


class ClientManager(object):
    """
    Can be used to manage a set of clients.

    :param client_class: The class to create when a new client is
        needed.
    :param args: The args for the client constructor.
    :param kwargs: The keyword args for the client constructor.
    """

    def __init__(self, client_class, *args, **kwargs):
        self.client_class = client_class
        self.args = args
        self.kwargs = kwargs
        self.clients = queue.Queue()
        self.client_id = 0

    def get_client(self):
        """
        Obtains a client for use, whether an existing unused client
        or a brand new one if none are available.
        """
        client = None
        try:
            client = self.clients.get(block=False)
        except queue.Empty:
            pass
        if not client:
            self.client_id += 1
            kwargs = dict(self.kwargs)
            kwargs['verbose_id'] = kwargs.get(
                'verbose_id', '') + str(self.client_id)
            client = self.client_class(*self.args, **kwargs)
        return client

    def put_client(self, client):
        """
        Returns a client back into the pool for availability to
        future calls to get_client. This should only be called if
        get_client was used to obtain the client; with_client is a
        context manager that does this for you.
        """
        self.clients.put(client)

    @contextlib.contextmanager
    def with_client(self):
        """
        A context manager that obtains a client for use, whether an
        existing unused client or a brand new one if none are
        available.
        """
        client = self.get_client()
        yield client
        self.put_client(client)
