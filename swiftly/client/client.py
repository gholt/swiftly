"""
Contains the base Client class for accessing Swift services.
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
from swiftly import VERSION
from swiftly.client.utils import quote


class Client(object):
    """
    The base class for accessing Swift services.

    For concrete examples, see
    :py:class:`swiftly.client.standardclient.StandardClient` and
    :py:class:`swiftly.client.directclient.DirectClient`.

    To form a new concrete subclass, you would need to implement
    :py:func:`request` and :py:func:`get_account_hash` minimally and
    optionally :py:func:`reset` and :py:func:`auth`.
    """

    def __init__(self):
        #: The string to use for the User-Agent request header.
        self.user_agent = 'Swiftly v%s' % VERSION
        #: These HTTP methods do not allow contents
        self.no_content_methods = ['COPY', 'DELETE', 'GET', 'HEAD']

    def reset(self):
        """
        Resets the client, closing any connections and discarding any
        state. This can be useful if some exceptional condition
        occurred and the request/response state can no longer be
        certain.
        """
        pass

    def auth(self):
        """
        Just performs any authentication steps without making an
        actual request to the Swift system.
        """
        pass

    def request(self, method, path, contents, headers, decode_json=False,
                stream=False, query=None, cdn=False):
        """
        Performs a direct HTTP request to the Swift service.

        :param method: The request method ('GET', 'HEAD', etc.)
        :param path: The request path.
        :param contents: The body of the request. May be a string or
            a file-like object.
        :param headers: A dict of request headers and values.
        :param decode_json: If set True, the response body will be
            treated as JSON and decoded result returned instead of
            the raw contents.
        :param stream: If set True, the response body will return as
            a file-like object; otherwise, the response body will be
            read in its entirety and returned as a string. Overrides
            decode_json.
        :param query: A dict of query parameters and values to append
            to the path.
        :param cdn: If set True, the request will be sent to the CDN
            management endpoint instead of the default storage
            endpoint.
        :returns: A tuple of (status, reason, headers, contents).

            :status: An int for the HTTP status code.
            :reason: The str for the HTTP status (ex: "Ok").
            :headers: A dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be
                a list.
            :contents: Depending on the decode_json and stream
                settings, this will either be the raw response
                string, the JSON decoded object, or a file-like
                object.
        """
        raise Exception('request method not implemented')

    def get_account_hash(self):
        """
        Returns the account identifier for the Swift account being
        accessed.
        """
        raise Exception('get_account_hash method not implemented')

    def _container_path(self, container):
        container = container.rstrip('/')
        if container.startswith('/'):
            return quote(container)
        else:
            return '/' + quote(container)

    def _object_path(self, container, obj):
        container = self._container_path(container)
        # Leading/trailing slashes are allowed in object names, so don't strip
        # them.
        return container + '/' + quote(obj)

    def head_account(self, headers=None, query=None, cdn=False):
        """
        HEADs the account and returns the results. Useful headers
        returned are:

        =========================== =================================
        x-account-bytes-used        Object storage used for the
                                    account, in bytes.
        x-account-container-count   The number of containers in the
                                    account.
        x-account-object-count      The number of objects in the
                                    account.
        =========================== =================================

        Also, any user headers beginning with x-account-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self.request(
            'HEAD', '', '', headers, query=query, cdn=cdn)

    def get_account(self, headers=None, prefix=None, delimiter=None,
                    marker=None, end_marker=None, limit=None, query=None,
                    cdn=False, decode_json=True):
        """
        GETs the account and returns the results. This is done to list
        the containers for the account. Some useful headers are also
        returned:

        =========================== =================================
        x-account-bytes-used        Object storage used for the
                                    account, in bytes.
        x-account-container-count   The number of containers in the
                                    account.
        x-account-object-count      The number of objects in the
                                    account.
        =========================== =================================

        Also, any user headers beginning with x-account-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param headers: Additional headers to send with the request.
        :param prefix: The prefix container names must match to be
            listed.
        :param delimiter: The delimiter for the listing. Delimiters
            indicate how far to progress through container names
            before "rolling them up". For instance, a delimiter='.'
            query on an account with the containers::

                one.one
                one.two
                two
                three.one

            would return the JSON value of::

                [{'subdir': 'one.'},
                 {'count': 0, 'bytes': 0, 'name': 'two'},
                 {'subdir': 'three.'}]

            Using this with prefix can allow you to traverse a psuedo
            hierarchy.
        :param marker: Only container names after this marker will be
            returned. Swift returns a limited number of containers
            per request (often 10,000). To get the next batch of
            names, you issue another query with the marker set to the
            last name you received. You can continue to issue
            requests until you receive no more names.
        :param end_marker: Only container names before this marker will be
            returned.
        :param limit: Limits the size of the list returned per
            request. The default and maximum depends on the Swift
            cluster (usually 10,000).
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param decode_json: If set False, the usual decoding of the
            JSON response will be skipped and the raw contents will
            be returned instead.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the decoded JSON response or the raw str
                for the HTTP body.
        """
        query = dict(query or {})
        query['format'] = 'json'
        if prefix:
            query['prefix'] = prefix
        if delimiter:
            query['delimiter'] = delimiter
        if marker:
            query['marker'] = marker
        if end_marker:
            query['end_marker'] = end_marker
        if limit:
            query['limit'] = limit
        return self.request(
            'GET', '', '', headers, decode_json=decode_json, query=query,
            cdn=cdn)

    def put_account(self, headers=None, query=None, cdn=False, body=None):
        """
        PUTs the account and returns the results. This is usually
        done with the extract-archive bulk upload request and has no
        other use I know of (but the call is left open in case there
        ever is).

        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some account PUT requests, like the
            extract-archive bulk upload request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self.request(
            'PUT', '', body or '', headers, query=query, cdn=cdn)

    def post_account(self, headers=None, query=None, cdn=False, body=None):
        """
        POSTs the account and returns the results. This is usually
        done to set X-Account-Meta-xxx headers. Note that any existing
        X-Account-Meta-xxx headers will remain untouched. To remove an
        X-Account-Meta-xxx header, send the header with an empty
        string as its value.

        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        return self.request(
            'POST', '', body or '', headers, query=query, cdn=cdn)

    def delete_account(self, headers=None,
                       yes_i_mean_delete_the_account=False, query=None,
                       cdn=False, body=None):
        """
        Sends a DELETE request to the account and returns the results.

        With ``query['bulk-delete'] = ''`` this might mean a bulk
        delete request where the body of the request is new-line
        separated, url-encoded list of names to delete. Be careful
        with this! One wrong move and you might mark your account for
        deletion of you have the access to do so!

        For a plain DELETE to the account, on clusters that support
        it and, assuming you have permissions to do so, the account
        will be marked as deleted and immediately begin removing the
        objects from the cluster in the backgound.

        THERE IS NO GOING BACK!

        :param headers: Additional headers to send with the request.
        :param yes_i_mean_delete_the_account: Set to True to verify
            you really mean to delete the entire account. This is
            required unless ``body and 'bulk-delete' in query``.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some account DELETE requests, like the bulk
            delete request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be
                a list.
            :contents: is the str for the HTTP body.
        """
        if not yes_i_mean_delete_the_account and (
                not body or not query or 'bulk-delete' not in query):
            return (0, 'yes_i_mean_delete_the_account was not set to True', {},
                    '')
        return self.request(
            'DELETE', '', body or '', headers, query=query, cdn=cdn)

    def head_container(self, container, headers=None, query=None, cdn=False):
        """
        HEADs the container and returns the results. Useful headers
        returned are:

        =========================== =================================
        x-container-bytes-used      Object storage used for the
                                    container, in bytes.
        x-container-object-count    The number of objects in the
                                    container.
        =========================== =================================

        Also, any user headers beginning with x-container-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self.request(
            'HEAD', path, '', headers, query=query, cdn=cdn)

    def get_container(self, container, headers=None, prefix=None,
                      delimiter=None, marker=None, end_marker=None,
                      limit=None, query=None, cdn=False, decode_json=True):
        """
        GETs the container and returns the results. This is done to
        list the objects for the container. Some useful headers are
        also returned:

        =========================== =================================
        x-container-bytes-used      Object storage used for the
                                    container, in bytes.
        x-container-object-count    The number of objects in the
                                    container.
        =========================== =================================

        Also, any user headers beginning with x-container-meta- are
        returned.

        These values can be delayed depending the Swift cluster.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param prefix: The prefix object names must match to be
            listed.
        :param delimiter: The delimiter for the listing. Delimiters
            indicate how far to progress through object names before
            "rolling them up". For instance, a delimiter='/' query on
            an container with the objects::

                one/one
                one/two
                two
                three/one

            would return the JSON value of::

                [{'subdir': 'one/'},
                 {'count': 0, 'bytes': 0, 'name': 'two'},
                 {'subdir': 'three/'}]

            Using this with prefix can allow you to traverse a psuedo
            hierarchy.
        :param marker: Only object names after this marker will be
            returned. Swift returns a limited number of objects per
            request (often 10,000). To get the next batch of names,
            you issue another query with the marker set to the last
            name you received. You can continue to issue requests
            until you receive no more names.
        :param end_marker: Only object names before this marker will be
            returned.
        :param limit: Limits the size of the list returned per
            request. The default and maximum depends on the Swift
            cluster (usually 10,000).
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param decode_json: If set False, the usual decoding of the
            JSON response will be skipped and the raw contents will
            be returned instead.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the decoded JSON response or the raw str
                for the HTTP body.
        """
        query = dict(query or {})
        query['format'] = 'json'
        if prefix:
            query['prefix'] = prefix
        if delimiter:
            query['delimiter'] = delimiter
        if marker:
            query['marker'] = marker
        if end_marker:
            query['end_marker'] = end_marker
        if limit:
            query['limit'] = limit
        return self.request(
            'GET', self._container_path(container), '', headers,
            decode_json=decode_json, query=query, cdn=cdn)

    def put_container(self, container, headers=None, query=None, cdn=False,
                      body=None):
        """
        PUTs the container and returns the results. This is usually
        done to create new containers and can also be used to set
        X-Container-Meta-xxx headers. Note that if the container
        already exists, any existing X-Container-Meta-xxx headers will
        remain untouched. To remove an X-Container-Meta-xxx header,
        send the header with an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some container PUT requests, like the
            extract-archive bulk upload request, take a body.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self.request(
            'PUT', path, body or '', headers, query=query, cdn=cdn)

    def post_container(self, container, headers=None, query=None, cdn=False,
                       body=None):
        """
        POSTs the container and returns the results. This is usually
        done to set X-Container-Meta-xxx headers. Note that any
        existing X-Container-Meta-xxx headers will remain untouched.
        To remove an X-Container-Meta-xxx header, send the header with
        an empty string as its value.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self.request(
            'POST', path, body or '', headers, query=query, cdn=cdn)

    def delete_container(self, container, headers=None, query=None, cdn=False,
                         body=None):
        """
        DELETEs the container and returns the results.

        :param container: The name of the container.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some container DELETE requests might take a body
            in the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._container_path(container)
        return self.request(
            'DELETE', path, body or '', headers, query=query, cdn=cdn)

    def head_object(self, container, obj, headers=None, query=None, cdn=False):
        """
        HEADs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self.request(
            'HEAD', path, '', headers, query=query, cdn=cdn)

    def get_object(self, container, obj, headers=None, stream=True, query=None,
                   cdn=False):
        """
        GETs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param stream: Indicates whether to stream the contents or
            preread them fully and return them as a str. Default:
            True to stream the contents. When streaming, contents
            will have the standard file-like-object read function,
            which accepts an optional size parameter to limit how
            much data is read per call. When streaming is on, be
            certain to fully read the contents before issuing another
            request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: if *stream* was True, *contents* is a
                file-like-object of the contents of the HTTP body. If
                *stream* was False, *contents* is just a simple str of
                the HTTP body.
        """
        path = self._object_path(container, obj)
        return self.request(
            'GET', path, '', headers, query=query, stream=stream, cdn=cdn)

    def put_object(self, container, obj, contents, headers=None, query=None,
                   cdn=False):
        """
        PUTs the object and returns the results. This is used to
        create or overwrite objects. X-Object-Meta-xxx can optionally
        be sent to be stored with the object. Content-Type,
        Content-Encoding and other standard HTTP headers can often
        also be set, depending on the Swift cluster.

        Note that you can set the ETag header to the MD5 sum of the
        contents for extra verification the object was stored
        correctly.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param contents: The contents of the object to store. This can
            be a simple str, or a file-like-object with at least a
            read function. If the file-like-object also has tell and
            seek functions, the PUT can be reattempted on any server
            error.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self.request(
            'PUT', path, contents, headers, query=query, cdn=cdn)

    def post_object(self, container, obj, headers=None, query=None, cdn=False,
                    body=None):
        """
        POSTs the object and returns the results. This is used to
        update the object's header values. Note that all headers must
        be sent with the POST, unlike the account and container POSTs.
        With account and container POSTs, existing headers are
        untouched. But with object POSTs, any existing headers are
        removed. The full list of supported headers depends on the
        Swift cluster, but usually include Content-Type,
        Content-Encoding, and any X-Object-Meta-xxx headers.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: No known Swift POSTs take a body; but the option
            is there for the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self.request(
            'POST', path, body or '', headers, query=query, cdn=cdn)

    def delete_object(self, container, obj, headers=None, query=None,
                      cdn=False, body=None):
        """
        DELETEs the object and returns the results.

        :param container: The name of the container.
        :param obj: The name of the object.
        :param headers: Additional headers to send with the request.
        :param query: Set to a dict of query values to send on the
            query string of the request.
        :param cdn: If set True, the CDN management interface will be
            used.
        :param body: Some object DELETE requests might take a body in
            the future.
        :returns: A tuple of (status, reason, headers, contents).

            :status: is an int for the HTTP status code.
            :reason: is the str for the HTTP status (ex: "Ok").
            :headers: is a dict with all lowercase keys of the HTTP
                headers; if a header has multiple values, it will be a
                list.
            :contents: is the str for the HTTP body.
        """
        path = self._object_path(container, obj)
        return self.request(
            'DELETE', path, body or '', headers, query=query, cdn=cdn)
