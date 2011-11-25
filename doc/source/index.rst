Swiftly
=======

    Copyright 2011 Gregory Holt

    A work in progress. Currently provides a Client class and a command line
    tool for common Swift functions. Works both with access as an external, end
    user to a Swift cluster and as an internal administrator of a cluster with
    direct access to the rings and to all back end servers.

Contents
--------

.. toctree::
    :maxdepth: 2

    license
    swiftly
    swiftly_cli
    swiftly_client

Overview
--------

Client Class Usage
..................

Example as a standard end user::

    from swiftly import Client
    client = Client('http://127.0.0.1:8080/auth/v1.0',
                    'test:tester', 'testing')
    print client.head_account()

Example as a administrator direct user::

    from swiftly import Client
    client = Client(swift_proxy=True,
                    swift_proxy_storage_path='/v1/AUTH_test')
    print client.head_account()


Command Line Tool Usage
.......................

Output from `swiftly`::

    Usage: swiftly [options] <command> [command_options] [args]

    Options:
      -A AUTH_URL, --auth_url=AUTH_URL
                            URL to auth system, example:
                            http://127.0.0.1:8080/auth/v1.0 You can also set this
                            with the environment variable SWIFTLY_AUTH_URL.
      -U AUTH_USER, --auth_user=AUTH_USER
                            User name for auth system, example: test:tester You
                            can also set this with the environment variable
                            SWIFTLY_AUTH_USER.
      -K AUTH_KEY, --auth_key=AUTH_KEY
                            Key for auth system, example: testing You can also set
                            this with the environment variable SWIFTLY_AUTH_KEY.
      -D DIRECT, --direct=DIRECT
                            Uses direct connect method to access Swift. Requires
                            access to rings and backend servers. The value is the
                            account path, example: /v1/AUTH_test You can also set
                            this with the environment variable SWIFTLY_DIRECT.
      -P PROXY, --proxy=PROXY
                            Uses the given proxy URL. You can also set this with
                            the environment variable SWIFTLY_PROXY.
      -S, --snet            Prepends the storage URL host name with "snet-".
                            Mostly only useful with Rackspace Cloud Files and
                            Rackspace ServiceNet. You can also set this with the
                            environment variable SWIFTLY_SNET (set to "true" or
                            "false").
      -R RETRIES, --retries=RETRIES
                            Indicates how many times to retry the request on a
                            server error. Default: 4. You can also set this with
                            the environment variable SWIFTLY_RETRIES.
      -C, --cache-auth      If set true, the storage URL and auth token are cached
                            in /tmp/<user>.swiftly for reuse. If there are already
                            cached values, they are used without authenticating
                            first. You can also set this with the environment
                            variable SWIFTLY_CACHE_AUTH (set to "true" or
                            "false").
    Commands:
      auth                  Outputs auth information.
      delete [options] <path>
                            Issues a DELETE request of the <path> given.
      get [options] [path]  Outputs the resulting contents from a GET request of
                            the [path] given. If no [path] is given, a GET request
                            on the account is performed.
      head [options] [path] Outputs the resulting headers from a HEAD request of
                            the [path] given. If no [path] is given, a HEAD request
                            on the account is performed.
      help [command]        Outputs help information for the given [command] or
                            general help if no [command] is given.
      post [options] [path] Issues a POST request of the [path] given. If no [path]
                            is given, a POST request on the account is performed.
      put [options] <path>  Performs a PUT request on the <path> given. If the
                            <path> is an object, the contents for the object are
                            read from standard input.


Output from `swiftly help auth`::

    Usage: swiftly [main_options] auth

    For help on [main_options] run swiftly with no args.

    Outputs auth information.


Output from `swiftly help delete`::

    Usage: swiftly [main_options] delete [options] <path>

    For help on [main_options] run swiftly with no args.

    Issues a DELETE request of the <path> given.

    Options:
      -h HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-some-header
                            :some-value -h "X-Some-Other-Header: Some other value"


Output from `swiftly help get`::

    Usage: swiftly [main_options] get [options] [path]

    For help on [main_options] run swiftly with no args.

    Outputs the resulting contents from a GET request of the [path] given. If no
    [path] is given, a GET request on the account is performed.

    Options:
      --headers             Output headers as well as the contents.
      -h HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hif-
                            match:6f432df40167a4af05ca593acc6b3e4c -h "If-
                            Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"
      -l LIMIT, --limit=LIMIT
                            For account and container GETs, this limits the number
                            of items returned. Without this option, all items are
                            returned, even if it requires several backend requests
                            to the gather the information.
      -d DELIMITER, --delimiter=DELIMITER
                            For account and container GETs, this sets the
                            delimiter for the listing retrieved. For example, a
                            container with the objects "abc/one", "abc/two", "xyz"
                            and a delimiter of "/" would return "abc/" and "xyz".
                            Using the same delimiter, but with a prefix of "abc/",
                            would return "abc/one" and "abc/two".
      -p PREFIX, --prefix=PREFIX
                            For account and container GETs, this sets the prefix
                            for the listing retrieved; the items returned will all
                            match the PREFIX given.
      -m MARKER, --marker=MARKER
                            For account and container GETs, this sets the marker
                            for the listing retrieved; the items returned will
                            begin with the item just after the MARKER given (note:
                            the marker does not have to actually exist).
      -e MARKER, --end_marker=MARKER
                            For account and container GETs, this sets the
                            end_marker for the listing retrieved; the items
                            returned will stop with the item just before the
                            MARKER given (note: the marker does not have to
                            actually exist).
      -f, --full            For account and container GETs, this will output
                            additional information about each item. For an account
                            GET, the items output will be bytes-used, object-
                            count, and container-name. For a container GET, the
                            items output will be bytes-used, last-modified-time,
                            etag, content-type, and object-name.
      -r, --raw             For account and container GETs, this will return the
                            raw JSON from the request. This will only do one
                            request, even if subsequent requests would be needed
                            to return all items. Use a subsequent call with
                            --marker set to the last item name returned to get the
                            next batch of items, if desired.
      --all-objects         For a container GET, performs a GET for every object
                            returned by the original container GET. Any headers
                            set with --header options are also sent for every
                            object GET.
      -o PATH, --output=PATH
                            Indicates where to send the output; default is
                            standard output. If the PATH ends with a slash "/" and
                            --all-objects is used, each object will be placed in a
                            similarly named file inside the PATH given.


Output from `swiftly help head`::

    Usage: swiftly [main_options] head [options] [path]

    For help on [main_options] run swiftly with no args.

    Outputs the resulting headers from a HEAD request of the [path] given. If no
    [path] is given, a HEAD request on the account is performed.

    Options:
      -h HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hif-
                            match:6f432df40167a4af05ca593acc6b3e4c -h "If-
                            Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"


Output from `swiftly help post`::

    Usage: swiftly [main_options] post [options] [path]

    For help on [main_options] run swiftly with no args.

    Issues a POST request of the [path] given. If no [path] is given, a POST
    request on the account is performed.

    Options:
      -h HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-object-meta-
                            color:blue -h "Content-Type: text/html"


Output from `swiftly help put`::

    Usage: swiftly [main_options] put [options] <path>

    For help on [main_options] run swiftly with no args.

    Performs a PUT request on the <path> given. If the <path> is an object, the
    contents for the object are read from standard input.

    Options:
      -h HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-object-meta-
                            color:blue -h "Content-Type: text/html"
      -i PATH, --input=PATH
                            Indicates where to read the contents from; default is
                            standard input. If the PATH is a directory, all files
                            in the directory will be uploaded as similarly named
                            objects and empty directories will create
                            text/directory marker objects.
      -n, --newer           For PUTs with an --input option, first performs a HEAD
                            on the object and compares the X-Object-Meta-Mtime
                            header with the modified time of the PATH obtained
                            from the --input option and then PUTs the object only
                            if the local time is newer. When the --input PATH is a
                            directory, this offers an easy way to upload only the
                            newer files since the last upload (at the expense of
                            HEAD requests). NOTE THAT THIS WILL NOT UPLOAD CHANGED
                            FILES THAT DO NOT HAVE A NEWER LOCAL MODIFIED TIME!
                            NEWER does not mean DIFFERENT.
      -d, --different       For PUTs with an --input option, first performs a HEAD
                            on the object and compares the X-Object-Meta-Mtime
                            header with the modified time of the PATH obtained
                            from the --input option and then PUTs the object only
                            if the local time is different. It will also check the
                            local and remote sizes and PUT if they differ.
                            ETag/MD5sum checking are not done (an option may be
                            provided in the future) since this is usually much
                            more disk intensive. When the --input PATH is a
                            directory, this offers an easy way to upload only the
                            differing files since the last upload (at the expense
                            of HEAD requests). NOTE THAT THIS CAN UPLOAD OLDER
                            FILES OVER NEWER ONES! DIFFERENT does not mean NEWER.
      -e, --empty           Indicates a zero-byte object should be PUT.


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
