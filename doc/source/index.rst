Swiftly
=======

    Copyright 2011-2013 Gregory Holt

    Currently provides a Client class and a command line tool for common Swift
    functions. Works both with access as a standard external user and as an
    internal administrator of a cluster with direct access to the rings and to
    all back end servers.

    Can optionally make use of Eventlet (0.11.0 or later recommended)
    <http://eventlet.net/> or PyCrypto (2.6.1 or later)
    <https://www.dlitz.net/software/pycrypto/>.

    Source code available at <http://github.com/gholt/swiftly>

    Note: If you ``sudo easy_install swiftly`` on Mac OS X, you may need to run
    ``sudo chmod -R og+r /Library/Python/2.7/site-packages`` in order to run
    swiftly.

API Reference
-------------

.. toctree::
    :maxdepth: 2

    license
    swiftly
    swiftly_cli
    swiftly_client
    swiftly_concurrency
    swiftly_dencrypt
    swiftly_filelikeiter

Overview
--------

Client Class Usage
..................

Example as a standard end user::

    from swiftly.client import Client
    client = Client('http://127.0.0.1:8080/auth/v1.0',
                    'test:tester', 'testing')
    print client.head_account()

Example as a administrator direct user::

    from swiftly.client import Client
    client = Client(swift_proxy=True,
                    swift_proxy_storage_path='/v1/AUTH_test')
    print client.head_account()


Command Line Tool Usage
.......................

help
^^^^

Output from `swiftly`::

    Usage: swiftly [options] <command> [command_options] [args]

    Options:
      -?, --help            Shows this help text.
      --version             Shows the version of Swiftly.
      -h                    Shows this help text.
      -A URL, --auth-url=URL
                            URL to auth system, example:
                            http://127.0.0.1:8080/auth/v1.0 You can also set this
                            with the environment variable SWIFTLY_AUTH_URL.
      -U USER, --auth-user=USER
                            User name for auth system, example: test:tester You
                            can also set this with the environment variable
                            SWIFTLY_AUTH_USER.
      -K KEY, --auth-key=KEY
                            Key for auth system, example: testing You can also set
                            this with the environment variable SWIFTLY_AUTH_KEY.
      -T TENANT, --auth-tenant=TENANT
                            Tenant name for auth system, example: test You can
                            also set this with the environment variable
                            SWIFTLY_AUTH_TENANT. If not specified and needed, the
                            auth user will be used.
      --auth-methods=name[,name[...]]
                            Auth methods to use with the auth system, example: aut
                            h2key,auth2password,auth2password_force_tenant,auth1
                            You can also set this with the environment variable
                            SWIFTLY_AUTH_METHODS. Swiftly will try to determine
                            the best order for you; but if you notice it keeps
                            making useless auth attempts and that drives you
                            crazy, you can override that here. All the available
                            auth methods are listed in the example.
      --region=VALUE        Region to use, if supported by auth, example: DFW You
                            can also set this with the environment variable
                            SWIFTLY_REGION. Default: default region specified by
                            the auth response.
      -D PATH, --direct=PATH
                            Uses direct connect method to access Swift. Requires
                            access to rings and backend servers. The PATH is the
                            account path, example: /v1/AUTH_test You can also set
                            this with the environment variable SWIFTLY_DIRECT.
      -P URL, --proxy=URL   Uses the given proxy URL. You can also set this with
                            the environment variable SWIFTLY_PROXY.
      -S, --snet            Prepends the storage URL host name with "snet-".
                            Mostly only useful with Rackspace Cloud Files and
                            Rackspace ServiceNet. You can also set this with the
                            environment variable SWIFTLY_SNET (set to "true" or
                            "false").
      -R INTEGER, --retries=INTEGER
                            Indicates how many times to retry the request on a
                            server error. Default: 4. You can also set this with
                            the environment variable SWIFTLY_RETRIES.
      -C, --cache-auth      If set true, the storage URL and auth token are cached
                            in your OS temporary directory as <user>.swiftly for
                            reuse. If there are already cached values, they are
                            used without authenticating first. You can also set
                            this with the environment variable SWIFTLY_CACHE_AUTH
                            (set to "true" or "false").
      --cdn                 Directs requests to the CDN management interface.
      --concurrency=INTEGER
                            Sets the the number of actions that can be done
                            simultaneously when possible (currently requires using
                            Eventlet too). Default: 1. You can also set this with
                            the environment variable SWIFTLY_CONCURRENCY. Note
                            that some nested actions may amplify the number of
                            concurrent actions. For instance, a put of an entire
                            directory will use up to this number of concurrent
                            actions. A put of a segmented object will use up to
                            this number of concurrent actions. But, if a directory
                            structure put is uploading segmented objects, this
                            nesting could cause up to INTEGER * INTEGER concurrent
                            actions.
      --eventlet            Enables Eventlet, if installed. This is disabled by
                            default if Eventlet is not installed or is less than
                            version 0.11.0 (because older Swiftly+Eventlet tends
                            to use excessive CPU.
      --no-eventlet         Disables Eventlet, even if installed and version
                            0.11.0 or greater.
      -v, --verbose         Causes output to standard error indicating actions
                            being taken. These output lines will be prefixed with
                            VERBOSE and will also include the number of seconds
                            elapsed since Swiftly started.
    Commands:
      auth                  Authenticates and then outputs the resulting
                            information.
      decrypt [key]         Decrypts standard input using the given [key] and sends
                            that to standard output. If the key is not provided on
                            the command line or is a single dash "-", it must be
                            provided via a SWIFTLY_CRYPT_KEY environment variable.
      delete [options] [path]
                            Issues a DELETE request of the [path] given.
      encrypt [key]         Encrypts standard input using the given [key] and sends
                            that to standard output. If the key is not provided on
                            the command line or is a single dash "-", it must be
                            provided via a SWIFTLY_CRYPT_KEY environment variable.
      for [options] <path> do [command]
                            This will issue the [command] for each item encountered
                            performing a listing on <path>.
      get [options] [path]  Outputs the resulting contents from a GET request of
                            the [path] given. If no [path] is given, a GET request
                            on the account is performed.
      head [options] [path] Outputs the resulting headers from a HEAD request of
                            the [path] given. If no [path] is given, a HEAD request
                            on the account is performed.
      help [command]        Outputs help information for the given [command] or
                            general help if no [command] is given.
      ping [options] [path] Runs a ping test against the account. The [path] will
                            be used as a prefix to the random container name used
                            (default: swiftly-ping).
      post [options] [path] Issues a POST request of the [path] given. If no [path]
                            is given, a POST request on the account is performed.
      put [options] [path]  Performs a PUT request on the <path> given. If the
                            <path> is an object, the contents for the object are
                            read from standard input.
      tempurl <method> <path> [seconds]
                            Outputs a TempURL using the information given. The
                            <path> should be to an object or object-prefix.
                            [seconds] defaults to 3600
      trans <x-trans-id-value>
                            Outputs information about the <x-trans-id-value> given,
                            such as the embedded transaction time.


auth
^^^^

Output from `swiftly help auth`::

    Usage: swiftly [main_options] auth

    For help on [main_options] run swiftly with no args.

    Authenticates and then outputs the resulting information.

    Possible Output Values:

        Auth Cache           The location where auth info may be cached.
        Auth URL             The URL of the auth service if in use.
        Auth User            The user to auth as if in use.
        Auth Key             The key to auth with if in use.
        Auth Tenant          The tenant to auth as if in use.
        Auth Methods         The auth methods in use if any specified.
        Direct Storage Path  The direct-mode path if in use.
        Regions              The available regions as reported by the auth service.
        Default Region       The default region as reported by the auth service.
        Selected Region      The region selected for use by Swiftly.
        SNet                 True if ServiceNet/InternalURL would be used.
        Storage URL          The URL to use for storage as reported by the auth
                             service.
        CDN Management URL   The URL to use for CDN management as reported by the
                             auth service.
        Auth Token           The auth token to use as reported by the auth service.
        Request Retries      The number retries to be done for any request.

    Example Output:

    Auth Cache:         /tmp/user.swiftly
    Auth URL:           https://identity.api.rackspacecloud.com/v2.0
    Auth User:          myusername
    Auth Key:           mykey
    Regions:            ORD DFW SYD IAD HKG
    Default Region:     ORD
    Selected Region:    IAD
    SNet:               True
    Storage URL:        https://snet-storage101.iad3.clouddrive.com/v1/account
    CDN Management URL: https://cdn5.clouddrive.com/v1/account
    Auth Token:         abcdef0123456789abcdef0123456789
    Request Retries:    4

    Options:
      -?, --help  Shows this help text.


decrypt
^^^^^^^

Output from `swiftly help decrypt`::

    Usage: swiftly [main_options] decrypt [key]

    For help on [main_options] run swiftly with no args.

    Decrypts standard input using the given [key] and sends that to standard
    output. If the key is not provided on the command line or is a single dash "-",
    it must be provided via a SWIFTLY_CRYPT_KEY environment variable.

    This currently only supports AES 256 in CBC mode but other algorithms may be
    offered in the future.

    Options:
      -?, --help  Shows this help text.


delete
^^^^^^

Output from `swiftly help delete`::

    Usage: swiftly [main_options] delete [options] [path]

    For help on [main_options] run swiftly with no args.

    Issues a DELETE request of the [path] given.

    Options:
      -?, --help            Shows this help text.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-some-header
                            :some-value -h "X-Some-Other-Header: Some other value"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
      -i PATH, --input=PATH
                            Indicates where to read the DELETE request body from;
                            use a dash (as in "-i -") to specify standard input
                            since DELETEs do not normally take input.
      --recursive           Normally a delete for a non-empty container will error
                            with a 409 Conflict; --recursive will first delete all
                            objects in a container and then delete the container
                            itself. For an account delete, all containers and
                            objects will be deleted (requires the --yes-i-mean-
                            empty-the-account option).
      --yes-i-mean-empty-the-account
                            Required when issuing a delete directly on an account
                            with the --recursive option. This will delete all
                            containers and objects in the account without deleting
                            the account itself, leaving an empty account. THERE IS
                            NO GOING BACK!
      --yes-i-mean-delete-the-account
                            Required when issuing a delete directly on an account.
                            Some Swift clusters do not support this. Those that do
                            will mark the account as deleted and immediately begin
                            removing the objects from the cluster in the
                            backgound. THERE IS NO GOING BACK!
      --ignore-404          Ignores 404 Not Found responses; the exit code will be
                            0 instead of 1.


encrypt
^^^^^^^

Output from `swiftly help encrypt`::

    Usage: swiftly [main_options] encrypt [key]

    For help on [main_options] run swiftly with no args.

    Encrypts standard input using the given [key] and sends that to standard
    output. If the key is not provided on the command line or is a single dash "-",
    it must be provided via a SWIFTLY_CRYPT_KEY environment variable.

    This currently uses AES 256 in CBC mode but other algorithms may be offered in
    the future.

    Options:
      -?, --help  Shows this help text.


for
^^^

Output from `swiftly help for`::

    Usage: swiftly [main_options] for [options] <path> do [command]

    For help on [main_options] run swiftly with no args.

    This will issue the [command] for each item encountered performing a listing on
    <path>.

    If the <path> is an empty string "" then an account listing is performed and
    the [command] will be run for each container listed. Otherwise, the <path> must
    be a container and the [command] will be run for each object listed.

    You may include the options listed below before the "do" to change how the
    listing is performed (prefix queries, limits, etc.)

    The "do" keyword separates the [command] from the rest of the "for" expression.
    After the "do" comes the [command] which will have any instances of "<item>"
    replaced with each item in the resulting "for" listing.

    For example, to head every container for an account:

        swiftly for "" do head "<item>"

    To post to every object in a container, forcing an auto-detected update to
    each's content-type:

        swiftly for my_container do post -hx-detect-content-type:true "<item>"

    To head every object in a container, but only those with the name prefix of
    "under_here/":

        swiftly for -p under_here/ my_container do head "<item>"

    Options:
      -?, --help            Shows this help text.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hif-
                            match:6f432df40167a4af05ca593acc6b3e4c -h "If-
                            Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
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
      -e MARKER, --end-marker=MARKER
                            For account and container GETs, this sets the end-
                            marker for the listing retrieved; the items returned
                            will stop with the item just before the MARKER given
                            (note: the marker does not have to actually exist).
      --ignore-404          Ignores 404 Not Found responses. Nothing will be
                            output, and the exit code will be 0 instead of 1.
      --output-names        Outputs the name of each item just before calling
                            [command] with it. To ensure easier parsing, the name
                            will be url encoded and prefixed with "Item Name: ".
                            For commands that have output of their own, this is
                            usually only useful with single concurrency; otherwise
                            the item names and the command output will get
                            interspersed and impossible to associate.


get
^^^

Output from `swiftly help get`::

    Usage: swiftly [main_options] get [options] [path]

    For help on [main_options] run swiftly with no args.

    Outputs the resulting contents from a GET request of the [path] given. If no
    [path] is given, a GET request on the account is performed.

    Options:
      -?, --help            Shows this help text.
      --headers             Output headers as well as the contents.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hif-
                            match:6f432df40167a4af05ca593acc6b3e4c -h "If-
                            Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
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
      -e MARKER, --end-marker=MARKER
                            For account and container GETs, this sets the end-
                            marker for the listing retrieved; the items returned
                            will stop with the item just before the MARKER given
                            (note: the marker does not have to actually exist).
      -f, --full            For account and container GETs, this will output
                            additional information about each item. For an account
                            GET, the items output will be bytes-used, object-
                            count, and container-name. For a container GET, the
                            items output will be bytes-used, last-modified-time,
                            etag, content-type, and object-name. Note that this is
                            mostly useless for --cdn queries; for those it is best
                            to just use --raw and parse the results yourself
                            (perhaps through "python -m json.tool").
      -r, --raw             For account and container GETs, this will return the
                            raw JSON from the request. This will only do one
                            request, even if subsequent requests would be needed
                            to return all items. Use a subsequent call with
                            --marker set to the last item name returned to get the
                            next batch of items, if desired.
      --all-objects         For an account GET, performs a container GET --all-
                            objects for every container returned by the original
                            account GET. For a container GET, performs a GET for
                            every object returned by that original container GET.
                            Any headers set with --header options are sent for
                            every GET. Any query parameter set with --query is
                            sent for every GET.
      -o PATH, --output=PATH
                            Indicates where to send the output; default is
                            standard output. If the PATH ends with a slash "/" and
                            --all-objects is used, each object will be placed in a
                            similarly named file inside the PATH given.
      --ignore-404          Ignores 404 Not Found responses. Nothing will be
                            output, but the exit code will be 0 instead of 1.
      --sub-command=COMMAND
                            Sends the contents of each object downloaded as
                            standard input to the COMMAND given and outputs the
                            command's standard output as if it were the object's
                            contents. This can be useful in combination with
                            --all-objects to filter the objects before writing
                            them to disk; for instance, downloading logs,
                            gunzipping them, grepping for a keyword, and only
                            storing matching lines locally (--sub-command "gunzip
                            | grep keyword" or --sub-command "zgrep keyword" if
                            your system has that).
      --remove-empty-files  Removes files that result as empty. This can be useful
                            in conjunction with --sub-command so you are left only
                            with the files that generated output.
      --decrypt=KEY         Will decrypt the downloaded object data with KEY. This
                            currently only supports AES 256 in CBC mode but other
                            algorithms may be offered in the future. You may
                            specify a single dash "-" as the KEY and instead the
                            KEY will be loaded from the SWIFTLY_CRYPT_KEY
                            environment variable.


head
^^^^

Output from `swiftly help head`::

    Usage: swiftly [main_options] head [options] [path]

    For help on [main_options] run swiftly with no args.

    Outputs the resulting headers from a HEAD request of the [path] given. If no
    [path] is given, a HEAD request on the account is performed.

    Options:
      -?, --help            Shows this help text.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hif-
                            match:6f432df40167a4af05ca593acc6b3e4c -h "If-
                            Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
      --ignore-404          Ignores 404 Not Found responses. Nothing will be
                            output, but the exit code will be 0 instead of 1.


ping
^^^^

Output from `swiftly help ping`::

    Usage: swiftly [main_options] ping [options] [path]

    For help on [main_options] run swiftly with no args.

    Runs a ping test against the account.
    The [path] will be used as a prefix to the random container name used (default:
    swiftly-ping).

    Options:
      -?, --help            Shows this help text.
      -v, --verbose         Outputs additional information as ping works.
      -c PING_COUNT, --count=PING_COUNT
                            Count of objects to create; default 1.
      -o OBJECT_RING, --object-ring=OBJECT_RING
                            The current object ring of the cluster being pinged.
                            This will enable output of which nodes are involved in
                            the object requests and their implied behavior. Use of
                            this also requires the main Swift code is installed
                            and importable.
      -l LIMIT, --limit=LIMIT
                            Limits the node output tables to LIMIT nodes.
      -t THRESHOLD, --threshold=THRESHOLD
                            Changes the threshold for the final (average * x)
                            reports. This will define the value of x, defaults to
                            2.


post
^^^^

Output from `swiftly help post`::

    Usage: swiftly [main_options] post [options] [path]

    For help on [main_options] run swiftly with no args.

    Issues a POST request of the [path] given. If no [path] is given, a POST
    request on the account is performed.

    Options:
      -?, --help            Shows this help text.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-object-meta-
                            color:blue -h "Content-Type: text/html"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
      -i PATH, --input=PATH
                            Indicates where to read the POST request body from;
                            use a dash (as in "-i -") to specify standard input
                            since POSTs to Swift do not normally take input.


put
^^^

Output from `swiftly help put`::

    Usage: swiftly [main_options] put [options] [path]

    For help on [main_options] run swiftly with no args.

    Performs a PUT request on the <path> given. If the <path> is an object, the
    contents for the object are read from standard input.

    Special Note About Segmented Objects:

    For object uploads exceeding the -s [size] (default: 5G) the object will be
    uploaded in segments. At this time, auto-segmenting only works for objects
    uploaded from source files -- objects sourced from standard input cannot exceed
    the maximum object size for the cluster.

    A segmented object is one that has its contents in several other objects. On
    download, these other objects are concatenated into a single object stream.

    Segmented objects can be useful to greatly exceed the maximum single object
    size, speed up uploading large objects with concurrent segment uploading, and
    provide the option to replace, insert, and delete segments within a whole
    object without having to alter or reupload any of the other segments.

    The main object of a segmented object is called the "manifest object". This
    object just has an X-Object-Manifest header that points to another path where
    the segments for the object contents are stored. For Swiftly, this header value
    is auto-generated as the same name as the manifest object, but with "_segments"
    added to the container name. This keeps the segments out of the main container
    listing, which is often useful.

    By default, Swift's dynamic large object support is used since it was
    implemented first. However, if you prefix the [size] with an 's', as in '-s
    s1048576' Swiftly will use static large object support. These static large
    objects are very similar as described above, except the manifest contains a
    static list of the object segments. For more information on the tradeoffs, see
    http://greg.brim.net/post/2013/05/16/1834.html

    Options:
      -?, --help            Shows this help text.
      -h HEADER:VALUE, -H HEADER:VALUE, --header=HEADER:VALUE
                            Add a header to the request. This can be used multiple
                            times for multiple headers. Examples: -hx-object-meta-
                            color:blue -h "Content-Type: text/html"
      -q NAME[=VALUE], --query=NAME[=VALUE]
                            Add a query parameter to the request. This can be used
                            multiple times for multiple query parameters. Example:
                            -qmultipart-manifest=get
      -i PATH, --input=PATH
                            Indicates where to read the contents from; default is
                            standard input. If the PATH is a directory, all files
                            in the directory will be uploaded as similarly named
                            objects and empty directories will create
                            text/directory marker objects. Use a dash (as in "-i
                            -") to specify standard input for account and
                            container PUTs, as those do not normally take input.
                            This is useful with -qextract-archive=<format> bulk
                            upload requests. For example: tar zc . | swiftly put
                            -qextract-archive=tar.gz -i - container
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
      -s BYTES, --segment-size=BYTES
                            Indicates the maximum size of an object before
                            uploading it as a segmented object. See full help text
                            for more information.
      --encrypt=KEY         Will encrypt the uploaded object data with KEY. This
                            currently uses AES 256 in CBC mode but other
                            algorithms may be offered in the future. You may
                            specify a single dash "-" as the KEY and instead the
                            KEY will be loaded from the SWIFTLY_CRYPT_KEY
                            environment variable.


tempurl
^^^^^^^

Output from `swiftly help tempurl`::

    Usage: swiftly [main_options] tempurl <method> <path> [seconds]

    For help on [main_options] run swiftly with no args.

    Outputs a TempURL using the information given.
    The <path> should be to an object or object-prefix.
    [seconds] defaults to 3600

    Options:
      -?, --help  Shows this help text.


trans
^^^^^

Output from `swiftly help trans`::

    Usage: swiftly [main_options] trans <x-trans-id-value>

    For help on [main_options] run swiftly with no args.

    Outputs information about the <x-trans-id-value> given, such as the embedded
    transaction time.

    Options:
      -?, --help  Shows this help text.


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
