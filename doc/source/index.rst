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
                            http://127.0.0.1:8080/auth/v1.0
      -U AUTH_USER, --auth_user=AUTH_USER
                            User name for auth system, example: test:tester
      -K AUTH_KEY, --auth_key=AUTH_KEY
                            Key for auth system, example: testing
      -D DIRECT, --direct=DIRECT
                            Uses direct connect method to access Swift. Requires
                            access to rings and backend servers. The value is the
                            account path, example: /v1/AUTH_test
      -P PROXY, --proxy=PROXY
                            Uses the given proxy URL.
      -S, --snet            Prepends the storage URL host name with "snet-".
                            Mostly only useful with Rackspace Cloud Files and
                            Rackspace ServiceNet.
      -R RETRIES, --retries=RETRIES
                            Indicates how many times to retry the request on a
                            server error. Default: 4.
    Commands:
      get [options] [path]  Prints the resulting contents from a GET request of the
                            [path] given. If no [path] is given, a GET request on
                            the account is performed.
      head [path]           Prints the resulting headers from a HEAD request of the
                            [path] given. If no [path] is given, a HEAD request on
                            the account is performed.
      help [command]        Prints help information for the given [command] or
                            general help if no [command] is given.

Output from `swiftly help get`::

    Usage: swiftly [main_options] get [options] [path]

    For help on [main_options] run swiftly with no args.

    Prints the resulting contents from a GET request of the [path] given. If no
    [path] is given, a GET request on the account is performed.

    Options:
      --headers  Output headers as well as the contents.

Output from `swiftly help head`::

    Usage: swiftly [main_options] head [path]

    For help on [main_options] run swiftly with no args.

    Prints the resulting headers from a HEAD request of the [path] given. If no
    [path] is given, a HEAD request on the account is performed.


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
