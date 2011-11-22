Swiftly - A client for Swift

A work in progress. Currently provides a Client class for common Swift
functions. Works both with access as an external, end user to a Swift
cluster and as an internal administrator of a cluster with direct
access to the rings and to all back end servers.

Example as a standard end user::

    from swiftly import Client
    client = Client('http://127.0.0.1:8080/auth/v1.0',
                    'test:tester', 'testing')
    print client.head_account()

Example as a administrator direct user:

    from swiftly import Client
    client = Client(swift_proxy=True,
                    swift_proxy_storage_path='/v1/AUTH_test')
    print client.head_account()
