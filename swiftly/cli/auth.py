"""
Contains a CLICommand that authenticates and then outputs the
resulting information.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

==============  ========================
client_manager  For connecting to Swift.
io_manager      For directing output.
==============  ========================
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

import swiftly.cli.command


def cli_auth(context):
    """
    Authenticates and then outputs the resulting information.

    See :py:mod:`swiftly.cli.auth` for context usage information.

    See :py:class:`CLIAuth` for more information.
    """
    with contextlib.nested(
            context.io_manager.with_stdout(),
            context.client_manager.with_client()) as (fp, client):
        info = []
        if client.cache_path:
            info.append(('Auth Cache', client.cache_path))
        if client.auth_url:
            info.append(('Auth URL', client.auth_url))
        if client.auth_user:
            info.append(('Auth User', client.auth_user))
        if client.auth_key:
            info.append(('Auth Key', client.auth_key))
        if client.auth_tenant:
            info.append(('Auth Tenant', client.auth_tenant))
        if client.auth_methods:
            info.append(('Auth Methods', client.auth_methods))
        if client.swift_proxy:
            info.append(('Direct Storage Path', client.storage_path))
        else:
            client.auth()
            if client.regions:
                info.append(('Regions', ' '.join(client.regions)))
            if client.regions_default:
                info.append(('Default Region', client.regions_default))
            if client.region:
                info.append(('Selected Region', client.region))
            if client.snet:
                info.append(('SNet', 'True'))
            info.append(('Storage URL', client.storage_url))
            if client.cdn_url:
                info.append(('CDN Management URL', client.cdn_url))
            info.append(('Auth Token', client.auth_token))
        info.append(('Request Retries', client.attempts - 1))
        fmt = '%%-%ds %%s\n' % (max(len(t) for t, v in info) + 1)
        for t, v in info:
            fp.write(fmt % (t + ':', v))
        fp.flush()


class CLIAuth(swiftly.cli.command.CLICommand):
    """
    A CLICommand that authenticates and then outputs the resulting
    information.

    See the output of ``swiftly help auth`` for more information.
    """

    def __init__(self, cli):
        super(CLIAuth, self).__init__(
            cli, 'auth', max_args=0, usage="""
Usage: %prog [main_options] auth

For help on [main_options] run %prog with no args.

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
            """.strip())

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        return cli_auth(context)
