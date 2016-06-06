"""
Contains a CLICommand that can issue DELETE requests.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

==============  =====================================================
cdn             True if the CDN Management URL should be used instead
                of the Storage URL.
client_manager  For connecting to Swift.
concurrency     The number of concurrent actions that can be
                performed.
headers         A dict of headers to send.
ignore_404      True if 404s should be silently ignored.
io_manager      For directing output.
query           A dict of query parameters to send.
==============  =====================================================
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
import six
from swiftly.concurrency import Concurrency
from swiftly.cli.command import CLICommand, ReturnCode


def cli_empty_account(context, yes_empty_account=False, until_empty=False):
    """
    Deletes all objects and containers in the account.

    You must set yes_empty_account to True to verify you really want to
    do this.

    By default, this will perform one pass at deleting all objects and
    containers; so if objects revert to previous versions or if new
    objects or containers otherwise arise during the process, the
    account may not be empty once done.

    Set `until_empty` to True if you want multiple passes to keep trying
    to fully empty and delete the containers. Note until_empty=True
    could run forever if something else is making new items faster than
    they're being deleted.

    See :py:mod:`swiftly.cli.delete` for context usage information.

    See :py:class:`CLIDelete` for more information.
    """
    if not yes_empty_account:
        raise ReturnCode(
            'called cli_empty_account without setting yes_empty_account=True')
    marker = None
    while True:
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_account(
                marker=marker, headers=context.headers, query=context.query,
                cdn=context.cdn)
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            raise ReturnCode('listing account: %s %s' % (status, reason))
        if not contents:
            if until_empty and marker:
                marker = None
                continue
            break
        for item in contents:
            cli_delete(
                context, item['name'], context.headers, recursive=True)
        marker = item['name']


def cli_empty_container(context, path, until_empty=False):
    """
    Deletes all objects in the container.

    By default, this will perform one pass at deleting all objects in
    the container; so if objects revert to previous versions or if new
    objects otherwise arise during the process, the container may not be
    empty once done.

    Set `until_empty` to True if you want multiple passes to keep trying
    to fully empty the container. Note until_empty=True could run
    forever if something else is making new objects faster than they're
    being deleted.

    See :py:mod:`swiftly.cli.delete` for context usage information.

    See :py:class:`CLIDelete` for more information.
    """
    path = path.rstrip('/').decode('utf8')
    conc = Concurrency(context.concurrency)

    def check_conc():
        for (exc_type, exc_value, exc_tb, result) in \
                six.itervalues(conc.get_results()):
            if exc_value:
                with context.io_manager.with_stderr() as fp:
                    fp.write(str(exc_value))
                    fp.write('\n')
                    fp.flush()

    marker = None
    while True:
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.get_container(
                path, marker=marker, headers=context.headers,
                query=context.query, cdn=context.cdn)
        if status // 100 != 2:
            if status == 404 and context.ignore_404:
                return
            raise ReturnCode(
                'listing container %r: %s %s' % (path, status, reason))
        if not contents:
            if until_empty and marker:
                marker = None
                continue
            break
        for item in contents:
            newpath = '%s/%s' % (path, item['name'])
            new_context = context.copy()
            new_context.ignore_404 = True
            check_conc()
            conc.spawn(newpath, cli_delete, new_context, newpath)
        marker = item['name']
        conc.join()
        check_conc()


def cli_delete(context, path, body=None, recursive=False,
               yes_empty_account=False, yes_delete_account=False,
               until_empty=False):
    """
    Deletes the item (account, container, or object) at the path.

    See :py:mod:`swiftly.cli.delete` for context usage information.

    See :py:class:`CLIDelete` for more information.

    :param context: The :py:class:`swiftly.cli.context.CLIContext` to
        use.
    :param path: The path of the item (acount, container, or object)
        to delete.
    :param body: The body to send with the DELETE request. Bodies are
        not normally sent with DELETE requests, but this can be
        useful with bulk deletes for instance.
    :param recursive: If True and the item is an account or
        container, deletes will be issued for any containing items as
        well. This does one pass at the deletion; so if objects revert
        to previous versions or if new objects otherwise arise during
        the process, the container(s) may not be empty once done. Set
        `until_empty` to True if you want multiple passes to keep trying
        to fully empty the containers.
    :param until_empty: If True and recursive is True, this will cause
        Swiftly to keep looping through the deletes until the containers
        are completely empty. Useful if you have object versioning
        turned on or otherwise have objects that seemingly reappear
        after being deleted. It could also run forever if you have
        something that's uploading objects at a faster rate than they
        are deleted.
    :param yes_empty_account: This must be set to True for
        verification when the item is an account and recursive is
        True.
    :param yes_delete_account: This must be set to True for
        verification when the item is an account and you really wish
        a delete to be issued for the account itself.
    """
    path = path.lstrip('/') if path else ''
    if not path:
        if yes_empty_account:
            cli_empty_account(
                context, yes_empty_account=yes_empty_account,
                until_empty=until_empty)
        if yes_delete_account:
            with context.client_manager.with_client() as client:
                status, reason, headers, contents = client.delete_account(
                    headers=context.headers, query=context.query,
                    cdn=context.cdn, body=body,
                    yes_i_mean_delete_the_account=yes_delete_account)
                if status // 100 != 2:
                    if status == 404 and context.ignore_404:
                        return
                    raise ReturnCode(
                        'deleting account: %s %s' % (status, reason))
    elif '/' not in path.rstrip('/'):
        path = path.rstrip('/')
        if recursive:
            cli_empty_container(context, path, until_empty=until_empty)
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.delete_container(
                path, headers=context.headers,
                query=context.query, cdn=context.cdn, body=body)
            if status // 100 != 2:
                if status == 404 and context.ignore_404:
                    return
                raise ReturnCode(
                    'deleting container %r: %s %s' % (path, status, reason))
    else:
        with context.client_manager.with_client() as client:
            status, reason, headers, contents = client.delete_object(
                *path.split('/', 1), headers=context.headers,
                query=context.query, cdn=context.cdn, body=body)
            if status // 100 != 2:
                if status == 404 and context.ignore_404:
                    return
                raise ReturnCode(
                    'deleting object %r: %s %s' % (path, status, reason))


class CLIDelete(CLICommand):
    """
    A CLICommand that can issue DELETE requests.

    See the output of ``swiftly help delete`` for more information.
    """

    def __init__(self, cli):
        super(CLIDelete, self).__init__(
            cli, 'delete', max_args=1, usage="""
Usage: %prog [main_options] delete [options] [path]

For help on [main_options] run %prog with no args.

Issues a DELETE request of the [path] given.""".strip())
        self.option_parser.add_option(
            '-h', '-H', '--header', dest='header', action='append',
            metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hx-some-header:some-value -h "X-Some-Other-Header: Some '
                 'other value"')
        self.option_parser.add_option(
            '-q', '--query', dest='query', action='append',
            metavar='NAME[=VALUE]',
            help='Add a query parameter to the request. This can be used '
                 'multiple times for multiple query parameters. Example: '
                 '-qmultipart-manifest=get')
        self.option_parser.add_option(
            '-i', '--input', dest='input_', metavar='PATH',
            help='Indicates where to read the DELETE request body from; '
                 'use a dash (as in "-i -") to specify standard input since '
                 'DELETEs do not normally take input.')
        self.option_parser.add_option(
            '--recursive', dest='recursive', action='store_true',
            help='Normally a delete for a non-empty container will error with '
                 'a 409 Conflict; --recursive will first delete all objects '
                 'in a container and then delete the container itself. For an '
                 'account delete, all containers and objects will be deleted '
                 '(requires the --yes-i-mean-empty-the-account option). Note '
                 'that this will do just one pass at deletion, so if objects '
                 'revert to previous versions or somehow otherwise arise '
                 'after the deletion pass, the container or account may not '
                 'be full empty once done. See --until-empty for a '
                 'multiple-pass option.')
        self.option_parser.add_option(
            '--until-empty', dest='until_empty', action='store_true',
            help='If used with --recursive, multiple passes will be attempted '
                 'to empty all the containers of objects and the account of '
                 'all containers. Note that could run forever if there is '
                 'something else creating items faster than they are deleted.')
        self.option_parser.add_option(
            '--yes-i-mean-empty-the-account', dest='yes_empty_account',
            action='store_true',
            help='Required when issuing a delete directly on an account with '
                 'the --recursive option. This will delete all containers and '
                 'objects in the account without deleting the account itself, '
                 'leaving an empty account. THERE IS NO GOING BACK!')
        self.option_parser.add_option(
            '--yes-i-mean-delete-the-account', dest='yes_delete_account',
            action='store_true',
            help='Required when issuing a delete directly on an account. Some '
                 'Swift clusters do not support this. Those that do will mark '
                 'the account as deleted and immediately begin removing the '
                 'objects from the cluster in the backgound. THERE IS NO '
                 'GOING BACK!')
        self.option_parser.add_option(
            '--ignore-404', dest='ignore_404', action='store_true',
            help='Ignores 404 Not Found responses; the exit code will be 0 '
                 'instead of 1.')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        context.ignore_404 = options.ignore_404
        path = args.pop(0).lstrip('/') if args else None
        body = None
        if options.input_:
            if options.input_ == '-':
                body = self.cli.context.io_manager.get_stdin()
            else:
                body = open(options.input_, 'rb')
        recursive = options.recursive
        until_empty = options.until_empty
        yes_empty_account = options.yes_empty_account
        yes_delete_account = options.yes_delete_account
        if not path:
            if not recursive:
                if not yes_delete_account:
                    raise ReturnCode("""
A delete directly on an account requires the --yes-i-mean-delete-the-account
option as well.

Some Swift clusters do not support this.

Those that do will mark the account as deleted and immediately begin removing
the objects from the cluster in the backgound.

THERE IS NO GOING BACK!""".strip())
            else:
                if not yes_empty_account:
                    raise ReturnCode("""
A delete --recursive directly on an account requires the
--yes-i-mean-empty-the-account option as well.

All containers and objects in the account will be deleted, leaving an empty
account.

THERE IS NO GOING BACK!""".strip())
        return cli_delete(
            context, path, body=body, recursive=recursive,
            yes_empty_account=yes_empty_account,
            yes_delete_account=yes_delete_account, until_empty=until_empty)
