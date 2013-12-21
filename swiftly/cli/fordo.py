"""
Contains a CLICommand that can issue other commands for each item in
an account or container listing.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

=======================  ============================================
cdn                      True if the CDN Management URL should be
                         used instead of the Storage URL.
client_manager           For connecting to Swift.
concurrency              The number of concurrent actions that can be
                         performed.
headers                  A dict of headers to send.
ignore_404               True if 404s should be silently ignored.
io_manager               For directing output.
query                    A dict of query parameters to send. Of
                         important use are limit, delimiter, prefix,
                         marker, and end_marker as they are common
                         listing query parameters.
remaining_args           The list of command line args to issue to
                         the sub-CLI instance; the first arg that
                         equals '<item>' will be replaced with each
                         item the for encounters. Any additional
                         instances of '<item>' will be left alone, as
                         you might be calling a nested "for ... do".
original_main_args       Used when constructing sub-CLI instances.
output_names             If True, outputs the name of each item just
                         before calling [command] with it. To ensure
                         easier parsing, the name will be url encoded
                         and prefixed with "Item Name: ". For
                         commands that have output of their own, this
                         is usually only useful with single
                         concurrency; otherwise the item names and
                         the command output will get interspersed and
                         impossible to associate.
=======================  ============================================
"""
"""
Copyright 2013 Gregory Holt

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
import urllib

from swiftly.cli.cli import CLI
from swiftly.cli.command import CLICommand, ReturnCode
from swiftly.concurrency import Concurrency


def _cli_call(context, name, args):
    if context.output_names:
        with context.io_manager.with_stdout() as fp:
            fp.write('Item Name: ')
            fp.write(urllib.quote(name.encode('utf8')))
            fp.write('\n')
            fp.flush()
    return CLI()(context.original_main_args + args)


def cli_fordo(context, path=None):
    """
    Issues commands for each item in an account or container listing.

    See :py:mod:`swiftly.cli.fordo` for context usage information.

    See :py:class:`CLIForDo` for more information.
    """
    path = path.lstrip('/') if path else None
    if path and '/' in path:
        raise ReturnCode(
            'path must be an empty string or a container name; was %r' % path)
    limit = context.query.get('limit')
    delimiter = context.query.get('delimiter')
    prefix = context.query.get('prefix')
    marker = context.query.get('marker')
    end_marker = context.query.get('end_marker')
    conc = Concurrency(context.concurrency)
    while True:
        with context.client_manager.with_client() as client:
            if not path:
                status, reason, headers, contents = client.get_account(
                    headers=context.headers, prefix=prefix,
                    delimiter=delimiter, marker=marker, end_marker=end_marker,
                    limit=limit, query=context.query, cdn=context.cdn)
            else:
                status, reason, headers, contents = client.get_container(
                    path, headers=context.headers, prefix=prefix,
                    delimiter=delimiter, marker=marker, end_marker=end_marker,
                    limit=limit, query=context.query, cdn=context.cdn)
            if status // 100 != 2:
                if status == 404 and context.ignore_404:
                    return
                if hasattr(contents, 'read'):
                    contents.read()
                if not path:
                    raise ReturnCode(
                        'listing account: %s %s' % (status, reason))
                else:
                    raise ReturnCode(
                        'listing container %r: %s %s' % (path, status, reason))
        if not contents:
            break
        for item in contents:
            name = (path + '/' if path else '') + item.get(
                'name', item.get('subdir'))
            args = list(context.remaining_args)
            try:
                index = args.index('<item>')
            except ValueError:
                raise ReturnCode(
                    'No "<item>" designation found in the "do" clause.')
            args[index] = name
            for (exc_type, exc_value, exc_tb, result) in \
                    conc.get_results().itervalues():
                if exc_value:
                    conc.join()
                    raise exc_value
            conc.spawn(name, _cli_call, context, name, args)
        marker = contents[-1]['name']
        if limit:
            break
    conc.join()
    for (exc_type, exc_value, exc_tb, result) in \
            conc.get_results().itervalues():
        if exc_value:
            conc.join()
            raise exc_value


class CLIForDo(CLICommand):
    """
    A CLICommand that can issue other commands for each item in an
    account or container listing.

    See the output of ``swiftly help for`` for more information.
    """

    def __init__(self, cli):
        super(CLIForDo, self).__init__(
            cli, 'fordo', min_args=1, max_args=1, usage="""
Usage: %prog [main_options] for [options] <path> do [command]

For help on [main_options] run %prog with no args.

This will issue the [command] for each item encountered performing a listing on
<path>.

If the <path> is an empty string "" then an account listing is performed and
the [command] will be run for each container listed. Otherwise, the <path> must
be a container and the [command] will be run for each object listed.

You may include the options listed below before the "do" to change how the
listing is performed (prefix queries, limits, etc.)

The "do" keyword separates the [command] from the rest of the "for" expression.
After the "do" comes the [command] which will have the first instance of
"<item>" replaced with each item in turn that is in the resulting "for"
listing. Any additional instances of "<item>" will be left alone, as you might
be calling a nested "for ... do".

For example, to head every container for an account:

    %prog for "" do head "<item>"

To head every object in every container for an account:

    %prog for "" do for "<item>" do head "<item>"

To post to every object in a container, forcing an auto-detected update to
each's content-type:

    %prog for my_container do post -hx-detect-content-type:true "<item>"

To head every object in a container, but only those with the name prefix of
"under_here/":

    %prog for -p under_here/ my_container do head "<item>" """.strip())
        self.option_parser.add_option(
            '-h', '-H', '--header', dest='header', action='append',
            metavar='HEADER:VALUE',
            help='Add a header to the request. This can be used multiple '
                 'times for multiple headers. Examples: '
                 '-hif-match:6f432df40167a4af05ca593acc6b3e4c -h '
                 '"If-Modified-Since: Wed, 23 Nov 2011 20:03:38 GMT"')
        self.option_parser.add_option(
            '-q', '--query', dest='query', action='append',
            metavar='NAME[=VALUE]',
            help='Add a query parameter to the request. This can be used '
                 'multiple times for multiple query parameters. Example: '
                 '-qmultipart-manifest=get')
        self.option_parser.add_option(
            '-l', '--limit', dest='limit',
            help='For account and container GETs, this limits the number of '
                 'items returned. Without this option, all items are '
                 'returned, even if it requires several backend requests to '
                 'the gather the information.')
        self.option_parser.add_option(
            '-d', '--delimiter', dest='delimiter',
            help='For account and container GETs, this sets the delimiter for '
                 'the listing retrieved. For example, a container with the '
                 'objects "abc/one", "abc/two", "xyz" and a delimiter of "/" '
                 'would return "abc/" and "xyz". Using the same delimiter, '
                 'but with a prefix of "abc/", would return "abc/one" and '
                 '"abc/two".')
        self.option_parser.add_option(
            '-p', '--prefix', dest='prefix',
            help='For account and container GETs, this sets the prefix for '
                 'the listing retrieved; the items returned will all match '
                 'the PREFIX given.')
        self.option_parser.add_option(
            '-m', '--marker', dest='marker',
            help='For account and container GETs, this sets the marker for '
                 'the listing retrieved; the items returned will begin with '
                 'the item just after the MARKER given (note: the marker does '
                 'not have to actually exist).')
        self.option_parser.add_option(
            '-e', '--end-marker', dest='end_marker', metavar='MARKER',
            help='For account and container GETs, this sets the end-marker '
                 'for the listing retrieved; the items returned will stop '
                 'with the item just before the MARKER given (note: the '
                 'marker does not have to actually exist).')
        self.option_parser.add_option(
            '--ignore-404', dest='ignore_404', action='store_true',
            help='Ignores 404 Not Found responses. Nothing will be output, '
                 'and the exit code will be 0 instead of 1.')
        self.option_parser.add_option(
            '--output-names', dest='output_names', action='store_true',
            help='Outputs the name of each item just before calling [command] '
                 'with it. To ensure easier parsing, the name will be url '
                 'encoded and prefixed with "Item Name: ". For commands that '
                 'have output of their own, this is usually only useful with '
                 'single concurrency; otherwise the item names and the '
                 'command output will get interspersed and impossible to '
                 'associate.')

    def __call__(self, args):
        try:
            index = args.index('do')
        except ValueError:
            raise ReturnCode('No "do" keyword found.')
        args, remaining_args = args[:index], args[index + 1:]
        options, args, context = self.parse_args_and_create_context(args)
        context.remaining_args = remaining_args
        context.headers = self.options_list_to_lowered_dict(options.header)
        context.query = self.options_list_to_lowered_dict(options.query)
        context.ignore_404 = options.ignore_404
        context.output_names = options.output_names
        if options.limit:
            context.query['limit'] = int(options.limit)
        if options.delimiter:
            context.query['delimiter'] = options.delimiter
        if options.prefix:
            context.query['prefix'] = options.prefix
        if options.marker:
            context.query['marker'] = options.marker
        if options.end_marker:
            context.query['end_marker'] = options.end_marker
        path = args.pop(0).lstrip('/') if args else None
        return cli_fordo(context, path)
