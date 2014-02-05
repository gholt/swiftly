"""
Contains a CLICommand that implements ping test functionality.

Uses the following from :py:class:`swiftly.cli.context.CLIContext`:

===============  ====================================================
client_manager   For connecting to Swift.
concurrency      The number of concurrent actions that can be
                 performed.
io_manager       For directing output.
limit            The maximum number of Swift nodes to output
                 information about.
object_ring      An instance of swift.common.ring.ring.Ring if you
                 want a report based on Swift nodes with implied
                 usage during the ping test.
ping_begin       The first time.time() when the entire ping test
                 began.
ping_begin_last  The time.time() the last ping task started.
ping_count       The number of objects to use.
ping_verbose     True if you want a full ping report rather than just
                 the overall time.
threshold        Defines the threshold for the threshold node report.
                 This is the multiplier over the average request
                 time.
===============  ====================================================
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
import collections
import StringIO
import time
import traceback
import uuid

from swiftly.concurrency import Concurrency
from swiftly.cli.command import CLICommand, ReturnCode

try:
    from eventlet import sleep
except ImportError:
    sleep = time.sleep


def _cli_ping_status(context, heading, ident, status, reason, headers,
                     contents):
    if headers:
        ident = headers.get('x-trans-id') or ident
    if hasattr(contents, 'read'):
        contents.read()
    if status and status // 100 != 2:
        raise ReturnCode(
            'with %s: %s %s %s' % (heading, status, reason, ident))
    now = time.time()
    if context.ping_verbose:
        with context.io_manager.with_stdout() as fp:
            fp.write(
                '% 6.02fs %s %s\n' %
                (now - context.ping_begin_last, heading, ident))
            fp.flush()
    context.ping_begin_last = now


def _cli_ping_objects(context, heading, conc, container, objects, func,
                      results):
    begin = time.time()
    for obj in objects:
        for (exc_type, exc_value, exc_tb, result) in \
                conc.get_results().itervalues():
            if exc_value:
                with context.io_manager.with_stderr() as fp:
                    fp.write(str(exc_value))
                    fp.write('\n')
                    fp.flush()
        conc.spawn(obj, func, context, results, container, obj)
    conc.join()
    for (exc_type, exc_value, exc_tb, result) in \
            conc.get_results().itervalues():
        if exc_value:
            with context.io_manager.with_stderr() as fp:
                fp.write(str(exc_value))
                fp.write('\n')
                fp.flush()
    elapsed = time.time() - begin
    _cli_ping_status(
        context,
        'object %s x%d at %d concurrency, %.02f/s' %
        (heading, len(objects), conc.concurrency, len(objects) / elapsed),
        None, None, None, None, None)
    overall = results.get('overall')
    if overall:
        overall = sorted(overall, key=lambda x: x[0])
        results['overall'] = overall
        if context.ping_verbose or context.graphite:
            best = overall[0][0]
            worst = overall[-1][0]
            mean = overall[len(overall) / 2][0]
            median = sum(x[0] for x in overall) / len(overall)
            threshold = max(2, mean * 2)
            slows = 0
            for x in overall:
                if x[0] > 2 and x[0] > threshold:
                    slows += 1
            slow_percentage = 100.0 * slows / len(overall)
            with context.io_manager.with_stdout() as fp:
                if context.ping_verbose:
                    fp.write(
                        '        best %.02fs, worst %.02fs, mean %.02fs, '
                        'median %.02fs\n        %d slower than 2s and twice '
                        'the mean, %.02f%%\n' % (
                            best, worst, mean, median, slows, slow_percentage))
                    fp.flush()
                if context.graphite:
                    fp.write(
                        '%s.%s.slow_percentage %.02f %d\n' % (
                            context.graphite, heading, slow_percentage,
                            time.time()))


def _cli_ping_object_put(context, results, container, obj):
    with context.client_manager.with_client() as client:
        begin = time.time()
        try:
            status, reason, headers, contents = client.put_object(
                container, obj, StringIO.StringIO('swiftly-ping'))
        except Exception:
            raise ReturnCode(
                'putting object %r: %s' % (obj, traceback.format_exc()))
        if status // 100 != 2:
            raise ReturnCode(
                'putting object %r: %s %s %s' %
                (obj, status, reason, headers.get('x-trans-id') or '-'))
        elapsed = time.time() - begin
        results['overall'].append((elapsed, headers.get('x-trans-id') or obj))
        if context.object_ring:
            for node in context.object_ring.get_nodes(
                    client.get_account_hash(), container, obj)[1]:
                results[node['ip']].append(
                    (elapsed, headers.get('x-trans-id') or obj))


def _cli_ping_object_get(context, results, container, obj):
    with context.client_manager.with_client() as client:
        begin = time.time()
        try:
            status, reason, headers, contents = client.get_object(
                container, obj, stream=False)
        except Exception:
            raise ReturnCode(
                'getting object %r: %s' % (obj, traceback.format_exc()))
        if status // 100 != 2:
            raise ReturnCode(
                'getting object %r: %s %s %s' %
                (obj, status, reason, headers.get('x-trans-id') or '-'))
        elapsed = time.time() - begin
        results['overall'].append((elapsed, headers.get('x-trans-id') or obj))
        if context.object_ring:
            for node in context.object_ring.get_nodes(
                    client.get_account_hash(), container, obj)[1]:
                results[node['ip']].append(
                    (elapsed, headers.get('x-trans-id') or obj))


def _cli_ping_object_delete(context, results, container, obj):
    with context.client_manager.with_client() as client:
        begin = time.time()
        try:
            status, reason, headers, contents = client.delete_object(
                container, obj)
        except Exception:
            raise ReturnCode(
                'deleting object %r: %s' % (obj, traceback.format_exc()))
        if status // 100 != 2 and status != 404:
            raise ReturnCode(
                'deleting object %r: %s %s %s' %
                (obj, status, reason, headers.get('x-trans-id') or '-'))
        elapsed = time.time() - begin
        results['overall'].append((elapsed, headers.get('x-trans-id') or obj))
        if context.object_ring:
            for node in context.object_ring.get_nodes(
                    client.get_account_hash(), container, obj)[1]:
                results[node['ip']].append(
                    (elapsed, headers.get('x-trans-id') or obj))


def _cli_ping_ring_report(context, timings_dict, label):
    timings_dict.pop('overall', None)  # Not currently used in this function
    if not timings_dict:
        return
    worsts = {}
    for ip, timings in timings_dict.iteritems():
        worst = [0, None]
        for timing in timings:
            if timing[0] > worst[0]:
                worst = timing
        worsts[ip] = worst
    with context.io_manager.with_stdout() as fp:
        fp.write(
            'Worst %s times for up to %d nodes with implied usage:\n' %
            (label, context.limit))
        for ip, (elapsed, xid) in sorted(
                worsts.iteritems(), key=lambda x: x[1][0],
                reverse=True)[:context.limit]:
            fp.write('    %20s % 6.02fs %s\n' % (ip, elapsed, xid))
        fp.flush()
    with context.io_manager.with_stdout() as fp:
        averages = {}
        for ip, timings in timings_dict.iteritems():
            averages[ip] = sum(t[0] for t in timings) / len(timings)
        fp.write(
            'Average %s times for up to %d nodes with implied usage:\n' %
            (label, context.limit))
        for ip, elapsed in sorted(
                averages.iteritems(), key=lambda x: x[1],
                reverse=True)[:context.limit]:
            fp.write('    %20s % 6.02fs\n' % (ip, elapsed))
        fp.flush()
    total = 0.0
    count = 0
    for ip, timings in timings_dict.iteritems():
        total += sum(t[0] for t in timings)
        count += len(timings)
    threshold = total / count * context.threshold
    counts = collections.defaultdict(lambda: 0)
    for ip, timings in timings_dict.iteritems():
        for t in timings:
            if t[0] > threshold:
                counts[ip] += 1
    with context.io_manager.with_stdout() as fp:
        fp.write(
            'Count of %s times past (average * %d) for up to %d nodes with '
            'implied usage:\n' % (label, context.threshold, context.limit))
        for ip, count in sorted(
                counts.iteritems(), key=lambda x: x[1],
                reverse=True)[:context.limit]:
            fp.write('    %20s % 6d\n' % (ip, count))
        fp.flush()
    percentages = {}
    for ip, count in counts.iteritems():
        percentages[ip] = (
            100.0 * count / len(timings_dict[ip]),
            count, len(timings_dict[ip]))
    with context.io_manager.with_stdout() as fp:
        fp.write(
            'Percentage of %s times past (average * %d) for up to %d nodes '
            'with implied usage:\n' %
            (label, context.threshold, context.limit))
        for ip, percentage in sorted(
                percentages.iteritems(), key=lambda x: x[1][0],
                reverse=True)[:context.limit]:
            fp.write(
                '    %20s % 6.02f%%  %d of %d\n' %
                (ip, percentage[0], percentage[1], percentage[2]))
        fp.flush()


def cli_ping(context, prefix):
    """
    Performs a ping test.

    See :py:mod:`swiftly.cli.ping` for context usage information.

    See :py:class:`CLIPing` for more information.

    :param context: The :py:class:`swiftly.cli.context.CLIContext` to
        use.
    :param prefix: The container name prefix to use. Default:
        swiftly-ping
    """
    if not prefix:
        prefix = 'swiftly-ping'
    ping_ring_object_puts = collections.defaultdict(lambda: [])
    ping_ring_object_gets = collections.defaultdict(lambda: [])
    ping_ring_object_deletes = collections.defaultdict(lambda: [])
    context.ping_begin = context.ping_begin_last = time.time()
    container = prefix + '-' + uuid.uuid4().hex
    objects = [uuid.uuid4().hex for x in xrange(context.ping_count)]
    conc = Concurrency(context.concurrency)
    with context.client_manager.with_client() as client:
        client.auth()
        _cli_ping_status(context, 'auth', '-', None, None, None, None)
        _cli_ping_status(context, 'account head', '-', *client.head_account())
        _cli_ping_status(
            context, 'container put', '-', *client.put_container(container))
    if _cli_ping_objects(
            context, 'put', conc, container, objects, _cli_ping_object_put,
            ping_ring_object_puts):
        with context.io_manager.with_stderr() as fp:
            fp.write(
                'ERROR put objects did not complete successfully due to '
                'previous error; but continuing\n')
            fp.flush()
    if _cli_ping_objects(
            context, 'get', conc, container, objects, _cli_ping_object_get,
            ping_ring_object_gets):
        with context.io_manager.with_stderr() as fp:
            fp.write(
                'ERROR get objects did not complete successfully due to '
                'previous error; but continuing\n')
            fp.flush()
    if _cli_ping_objects(
            context, 'delete', conc, container, objects,
            _cli_ping_object_delete, ping_ring_object_deletes):
        with context.io_manager.with_stderr() as fp:
            fp.write(
                'ERROR delete objects did not complete successfully due to '
                'previous error; but continuing\n')
            fp.flush()
    for attempt in xrange(5):
        if attempt:
            sleep(2**attempt)
        with context.client_manager.with_client() as client:
            try:
                _cli_ping_status(
                    context, 'container delete', '-',
                    *client.delete_container(container))
                break
            except ReturnCode as err:
                with context.io_manager.with_stderr() as fp:
                    fp.write(str(err))
                    fp.write('\n')
                    fp.flush()
    else:
        with context.io_manager.with_stderr() as fp:
            fp.write(
                'ERROR could not confirm deletion of container due to '
                'previous error; but continuing\n')
            fp.flush()
    end = time.time()
    with context.io_manager.with_stdout() as fp:
        if context.graphite:
            fp.write(
                '%s.ping_overall %.02f %d\n' % (
                    context.graphite, end - context.ping_begin, time.time()))
        if context.ping_verbose:
            fp.write('% 6.02fs total\n' % (end - context.ping_begin))
        elif not context.graphite:
            fp.write('%.02fs\n' % (end - context.ping_begin))
        fp.flush()
    ping_ring_overall = collections.defaultdict(lambda: [])
    _cli_ping_ring_report(context, ping_ring_object_puts, 'PUT')
    for ip, timings in ping_ring_object_puts.iteritems():
        ping_ring_overall[ip].extend(timings)
    _cli_ping_ring_report(context, ping_ring_object_gets, 'GET')
    for ip, timings in ping_ring_object_gets.iteritems():
        ping_ring_overall[ip].extend(timings)
    _cli_ping_ring_report(context, ping_ring_object_deletes, 'DELETE')
    for ip, timings in ping_ring_object_deletes.iteritems():
        ping_ring_overall[ip].extend(timings)
    _cli_ping_ring_report(context, ping_ring_overall, 'overall')


class CLIPing(CLICommand):
    """
    A CLICommand that implements ping test functionality.

    See the output of ``swiftly help ping`` for more information.
    """

    def __init__(self, cli):
        super(CLIPing, self).__init__(
            cli, 'ping', max_args=1, usage="""
Usage: %prog [main_options] ping [options] [path]

For help on [main_options] run %prog with no args.

Runs a ping test against the account.
The [path] will be used as a prefix to the random container name used (default:
swiftly-ping).""".strip())
        self.option_parser.add_option(
            '-v', '--verbose', dest='ping_verbose', action='store_true',
            help='Outputs additional information as ping works.')
        self.option_parser.add_option(
            '-c', '--count', dest='ping_count', default=1,
            help='Count of objects to create; default 1.')
        self.option_parser.add_option(
            '-o', '--object-ring', dest='object_ring',
            help='The current object ring of the cluster being pinged. This '
                 'will enable output of which nodes are involved in the '
                 'object requests and their implied behavior. Use of this '
                 'also requires the main Swift code is installed and '
                 'importable.')
        self.option_parser.add_option(
            '-l', '--limit', dest='limit',
            help='Limits the node output tables to LIMIT nodes.')
        self.option_parser.add_option(
            '-t', '--threshold', dest='threshold',
            help='Changes the threshold for the final (average * x) reports. '
                 'This will define the value of x, defaults to 2.')
        self.option_parser.add_option(
            '-g', '--graphite', dest='graphite', metavar='PREFIX',
            help='Switches to "graphite" output. The output will be lines of '
                 '"PREFIX.metric value timestamp" suitable for piping to '
                 'graphite (through netcat or something similar).')

    def __call__(self, args):
        options, args, context = self.parse_args_and_create_context(args)
        context.ping_count = int(options.ping_count or 1)
        context.ping_verbose = options.ping_verbose
        context.object_ring = None
        if options.object_ring:
            import swift.common.ring.ring
            context.object_ring = swift.common.ring.ring.Ring(
                options.object_ring)
        context.limit = int(options.limit or 10)
        context.threshold = int(options.threshold or 2)
        context.graphite = options.graphite
        prefix = args.pop(0) if args else 'swiftly-ping'
        return cli_ping(context, prefix)
