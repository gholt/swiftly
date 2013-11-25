"""
Provides a logger lookalike that throws away everything sent to it
for use with Swift Proxy Server code.

See swift.common.utils.LogAdapter for what this is acting as.
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


class NullLogger(object):

    client_ip = 'client_ip'
    thread_locals = 'thread_locals'
    txn_id = 'txn_id'

    def debug(*args, **kwargs):
        pass

    def error(*args, **kwargs):
        pass

    def exception(*args, **kwargs):
        pass

    def increment(*args, **kwargs):
        pass

    def set_statsd_prefix(*args, **kwargs):
        pass

    def warn(*args, **kwargs):
        pass

    def warning(*args, **kwargs):
        pass
