"""
Provides a local in-memory memcache client lookalike for use with
Swift Proxy Server code. This can also be used as a WSGI app with
the Brim.Net Core Package.

See swift.common.memcached.MemcacheRing for what this is acting as.
"""
"""
Copyright 2011-2014 Gregory Holt

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


class _Node(object):

    def __init__(self, key, val, prv, nxt):
        self.key = key
        self.val = val
        self.prv = prv
        self.nxt = nxt


class LocalMemcache(object):

    def __init__(self, name=None, parsed_conf=None, next_app=None):
        self.max_count = 1000
        # Copy all items from the parsed_conf to actual instance attributes.
        if parsed_conf:
            for k, v in parsed_conf.iteritems():
                setattr(self, k, v)
        self.name = name
        self.next_app = next_app
        self.cache = {}
        self.first = None
        self.last = None
        self.count = 0

    def __call__(self, env, start_response):
        env['memcache'] = self
        return self.next_app(env, start_response)

    def set(self, key, value, serialize=True, timeout=0, time=0):
        self.delete(key)
        self.last = node = _Node(key, value, self.last, None)
        if node.prv:
            node.prv.nxt = node
        elif not self.first:
            self.first = node
        self.cache[key] = node
        self.count += 1
        if self.count > self.max_count:
            self.delete(self.first.key)

    def get(self, key):
        node = self.cache.get(key)
        return node.val if node else None

    def incr(self, key, delta=1, timeout=0, time=0):
        result = (self.get(key) or 0) + delta
        self.set(key, result)
        return result

    def decr(self, key, delta=1, timeout=0, time=0):
        return self.incr(key, delta=-delta, timeout=timeout, time=time)

    def delete(self, key):
        node = self.cache.get(key)
        if node:
            del self.cache[key]
            if node.prv:
                node.prv.nxt = node.nxt
            if node.nxt:
                node.nxt.prv = node.prv
            if self.first == node:
                self.first = node.nxt
            if self.last == node:
                self.last = node.prv
            self.count -= 1

    def set_multi(self, mapping, server_key, serialize=True, timeout=0,
                  time=0):
        for key, value in mapping.iteritems():
            self.set(key, value)

    def get_multi(self, keys, server_key):
        return [self.get(k) for k in keys]

    @classmethod
    def parse_conf(cls, name, conf):
        return {'max_count': conf.get_int(name, 'max_count', 1000)}
