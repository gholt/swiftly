"""
Contains general tools useful when accessing Swift services.
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
import hashlib
import hmac
import time
from six.moves import urllib_parse as parse


def generate_temp_url(method, url, seconds, key):
    """
    Returns a TempURL good for the given request method, url, and
    number of seconds from now, signed by the given key.
    """
    method = method.upper()
    base_url, object_path = url.split('/v1/')
    object_path = '/v1/' + object_path
    expires = int(time.time() + seconds)
    hmac_body = '%s\n%s\n%s' % (method, expires, object_path)
    sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
    return '%s%s?temp_url_sig=%s&temp_url_expires=%s' % (
        base_url, object_path, sig, expires)


def get_trans_id_time(trans_id):
    """
    Returns the time.time() embedded in the trans_id or None if no
    time information is embedded.

    Copied from the Swift codebase.
    Copyright (c) 2010-2012 OpenStack Foundation
    """
    if len(trans_id) >= 34 and trans_id[:2] == 'tx' and trans_id[23] == '-':
        try:
            return int(trans_id[24:34], 16)
        except ValueError:
            pass
    return None


def quote(value, safe='/:'):
    """
    Much like parse.quote in that it returns a URL encoded string
    for the given value, protecting the safe characters; but this
    version also ensures the value is UTF-8 encoded.
    """
    if isinstance(value, six.text_type):
        value = value.encode('utf8')
    elif not isinstance(value, six.string_types):
        value = str(value)
    return parse.quote(value, safe)


def headers_to_dict(headers):
    """
    Converts a sequence of (name, value) tuples into a dict where if
    a given name occurs more than once its value in the dict will be
    a list of values.
    """
    hdrs = {}
    for h, v in headers:
        h = h.lower()
        if h in hdrs:
            if isinstance(hdrs[h], list):
                hdrs[h].append(v)
            else:
                hdrs[h] = [hdrs[h], v]
        else:
            hdrs[h] = v
    return hdrs
