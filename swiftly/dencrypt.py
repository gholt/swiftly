"""
Encryption routines for Swiftly.

Requires PyCrypto 2.6.1 or greater.
<https://www.dlitz.net/software/pycrypto/>

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
import hashlib

try:
    import Crypto.Cipher.AES
    import Crypto.Random
    AES256CBC_Support = True
except ImportError:
    AES256CBC_Support = False


#: Constant that can be used a preamble for algorithm detection.
AES256CBC = '\x00'


def aes_encrypt(key, stdin, preamble=None, chunk_size=65536,
                content_length=None):
    """
    Generator that encrypts a content stream using AES 256 in CBC
    mode.

    :param key: Any string to use as the encryption key.
    :param stdin: Where to read the contents from.
    :param preamble: str to yield initially useful for providing a
        hint for future readers as to the algorithm in use.
    :param chunk_size: Largest amount to read at once.
    :param content_length: The number of bytes to read from stdin.
        None or < 0 indicates reading until EOF.
    """
    if not AES256CBC_Support:
        raise Exception(
            'AES256CBC not supported; likely pycrypto is not installed')
    if preamble:
        yield preamble
    # Always use 256-bit key
    key = hashlib.sha256(key).digest()
    # At least 16 and a multiple of 16
    chunk_size = max(16, chunk_size >> 4 << 4)
    iv = Crypto.Random.new().read(16)
    yield iv
    encryptor = Crypto.Cipher.AES.new(key, Crypto.Cipher.AES.MODE_CBC, iv)
    reading = True
    left = None
    if content_length is not None and content_length >= 0:
        left = content_length
    while reading:
        size = chunk_size
        if left is not None and size > left:
            size = left
        chunk = stdin.read(size)
        if not chunk:
            if left is not None and left > 0:
                raise IOError('Early EOF from input')
            # Indicates how many usable bytes in last block
            yield encryptor.encrypt('\x00' * 16)
            break
        if left is not None:
            left -= len(chunk)
            if left <= 0:
                reading = False
        block = chunk
        trailing = len(block) % 16
        while trailing:
            size = 16 - trailing
            if left is not None and size > left:
                size = left
            chunk = stdin.read(size)
            if not chunk:
                if left is not None and left > 0:
                    raise IOError('Early EOF from input')
                reading = False
                # Indicates how many usable bytes in last block
                chunk = chr(trailing) * (16 - trailing)
            elif left is not None:
                left -= len(chunk)
                if left <= 0:
                    reading = False
            block += chunk
            trailing = len(block) % 16
        yield encryptor.encrypt(block)


def aes_decrypt(key, stdin, chunk_size=65536):
    """
    Generator that decrypts a content stream using AES 256 in CBC
    mode.

    :param key: Any string to use as the decryption key.
    :param stdin: Where to read the encrypted data from.
    :param chunk_size: Largest amount to read at once.
    """
    if not AES256CBC_Support:
        raise Exception(
            'AES256CBC not supported; likely pycrypto is not installed')
    # Always use 256-bit key
    key = hashlib.sha256(key).digest()
    # At least 16 and a multiple of 16
    chunk_size = max(16, chunk_size >> 4 << 4)
    iv = stdin.read(16)
    while len(iv) < 16:
        chunk = stdin.read(16 - len(iv))
        if not chunk:
            raise IOError('EOF reading IV')
    decryptor = Crypto.Cipher.AES.new(key, Crypto.Cipher.AES.MODE_CBC, iv)
    data = ''
    while True:
        chunk = stdin.read(chunk_size)
        if not chunk:
            if len(data) != 16:
                raise IOError('EOF reading encrypted stream')
            data = decryptor.decrypt(data)
            trailing = ord(data[-1])
            if trailing > 15:
                raise IOError(
                    'EOF reading encrypted stream or trailing value corrupted '
                    '%s' % trailing)
            yield data[:trailing]
            break
        data += chunk
        if len(data) > 16:
            # Always leave at least one byte pending
            trailing = (len(data) % 16) or 16
            yield decryptor.decrypt(data[:-trailing])
            data = data[-trailing:]
