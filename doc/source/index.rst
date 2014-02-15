Swiftly
*******

.. include:: ../version.rst

Latest documentation is always available at http://gholt.github.io/swiftly/

Source code available at http://github.com/gholt/swiftly/

    Copyright 2011-2014 Gregory Holt

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

.. toctree::
    :maxdepth: 1

    readme
    license
    authors
    changelog
    swiftlyconfsample

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


API
===

.. toctree::

    api


.. _overview:


Overview
========

Provides a command line tool as well as Client and other classes for common
Swift functions. Works both with access as a standard external user and as an
internal administrator of a cluster with direct access to the rings and to all
back end servers.


Dependencies
============

Can optionally make use of Eventlet (0.11.0 or later recommended)
http://eventlet.net/

Can optionally make use of PyCrypto (2.6.1 or later)
https://www.dlitz.net/software/pycrypto/

.. note::

    If you ``sudo easy_install swiftly`` on Mac OS X, you may need to run ``sudo chmod -R og+r /Library/Python/2.7/site-packages`` in order to run swiftly.


Client Class Usage
==================

Example as a standard end user::

    from swiftly.client import StandardClient
    client = StandardClient(
        auth_url='http://127.0.0.1:8080/auth/v1.0',
        auth_user='test:tester', auth_key='testing')
    print client.head_account()

Example as a administrator direct user::

    from swiftly.client import DirectClient
    client = DirectClient(swift_proxy_storage_path='/v1/AUTH_test')
    print client.head_account()

.. include:: ../cli.rst

