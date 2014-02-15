"""
Contains tools for connecting to Swift services.

For convenience, the following names are imported from submodules:

=================  ========================================================
StandardClient     :py:class:`swiftly.client.standardclient.StandardClient`
DirectClient       :py:class:`swiftly.client.directclient.DirectClient`
LocalClient        :py:class:`swiftly.client.localclient.LocalClient`
ClientManager      :py:class:`swiftly.client.manager.ClientManager`
generate_temp_url  :py:func:`swiftly.client.utils.generate_temp_url`
get_trans_id_time  :py:func:`swiftly.client.utils.get_trans_id_time`
=================  ========================================================

Copyright 2011-2013 Gregory Holt
Portions Copyright (c) 2010-2012 OpenStack Foundation

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
# flake8: noqa
from swiftly.client.directclient import DirectClient
from swiftly.client.localclient import LocalClient
from swiftly.client.standardclient import StandardClient
from swiftly.client.manager import ClientManager
from swiftly.client.utils import generate_temp_url, get_trans_id_time
