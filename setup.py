#!/usr/bin/python
# Copyright 2011-2013 Gregory Holt
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

import swiftly


setup(
    name='swiftly', version=swiftly.VERSION, description='Client for Swift',
    author='Gregory Holt', author_email='swiftly@brim.net',
    url='http://gholt.github.com/swiftly/',
    packages=['swiftly', 'swiftly.cli', 'swiftly.client'],
    scripts=['bin/swiftly'])
