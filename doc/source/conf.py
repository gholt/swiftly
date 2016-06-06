"""Configuration for Sphinx Doc Generation."""
"""Copyright and License.

Copyright 2012-2014 Gregory Holt

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from six import moves
from os import walk
from os.path import dirname, join as path_join, sep as path_sep
from subprocess import Popen
from tempfile import TemporaryFile

import swiftly.cli.cli
import swiftly as package
project = 'Swiftly'
author = 'Gregory Holt'
copyright = '2011-2014, Gregory Holt'

exclude_trees = []
extensions = [
    'sphinx.ext.autodoc', 'sphinx.ext.doctest', 'sphinx.ext.todo',
    'sphinx.ext.coverage']
htmlhelp_basename = package.__name__ + 'doc'
html_static_path = ['_static']
html_theme = 'default'
html_style = 'brim.css'
html_use_smartypants = False
latex_documents = [(
    'index', package.__name__ + '.tex', project + 'Documentation', author,
    'manual')]
master_doc = 'index'
modules = set()
pygments_style = 'sphinx'
release = package.__version__
source_suffix = '.rst'
templates_path = ['_templates']
topdir = path_join(dirname(__file__), '..', '..')
version = package.__version__

with open(path_join(topdir, 'doc', 'version.rst'), 'w') as fp:
    if int(package.__version__.split('.')[1]) % 2:
        fp.write(
            '.. warning::\n\n    Version %s is considered a development '
            'version.\n\n' % package.__version__)
for root, dirs, names in walk(path_join(topdir, package.__name__)):
    parent = root[len(topdir):].strip(path_sep).replace(path_sep, '.')
    if not parent.startswith(package.__name__ + '.test'):
        for name in names:
            if name == '__init__.py':
                modules.add(parent)
            elif name.endswith('.py'):
                modules.add(parent + '.' + name[:-3])
with open(path_join(topdir, 'doc', 'source', 'api.rst'), 'w') as fp:
    fp.write('.. _%s_package:\n\n' % package.__name__)
    first = True
    for module in sorted(modules):
        section_char = '#*=-^"'[min(module.count('.'), 5)]
        title = module + '\n' + (section_char * len(module))
        if first:
            fp.write(
                '.. raw:: html\n\n'
                '    <h1>API Documentation</h1>\n\n')
            first = False
        fp.write(
            '%s\n\n.. automodule:: %s\n    :members:\n    :undoc-members:\n'
            '    :special-members: __call__\n    :show-inheritance:\n\n' %
            (title, module))
with open(path_join(topdir, 'doc', 'cli.rst'), 'w') as fp:
    fp.write('Command Line Help\n')
    fp.write('=================\n')
    fp.write('\n')
    commands = swiftly.cli.cli.COMMANDS
    for index in moves.range(len(commands)):
        name = commands[index].split('.')[2]
        if name == 'help':
            commands[index] = ''
        else:
            commands[index] = 'swiftly help ' + name
    commands = sorted(command for command in commands if command)
    commands.insert(0, 'swiftly help')
    for command in commands:
        fp.write(command + '\n')
        fp.write('-' * len(command) + '\n')
        fp.write('\n')
        fp.write('::\n')
        fp.write('\n')
        with TemporaryFile() as tfp:
            process = Popen(command, shell=True, stdout=tfp)
            process.wait()
            tfp.seek(0)
            for line in tfp:
                fp.write('    ' + line)
        fp.write('\n')
        fp.write('\n')
