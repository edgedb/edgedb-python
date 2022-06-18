#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import sys

if sys.version_info < (3, 6):
    # edgedb.NamedTuple() relies on ordered **kwargs.
    raise RuntimeError('edgedb requires Python 3.6 or greater')

import os
import os.path
import pathlib
import re
import subprocess

# We use vanilla build_ext, to avoid importing Cython via
# the setuptools version.
from distutils import extension as distutils_extension
from distutils.command import build_ext as distutils_build_ext

import setuptools
from setuptools.command import build_py as setuptools_build_py
from setuptools.command import sdist as setuptools_sdist


CYTHON_DEPENDENCY = 'Cython(>=0.29.24,<0.30.0)'

# Minimal dependencies required to test edgedb.
TEST_DEPENDENCIES = [
    # pycodestyle is a dependency of flake8, but it must be frozen because
    # their combination breaks too often
    # (example breakage: https://gitlab.com/pycqa/flake8/issues/427)
    'pycodestyle~=2.6.0',
    'pyflakes~=2.2.0',
    'flake8-bugbear~=21.4.3',
    'flake8~=3.8.1',
    'uvloop>=0.15.1; platform_system != "Windows" and python_version >= "3.7"',
]

# Dependencies required to build documentation.
DOC_DEPENDENCIES = [
    'sphinx~=4.2.0',
    'sphinxcontrib-asyncio~=0.3.0',
    'sphinx_rtd_theme~=1.0.0',
]

EXTRA_DEPENDENCIES = {
    'docs': DOC_DEPENDENCIES,
    'test': TEST_DEPENDENCIES,
    # Dependencies required to develop edgedb.
    'dev': [
        CYTHON_DEPENDENCY,
        'pytest>=3.6.0',
    ] + DOC_DEPENDENCIES + TEST_DEPENDENCIES
}


CFLAGS = ['-O2']
LDFLAGS = []
SYSTEM = sys.platform

if SYSTEM != 'win32':
    CFLAGS.extend(['-std=gnu99', '-fsigned-char', '-Wall',
                   '-Wsign-compare', '-Wconversion'])

if SYSTEM == 'darwin':
    # Lots of warnings from the standard library on macOS 10.14
    CFLAGS.extend(['-Wno-nullability-completeness'])

_ROOT = pathlib.Path(__file__).parent


with open(str(_ROOT / 'README.rst')) as f:
    readme = f.read()


with open(str(_ROOT / 'edgedb' / '_version.py')) as f:
    for line in f:
        if line.startswith('__version__ ='):
            _, _, version = line.partition('=')
            VERSION = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError(
            'unable to read the version from edgedb/_version.py')


if (_ROOT / '.git').is_dir() and 'dev' in VERSION:
    # This is a git checkout, use git to
    # generate a precise version.
    def git_commitish():
        env = {}
        v = os.environ.get('PATH')
        if v is not None:
            env['PATH'] = v

        git = subprocess.run(['git', 'rev-parse', 'HEAD'], env=env,
                             cwd=str(_ROOT), stdout=subprocess.PIPE)
        if git.returncode == 0:
            commitish = git.stdout.strip().decode('ascii')
        else:
            commitish = 'unknown'

        return commitish

    VERSION += '+' + git_commitish()[:7]


class VersionMixin:

    def _fix_version(self, filename):
        # Replace edgedb.__version__ with the actual version
        # of the distribution (possibly inferred from git).

        with open(str(filename)) as f:
            content = f.read()

        version_re = r"(.*__version__\s*=\s*)'[^']+'(.*)"
        repl = r"\1'{}'\2".format(self.distribution.metadata.version)
        content = re.sub(version_re, repl, content)

        with open(str(filename), 'w') as f:
            f.write(content)


class sdist(setuptools_sdist.sdist, VersionMixin):

    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        self._fix_version(pathlib.Path(base_dir) / 'edgedb' / '_version.py')


class build_py(setuptools_build_py.build_py, VersionMixin):

    def build_module(self, module, module_file, package):
        outfile, copied = super().build_module(module, module_file, package)

        if module == '_version' and package == 'edgedb':
            self._fix_version(outfile)

        return outfile, copied


class build_ext(distutils_build_ext.build_ext):

    user_options = distutils_build_ext.build_ext.user_options + [
        ('cython-always', None,
            'run cythonize() even if .c files are present'),
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cython compiler directives'),
    ]

    def initialize_options(self):
        # initialize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        super(build_ext, self).initialize_options()

        if os.environ.get('EDGEDB_DEBUG'):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = "linetrace=True"
            self.define = 'PG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL'
            self.debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_directives = None
            self.debug = False

    def finalize_options(self):
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        need_cythonize = self.cython_always
        cfiles = {}

        for extension in self.distribution.ext_modules:
            for i, sfile in enumerate(extension.sources):
                if sfile.endswith('.pyx'):
                    prefix, ext = os.path.splitext(sfile)
                    cfile = prefix + '.c'

                    if os.path.exists(cfile) and not self.cython_always:
                        extension.sources[i] = cfile
                    else:
                        if os.path.exists(cfile):
                            cfiles[cfile] = os.path.getmtime(cfile)
                        else:
                            cfiles[cfile] = 0
                        need_cythonize = True

        if need_cythonize:
            import pkg_resources

            # Double check Cython presence in case setup_requires
            # didn't go into effect (most likely because someone
            # imported Cython before setup_requires injected the
            # correct egg into sys.path.
            try:
                import Cython
            except ImportError:
                raise RuntimeError(
                    'please install {} to compile edgedb from source'.format(
                        CYTHON_DEPENDENCY))

            cython_dep = pkg_resources.Requirement.parse(CYTHON_DEPENDENCY)
            if Cython.__version__ not in cython_dep:
                raise RuntimeError(
                    'edgedb requires {}, got Cython=={}'.format(
                        CYTHON_DEPENDENCY, Cython.__version__
                    ))

            from Cython.Build import cythonize

            directives = {
                'language_level': '3'
            }
            if self.cython_directives:
                for directive in self.cython_directives.split(','):
                    k, _, v = directive.partition('=')
                    if v.lower() == 'false':
                        v = False
                    if v.lower() == 'true':
                        v = True

                    directives[k] = v

            self.distribution.ext_modules[:] = cythonize(
                self.distribution.ext_modules,
                compiler_directives=directives,
                annotate=self.cython_annotate)

        super(build_ext, self).finalize_options()


INCLUDE_DIRS = [
    'edgedb/pgproto/',
    'edgedb/datatypes',
]


setup_requires = []

if (not (_ROOT / 'edgedb' / 'protocol' / 'protocol.c').exists() or
        '--cython-always' in sys.argv):
    # No Cython output, require Cython to build.
    setup_requires.append(CYTHON_DEPENDENCY)


with open(str(_ROOT / 'README.rst')) as f:
    readme = f.read()


setuptools.setup(
    name='edgedb',
    version=VERSION,
    description='EdgeDB Python driver',
    long_description=readme,
    platforms=['macOS', 'POSIX', 'Windows'],
    author='MagicStack Inc',
    author_email='hello@magic.io',
    url='https://github.com/edgedb/edgedb-python',
    license='Apache License, Version 2.0',
    packages=['edgedb'],
    provides=['edgedb'],
    zip_safe=False,
    include_package_data=True,
    package_data={'edgedb': ['py.typed']},
    ext_modules=[
        distutils_extension.Extension(
            "edgedb.pgproto.pgproto",
            ["edgedb/pgproto/pgproto.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS),

        distutils_extension.Extension(
            "edgedb.datatypes.datatypes",
            ["edgedb/datatypes/args.c",
             "edgedb/datatypes/record_desc.c",
             "edgedb/datatypes/tuple.c",
             "edgedb/datatypes/namedtuple.c",
             "edgedb/datatypes/object.c",
             "edgedb/datatypes/set.c",
             "edgedb/datatypes/hash.c",
             "edgedb/datatypes/array.c",
             "edgedb/datatypes/link.c",
             "edgedb/datatypes/linkset.c",
             "edgedb/datatypes/repr.c",
             "edgedb/datatypes/comp.c",
             "edgedb/datatypes/datatypes.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS),

        distutils_extension.Extension(
            "edgedb.protocol.protocol",
            ["edgedb/protocol/protocol.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS,
            include_dirs=INCLUDE_DIRS),

        distutils_extension.Extension(
            "edgedb.protocol.asyncio_proto",
            ["edgedb/protocol/asyncio_proto.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS,
            include_dirs=INCLUDE_DIRS),

        distutils_extension.Extension(
            "edgedb.protocol.blocking_proto",
            ["edgedb/protocol/blocking_proto.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS,
            include_dirs=INCLUDE_DIRS),
    ],
    cmdclass={'build_ext': build_ext},
    test_suite='tests.suite',
    install_requires=[
        'typing-extensions>=3.10.0',
        'certifi>=2021.5.30; platform_system == "Windows"',
    ],
    extras_require=EXTRA_DEPENDENCIES,
    setup_requires=setup_requires,
)
