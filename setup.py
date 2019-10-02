# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from os.path import join, dirname

try:
    from setuptools import setup, find_packages
except ImportError:
    print("*** Could not find setuptools. Did you install pip3? *** \n\n")
    raise


def str_from_file(name):
    with open(join(dirname(__file__), name)) as f:
        return f.read().strip()


raw_version = str_from_file("version.txt")
VERSION = raw_version.split(".")
__version__ = VERSION
__versionstr__ = raw_version

long_description = str_from_file("README.rst")

################################################################################################
#
# Adapt `create-notice.sh` whenever changing dependencies here.
#
# That script grabs all license files so we include them in the notice file.
#
################################################################################################
install_requires = [
    # License: Apache 2.0
    # transitive dependency urllib3: MIT
    "elasticsearch==6.2.0",
    # License: BSD
    "psutil==5.4.0",
    # License: MIT
    "py-cpuinfo==3.2.0",
    # License: MIT
    "tabulate==0.8.1",
    # License: MIT
    "jsonschema==2.5.1",
    # License: BSD
    # transitive dependency Markupsafe: BSD
    "Jinja2==2.10.1",
    # License: MIT
    "thespian==3.9.3",
    # recommended library for thespian to identify actors more easily with `ps`
    # "setproctitle==1.1.10",
    # always use the latest version, these are certificate files...
    # License: MPL 2.0
    "certifi",
    # License: Apache 2.0
    # transitive dependencies:
    #   botocore: Apache 2.0
    #   jmespath: MIT
    #   s3transfer: Apache 2.0
    "boto3==1.9.120"
]

tests_require = [
    # transitive dependency of pytest but pinned to that version to avoid:
    #
    # TypeError: attrib() got an unexpected keyword argument 'convert'
    #
    # see also https://stackoverflow.com/questions/58189683/typeerror-attrib-got-an-unexpected-keyword-argument-convert
    "attrs==19.1.0",
    "pytest==3.4.2",
    "pytest-benchmark==3.1.1"
]

# we call the tool rally, but it will be published as esrally on pypi
setup(name="esrally",
      maintainer="Daniel Mitterdorfer",
      maintainer_email="daniel.mitterdorfer@gmail.com",
      version=__versionstr__,
      description="Macrobenchmarking framework for Elasticsearch",
      long_description=long_description,
      url="https://github.com/elastic/rally",
      license="Apache License, Version 2.0",
      packages=find_packages(
          where=".",
          exclude=("tests*", "benchmarks*")
      ),
      include_package_data=True,
      package_data={"": ["*.json", "*.yml"]},
      install_requires=install_requires,
      test_suite="tests",
      tests_require=tests_require,
      setup_requires=[
          "pytest-runner==2.10.1",
      ],
      entry_points={
          "console_scripts": [
              "esrally=esrally.rally:main",
              "esrallyd=esrally.rallyd:main"
          ],
      },
      classifiers=[
          "Topic :: System :: Benchmark",
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: MacOS :: MacOS X",
          "Operating System :: POSIX",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
      ],
      zip_safe=False)
