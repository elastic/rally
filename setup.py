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

# tuples of (major, minor) of supported Python versions ordered from lowest to highest
supported_python_versions = [(3, 8), (3, 9)]

################################################################################################
#
# Adapt `create-notice.sh` whenever changing dependencies here.
#
# That script grabs all license files so we include them in the notice file.
#
################################################################################################
install_requires = [
    # License: Apache 2.0
    # transitive dependencies:
    #   urllib3: MIT
    #   aiohttp: Apache 2.0
    "elasticsearch[async]==7.10.1",
    # License: BSD
    "psutil==5.8.0",
    # License: MIT
    "py-cpuinfo==7.0.0",
    # License: MIT
    "tabulate==0.8.7",
    # License: MIT
    "jsonschema==3.1.1",
    # License: BSD
    # transitive dependency Markupsafe: BSD
    "Jinja2==2.11.3",
    # License: MIT
    "thespian==3.10.1",
    # recommended library for thespian to identify actors more easily with `ps`
    # "setproctitle==1.1.10",
    # always use the latest version, these are certificate files...
    # License: MPL 2.0
    "certifi",
    # License: Apache 2.0
    "yappi==1.2.3",
    # License: BSD
    "ijson==2.6.1",
    # License: Apache 2.0
    # transitive dependencies:
    #   google-crc32c: Apache 2.0
    "google-resumable-media[requests]==1.1.0",
    # License: Apache 2.0
    "google-auth==1.22.1"
]

s3_require = [
    # License: Apache 2.0
    # transitive dependencies:
    #   botocore: Apache 2.0
    #   jmespath: MIT
    #   s3transfer: Apache 2.0
    "boto3==1.10.32",
]

tests_require = [
    "ujson",
    "pytest==5.4.0",
    "pytest-benchmark==3.2.2",
    "pytest-asyncio==0.14.0"
]

# These packages are only required when developing Rally
develop_require = [
    "tox==3.14.0",
    "coverage==4.5.4",
    "sphinx==2.2.0",
    "sphinx_rtd_theme==0.5.1",
    "twine==1.15.0",
    "wheel==0.33.6",
    "github3.py==1.3.0",
    "pylint==2.6.0",
    "pylint-quotes==0.2.1"
]

python_version_classifiers = ["Programming Language :: Python :: {}.{}".format(major, minor)
                              for major, minor in supported_python_versions]

first_supported_version = "{}.{}".format(supported_python_versions[0][0], supported_python_versions[0][1])
# next minor after the latest supported version
first_unsupported_version = "{}.{}".format(supported_python_versions[-1][0], supported_python_versions[-1][1] + 1)

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
          exclude=("tests*", "benchmarks*", "it*")
      ),
      include_package_data=True,
      # supported Python versions. This will prohibit pip (> 9.0.0) from even installing Rally on an unsupported
      # Python version.
      # See also https://packaging.python.org/guides/distributing-packages-using-setuptools/#python-requires
      #
      # According to https://www.python.org/dev/peps/pep-0440/#version-matching, a trailing ".*" should
      # ignore patch versions:
      #
      # "additional trailing segments will be ignored when determining whether or not a version identifier matches
      # the clause"
      #
      # However, with the pattern ">=3.5.*,<=3.8.*", the version "3.8.0" is not accepted. Therefore, we match
      # the minor version after the last supported one (i.e. if 3.8 is the last supported, we'll emit "<3.9")
      python_requires=">={},<{}".format(first_supported_version, first_unsupported_version),
      package_data={"": ["*.json", "*.yml"]},
      install_requires=install_requires,
      test_suite="tests",
      tests_require=tests_require,
      extras_require={
          "develop": tests_require + develop_require + s3_require,
          "s3": s3_require
      },
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
      ] + python_version_classifiers,
      zip_safe=False)
