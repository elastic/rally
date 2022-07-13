# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
from os.path import dirname, join

try:
    from setuptools import find_packages, setup
except ImportError:
    print("*** Could not find setuptools. Did you install pip3? *** \n\n")
    raise


def str_from_file(name):
    with open(join(dirname(__file__), name)) as f:
        return f.read().strip()


raw_version = str_from_file("esrally/_version.py")
version = re.match(r'__version__ = "(.+)"', raw_version).group(1)

# tuples of (major, minor) of supported Python versions ordered from lowest to highest
supported_python_versions = [(3, 8), (3, 9), (3, 10)]

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
    "elasticsearch[async]==7.14.0",
    "urllib3==1.26.9",
    # License: BSD
    "psutil==5.8.0",
    # License: MIT
    "py-cpuinfo==7.0.0",
    # License: MIT
    "tabulate==0.8.9",
    # License: MIT
    "jsonschema==3.1.1",
    # License: BSD
    # transitive dependency Markupsafe: BSD
    "Jinja2==2.11.3",
    "markupsafe==2.0.1",
    # License: MIT
    "thespian==3.10.1",
    # recommended library for thespian to identify actors more easily with `ps`
    # "setproctitle==1.1.10",
    # always use the latest version, these are certificate files...
    # License: MPL 2.0
    "certifi",
    # License: MIT
    "yappi==1.3.3",
    # License: BSD
    "ijson==2.6.1",
    # License: Apache 2.0
    # transitive dependencies:
    #   google-crc32c: Apache 2.0
    "google-resumable-media[requests]==1.1.0",
    # License: Apache 2.0
    "google-auth==1.22.1",
]

s3_require = [
    # License: Apache 2.0
    # transitive dependencies:
    #   botocore: Apache 2.0
    #   jmespath: MIT
    #   s3transfer: Apache 2.0
    "boto3==1.18.46",
]

tests_require = ["ujson", "pytest==6.2.5", "pytest-benchmark==3.2.2", "pytest-asyncio==0.18.1"]

# These packages are only required when developing Rally
develop_require = [
    "tox==3.14.0",
    "sphinx==4.2.0",
    "furo==2022.06.21",
    "twine==1.15.0",
    "wheel==0.33.6",
    "github3.py==1.3.0",
    "pylint==2.6.0",
    "black==22.3.0",
    "isort==5.8.0",
    "trustme==0.9.0",
    "pytest-httpserver==1.0.4",
]

python_version_classifiers = ["Programming Language :: Python :: {}.{}".format(major, minor) for major, minor in supported_python_versions]

first_supported_version = "{}.{}".format(supported_python_versions[0][0], supported_python_versions[0][1])

# we call the tool rally, but it will be published as esrally on pypi
setup(
    name="esrally",
    maintainer="Daniel Mitterdorfer",
    maintainer_email="daniel.mitterdorfer@gmail.com",
    version=version,
    description="Macrobenchmarking framework for Elasticsearch",
    long_description=str_from_file("README.md"),
    long_description_content_type="text/markdown",
    url="https://github.com/elastic/rally",
    license="Apache License, Version 2.0",
    packages=find_packages(where=".", exclude=("tests*", "benchmarks*", "it*")),
    include_package_data=True,
    python_requires=">={}".format(first_supported_version),
    package_data={"": ["*.json", "*.yml"]},
    install_requires=install_requires,
    test_suite="tests",
    tests_require=tests_require,
    extras_require={"develop": tests_require + develop_require + s3_require, "s3": s3_require},
    entry_points={
        "console_scripts": ["esrally=esrally.rally:main", "esrallyd=esrally.rallyd:main"],
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
    ]
    + python_version_classifiers,
    zip_safe=False,
)
