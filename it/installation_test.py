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

"""
These tests ensure the validity of Rally installation instructions (as shown in docs)
"""

import json
import os

import it

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_CI_VARS = ".ci/variables.json"

with open(os.path.join(ROOT_DIR, _CI_VARS), "rt") as fp:
    try:
        MIN_PY_VER = json.load(fp)["python_versions"]["MIN_PY_VER"]
    except KeyError:
        raise EnvironmentError(f"Installation tests require [python_versions.MIN_PY_VER] key in [{_CI_VARS}]")


def test_installs_inside_venv():
    # as in the VirtualEnv Install section of install.rst
    commands = (
        "cp -a /rally_ro /rally &&"
        "python3 -mvenv .venv &&"
        "source .venv/bin/activate &&"
        "cd /rally &&"
        "python3 -m pip install --upgrade pip &&"
        "python3 -m pip install -e . &&"
        "esrally list tracks"
    )

    assert it.command_in_docker(commands, python_version=MIN_PY_VER) == 0


def test_local_installation():
    # as in the Installing Rally install.rst
    commands = (
        "cp -a /rally_ro /rally &&"
        "cd /rally &&"
        "export PATH=$PATH:~/.local/bin &&"
        "python3 -m pip install --user --upgrade pip &&"
        "python3 -m pip install --user -e . &&"
        "esrally list tracks"
    )

    assert it.command_in_docker(commands, python_version=MIN_PY_VER) == 0
