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

SHELL = /bin/bash
# We assume an active virtualenv for development
PYENV_REGEX = .pyenv/shims
PY_BIN = python3
# https://github.com/pypa/pip/issues/5599
PIP_WRAPPER = $(PY_BIN) -m pip
export PY38 = $(shell jq -r '.python_versions.PY38' .ci/variables.json)
export PY39 = $(shell jq -r '.python_versions.PY39' .ci/variables.json)
export PY310 = $(shell jq -r '.python_versions.PY310' .ci/variables.json)
export PIP_VERSION = $(shell jq -r '.prerequisite_versions.PIP' .ci/variables.json)
export WHEEL_VERSION = $(shell jq -r '.prerequisite_versions.WHEEL' .ci/variables.json)
VIRTUAL_ENV ?= .venv
VENV_ACTIVATE_FILE = $(VIRTUAL_ENV)/bin/activate
VENV_ACTIVATE = . $(VENV_ACTIVATE_FILE)
VEPYTHON = $(VIRTUAL_ENV)/bin/$(PY_BIN)
PYENV_ERROR = "\033[0;31mIMPORTANT\033[0m: Please install pyenv.\n"
PYENV_PREREQ_HELP = "\033[0;31mIMPORTANT\033[0m: please type \033[0;31mpyenv init\033[0m, follow the instructions there and restart your terminal before proceeding any further.\n"
VE_MISSING_HELP = "\033[0;31mIMPORTANT\033[0m: Couldn't find $(PWD)/$(VIRTUAL_ENV); have you executed make venv-create?\033[0m\n"

prereq:
	pyenv install --skip-existing $(PY38)
	pyenv install --skip-existing $(PY39)
	pyenv install --skip-existing $(PY310)
	pyenv local $(PY38)
	@# Ensure all Python versions are registered for this project
	@ jq -r '.python_versions | [.[] | tostring] | join("\n")' .ci/variables.json > .python-version
	-@ printf $(PYENV_PREREQ_HELP)

venv-create:
	@if [[ ! -x $$(command -v pyenv) ]]; then \
		printf $(PYENV_ERROR); \
		exit 1; \
	fi;
	@if [[ ! -f $(VENV_ACTIVATE_FILE) ]]; then \
		eval "$$(pyenv init -)" && eval "$$(pyenv init --path)" && $(PY_BIN) -mvenv $(VIRTUAL_ENV); \
		printf "Created python3 venv under $(PWD)/$(VIRTUAL_ENV).\n"; \
	fi;

check-venv:
	@if [[ ! -f $(VENV_ACTIVATE_FILE) ]]; then \
	printf $(VE_MISSING_HELP); \
	fi

install-user: venv-create
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install --upgrade pip==$(PIP_VERSION) wheel==$(WHEEL_VERSION)
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install -e .

install: install-user
	# Also install development dependencies
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install -e .[develop]

clean: nondocs-clean docs-clean

nondocs-clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

docs-clean:
	cd docs && $(MAKE) clean

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;

# Force recreation of the virtual environment used by tox.
#
# See https://tox.readthedocs.io/en/latest/#system-overview:
#
# > Note pip will not update project dependencies (specified either in the install_requires or the extras
# > section of the setup.py) if any version already exists in the virtual environment; therefore we recommend
# > to recreate your environments whenever your project dependencies change.
tox-env-clean:
	rm -rf .tox

lint: check-venv
	@. $(VENV_ACTIVATE_FILE); find esrally benchmarks scripts tests it setup.py -name "*.py" -exec pylint -j0 -rn --rcfile=$(CURDIR)/.pylintrc \{\} +
	@. $(VENV_ACTIVATE_FILE); black --check --diff .
	@. $(VENV_ACTIVATE_FILE); isort --check --diff .

format: check-venv
	@. $(VENV_ACTIVATE_FILE); black .
	@. $(VENV_ACTIVATE_FILE); isort .

docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) html

serve-docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) serve

test: check-venv
	. $(VENV_ACTIVATE_FILE); pytest tests/

precommit: lint

unit: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py38-unit
	. $(VENV_ACTIVATE_FILE); tox -e py310-unit

# checks min and max python versions
it: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py38-it
	. $(VENV_ACTIVATE_FILE); tox -e py310-it

it38: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py38-it

it39: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py39-it

it310: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py310-it

check-all: lint test it

benchmark: check-venv
	. $(VENV_ACTIVATE_FILE); pytest benchmarks/

release-checks: check-venv
	. $(VENV_ACTIVATE_FILE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: check-venv release-checks clean docs it
	. $(VENV_ACTIVATE_FILE); ./release.sh $(release_version) $(next_version)

.PHONY: install clean nondocs-clean docs-clean python-caches-clean tox-env-clean docs serve-docs test it it38 it39 it310 benchmark release release-checks prereq venv-create check-env
