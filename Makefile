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
PY36 = $(shell jq '.python_versions.PY36' .ci/variables.json)
PY37 = $(shell jq '.python_versions.PY37' .ci/variables.json)
PY38 = $(shell jq '.python_versions.PY38' .ci/variables.json)
VENV_NAME ?= .venv
VENV_ACTIVATE_FILE = $(VENV_NAME)/bin/activate
VENV_ACTIVATE = . $(VENV_ACTIVATE_FILE)
VEPYTHON = $(VENV_NAME)/bin/$(PY_BIN)
VEPYLINT = $(VENV_NAME)/bin/pylint
PYENV_ERROR = "\033[0;31mIMPORTANT\033[0m: Please install pyenv.\n"
PYENV_PATH_ERROR = "\033[0;31mIMPORTANT\033[0m: Please add $(HOME)/$(PYENV_REGEX) to your PATH env.\n"
PYENV_PREREQ_HELP = "\033[0;31mIMPORTANT\033[0m: please add \033[0;31meval \"\$$(pyenv init -)\"\033[0m to your bash profile and restart your terminal before proceeding any further.\n"
VE_MISSING_HELP = "\033[0;31mIMPORTANT\033[0m: Couldn't find $(PWD)/$(VENV_NAME); have you executed make venv-create?\033[0m\n"

prereq:
	pyenv install --skip-existing $(PY36)
	pyenv install --skip-existing $(PY37)
	pyenv install --skip-existing $(PY38)
	pyenv local $(PY36) $(PY37) $(PY38)
	@# Ensure all Python versions are registered for this project
	@ jq -r '.python_versions | [.[] | tostring] | join("\n")' .ci/variables.json > .python-version
	-@ printf $(PYENV_PREREQ_HELP)

venv-create:
	@if [[ ! -x $$(command -v pyenv) ]]; then \
		printf $(PYENV_ERROR); \
		exit 1; \
	fi;
	@if [[ ! "$(PATH)" =~ $(PYENV_REGEX) ]]; then \
		printf $(PYENV_PATH_ERROR); \
		exit 1; \
	fi;
	@if [[ ! -f $(VENV_ACTIVATE_FILE) ]]; then \
		eval "$$(pyenv init -)" && $(PY_BIN) -mvenv $(VENV_NAME); \
		printf "Created python3 venv under $(PWD)/$(VENV_NAME).\n"; \
	fi;

check-venv:
	@if [[ ! -f $(VENV_ACTIVATE_FILE) ]]; then \
	printf $(VE_MISSING_HELP); \
	fi

install: venv-create
	. $(VENV_ACTIVATE_FILE); pip install --upgrade pip setuptools
	. $(VENV_ACTIVATE_FILE); pip3 install -e .
	# Also install development dependencies
	. $(VENV_ACTIVATE_FILE); pip3 install -e .[develop]

clean: nondocs-clean docs-clean

nondocs-clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

docs-clean:
	cd docs && $(MAKE) clean

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-@find . -name "__pycache__" -exec rm -rf -- \{\} \;
	-@find . -name ".pyc" -exec rm -rf -- \{\} \;

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
	@find esrally benchmarks scripts tests -name "*.py" -exec $(VEPYLINT) -j0 -rn --load-plugins pylint_quotes --rcfile=$(CURDIR)/.pylintrc \{\} +

docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) html

serve-docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) serve

test: check-venv
	$(VEPYTHON) setup.py test

precommit: lint

it: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox

it36: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py36

it37: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py37

it38: check-venv python-caches-clean tox-env-clean
	. $(VENV_ACTIVATE_FILE); tox -e py38

benchmark: check-venv
	$(VEPYTHON) setup.py pytest --addopts="-s benchmarks"

coverage: check-venv
	. $(VENV_ACTIVATE_FILE); coverage run setup.py test
	. $(VENV_ACTIVATE_FILE); coverage html

release-checks: check-venv
	. $(VENV_ACTIVATE_FILE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: check-venv release-checks clean docs it
	. $(VENV_ACTIVATE_FILE); ./release.sh $(release_version) $(next_version)

.PHONY: install clean nondocs-clean docs-clean python-caches-clean tox-env-clean docs serve-docs test it it36 it37 it38 benchmark coverage release release-checks prereq venv-create check-env
