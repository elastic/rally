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

SHELL := /bin/bash
# We assume an active virtualenv for development
PYENV_REGEX := .pyenv/shims
PY_BIN := python3
# https://github.com/pypa/pip/issues/5599
PIP_WRAPPER := $(PY_BIN) -m pip
export PY38 := $(shell jq -r '.python_versions.PY38' .ci/variables.json)
export PY39 := $(shell jq -r '.python_versions.PY39' .ci/variables.json)
export PY310 := $(shell jq -r '.python_versions.PY310' .ci/variables.json)
export PY311 := $(shell jq -r '.python_versions.PY311' .ci/variables.json)
export HATCH_VERSION := $(shell jq -r '.prerequisite_versions.HATCH' .ci/variables.json)
export HATCHLING_VERSION := $(shell jq -r '.prerequisite_versions.HATCHLING' .ci/variables.json)
export PIP_VERSION := $(shell jq -r '.prerequisite_versions.PIP' .ci/variables.json)
export WHEEL_VERSION := $(shell jq -r '.prerequisite_versions.WHEEL' .ci/variables.json)
VIRTUAL_ENV ?= .venv
VENV_ACTIVATE_FILE := $(VIRTUAL_ENV)/bin/activate
VENV_ACTIVATE := . $(VENV_ACTIVATE_FILE)
VEPYTHON := $(VIRTUAL_ENV)/bin/$(PY_BIN)
PYENV_ERROR := "\033[0;31mIMPORTANT\033[0m: Please install pyenv.\n"
PYENV_PREREQ_HELP := "\033[0;31mIMPORTANT\033[0m: please type \033[0;31mpyenv init\033[0m, follow the instructions there and restart your terminal before proceeding any further.\n"
VE_MISSING_HELP := "\033[0;31mIMPORTANT\033[0m: Couldn't find $(PWD)/$(VIRTUAL_ENV); have you executed make venv-create?\033[0m\n"

prereq:
	pyenv install --skip-existing $(PY38)
	pyenv install --skip-existing $(PY39)
	pyenv install --skip-existing $(PY310)
	pyenv install --skip-existing $(PY311)
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
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install --upgrade hatch==$(HATCH_VERSION) hatchling==$(HATCHLING_VERSION) pip==$(PIP_VERSION) wheel==$(WHEEL_VERSION)
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install -e .

install: install-user
	# Also install development dependencies
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install -e .[develop]
	. $(VENV_ACTIVATE_FILE); $(PIP_WRAPPER) install git+https://github.com/elastic/pytest-rally.git


clean: nondocs-clean docs-clean

nondocs-clean:
	rm -rf .benchmarks .eggs .nox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

docs-clean:
	cd docs && $(MAKE) clean

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;

lint: check-venv
	@. $(VENV_ACTIVATE_FILE); pre-commit run --all-files

# pre-commit run also formats files, but let's keep `make format` for convenience
format: lint

docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) html

serve-docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) serve

test: check-venv
	. $(VENV_ACTIVATE_FILE); nox -s test-3.8
	. $(VENV_ACTIVATE_FILE); nox -s test-3.11

# checks min and max python versions
it: check-venv python-caches-clean
	. $(VENV_ACTIVATE_FILE); nox -s it-3.8
	. $(VENV_ACTIVATE_FILE); nox -s it-3.11

check-all: lint test it

benchmark: check-venv
	. $(VENV_ACTIVATE_FILE); pytest benchmarks/

release-checks: check-venv
	. $(VENV_ACTIVATE_FILE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: check-venv release-checks clean docs lint test it
	. $(VENV_ACTIVATE_FILE); ./release.sh $(release_version) $(next_version)

.PHONY: install clean nondocs-clean docs-clean python-caches-clean lint format docs serve-docs test it check-all benchmark release release-checks prereq venv-create check-env
