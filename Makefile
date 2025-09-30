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
VIRTUAL_ENV ?= .venv
VENV_ACTIVATE_FILE := $(VIRTUAL_ENV)/bin/activate
VE_MISSING_HELP := "\033[0;31mIMPORTANT\033[0m: Couldn't find $(PWD)/$(VIRTUAL_ENV); have you executed make install?\033[0m\n"

PY_VERSION := $(shell jq -r '.python_versions.DEFAULT_PY_VER' .ci/variables.json)

.PHONY: install \
	check-venv \
	reinstall \
	check-uv \
	venv-destroy \
	clean \
	uv-add \
	uv-lock \
	nondocs-clean \
	docs-clean \
	python-caches-clean \
	lint \
	format \
	docs \
	serve-docs \
	test \
	it \
	check-all \
	benchmark \
	release-checks \
	release

install: check-uv
	uv sync --python $(PY_VERSION) --locked --extra=develop

reinstall: venv-destroy install

check-venv:
	@if [[ ! -f $(VENV_ACTIVATE_FILE) ]]; then \
		printf $(VE_MISSING_HELP); \
	fi

check-uv:
	@if [[ ! -x $$(command -v uv) ]]; then \
		printf "Please install uv by running the following outside of a virtual env: [ curl -LsSf https://astral.sh/uv/install.sh | sh ]\n"; \
	fi

venv-destroy:
	@echo "Removing virtual environment $(VIRTUAL_ENV)"
	rm -rf $(VIRTUAL_ENV)

clean: nondocs-clean docs-clean

uv-add:
ifndef ARGS
	$(error Missing arguments. Use make uv-add ARGS="...")
endif
	uv add --python $(PY_VERSION) $$ARGS

uv-lock:
	uv lock --python $(PY_VERSION)

nondocs-clean:
	rm -rf .benchmarks .eggs .nox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

docs-clean:
	cd docs && $(MAKE) clean

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;

lint: check-uv
	uv run --python=$(PY_VERSION) pre-commit run --all-files

# pre-commit run also formats files, but let's keep `make format` for convenience
format: lint

docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) html

serve-docs: check-venv
	@. $(VENV_ACTIVATE_FILE); cd docs && $(MAKE) serve

test: test-3.12

test-3.10: check-venv
	. $(VENV_ACTIVATE_FILE); nox -s test-3.10

test-3.11: check-venv
	. $(VENV_ACTIVATE_FILE); nox -s test-3.11

# It checks the recommended python version
test-3.12: check-venv
	. $(VENV_ACTIVATE_FILE); nox -s test-3.12

test-3.13: check-venv
	. $(VENV_ACTIVATE_FILE); nox -s test-3.13

# It checks the recommended python version
it: it-3.12

it-3.10: check-venv python-caches-clean
	. $(VENV_ACTIVATE_FILE); nox -s it-3.10

it-3.11: check-venv python-caches-clean
	. $(VENV_ACTIVATE_FILE); nox -s it-3.11

it-3.12: check-venv python-caches-clean
	. $(VENV_ACTIVATE_FILE); nox -s it-3.12

it-3.13: check-venv python-caches-clean
	. $(VENV_ACTIVATE_FILE); nox -s it-3.13

check-all: lint test it

benchmark: check-venv
	uv run --python=$(PY_VERSION) pytest benchmarks/

release-checks: check-venv
	. $(VENV_ACTIVATE_FILE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: check-venv release-checks clean docs lint test it
	. $(VENV_ACTIVATE_FILE); ./release.sh $(release_version) $(next_version)
