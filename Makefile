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
VIRTUAL_ENV := $(or $(VIRTUAL_ENV),.venv$(if $(PY_VERSION),-$(PY_VERSION)))
VENV_ACTIVATE_FILE := $(VIRTUAL_ENV)/bin/activate
VENV_ACTIVATE := source $(VENV_ACTIVATE_FILE)

PY_VERSION := $(shell jq -r '.python_versions.DEFAULT_PY_VER' .ci/variables.json)
export UV_PYTHON := $(PY_VERSION)
export UV_PROJECT_ENVIRONMENT := $(VIRTUAL_ENV)

PRE_COMMIT_HOOK_PATH := .git/hooks/pre-commit

LOG_CI_LEVEL := INFO

# --- Global goals ---

all: lint test it

clean: clean-others clean-docs

check-all: all

.PHONY: \
	all \
	clean \
	check-all \
	uv \
	uv-add \
	uv-lock \
	venv \
	clean-venv \
	install_pytest_rally_plugin \
	install \
	reinstall \
	venv-destroy \
	clean-others \
	clean-pycache \
	lint \
	format \
	pre-commit \
	install-pre-commit \
	docs \
	serve-docs \
	clean-docs \
	test \
	test-all \
	test-3.10 \
	test-3.11 \
	test-3.12 \
	test-3.13 \
	it \
	it_serverless \
	it_tracks_compat \
	benchmark \
	release-checks \
	release \
	sh

# --- uv goals ---

# It checks uv is installed.
uv:
	@if [[ ! -x $$(command -v uv) ]]; then \
		printf "Please install uv by running the following outside of a virtual environment (https://docs.astral.sh/uv/getting-started/installation/)"; \
	fi

# It adds a list of packages to the project.
uv-add:
ifndef ARGS
	$(error Missing arguments. Use make uv-add ARGS="...")
endif
	uv add $$ARGS

# It updates the uv lock file.
uv-lock:
	uv lock


# --- venv goals ---

# It creates the project virtual environment.
venv: uv $(VENV_ACTIVATE_FILE)

$(VENV_ACTIVATE_FILE):
	uv venv --allow-existing --seed
	uv sync --locked --extra=develop

# It delete the project virtual environment from disk.
clean-venv:
	rm -fR '$(VIRTUAL_ENV)'

# It installs the Rally PyTest plugin.
install_pytest_rally_plugin: venv
	$(VENV_ACTIVATE); uv pip install 'pytest-rally @ git+https://github.com/elastic/pytest-rally.git'

# Old legacy alias goals
install: venv
	uv sync --locked --extra=develop

reinstall: clean-venv
	$(MAKE) venv

venv-destroy: clean-venv


# --- Other goals ---

# It delete many temporary files types.
clean-others: clean-pycache
	rm -rf .benchmarks .eggs .nox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
clean-pycache:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;


# --- Linter goals ---

# It run all linters on all files using pre-commit.
lint: venv
	uv run -- pre-commit run --all-files

# It run all linters on changed files using pre-commit.
pre-commit: venv
	uv run -- pre-commit run

# It install a pre-commit hook in the project .git dir so modified files are checked before creating every commit.
install-pre-commit: $(PRE_COMMIT_HOOK_PATH)

$(PRE_COMMIT_HOOK_PATH):
	mkdir -p $(dir $@)
	echo '#!/usr/bin/env $(SHELL)' > '$@'
	echo 'make pre-commit' >> '$@'
	chmod ugo+x '$@'

# pre-commit run also formats files, but let's keep `make format` for convenience.
format: lint


# --- Doc goals ---

# It build project documentation.
docs: venv
	$(VENV_ACTIVATE); $(MAKE) -C docs/ html

serve-docs: venv
	$(VENV_ACTIVATE); $(MAKE) -C docs/ serve

# It cleans project documentation.
clean-docs: venv
	$(VENV_ACTIVATE); $(MAKE) -C docs/ clean


# --- Unit tests goals ---

# It runs unit tests using the default python interpreter version.
test: venv
	uv run -- pytest -s $(or $(ARGS), tests/)

# It runs unit tests using all supported python versions.
test-all: test-3.10 test-3.11 test-3.12 test-3.13

# It runs unit tests using Python 3.10.
test-3.10:
	$(MAKE) test PY_VERSION=3.10

# It runs unit tests using Python 3.11.
test-3.11:
	$(MAKE) test PY_VERSION=3.11

# It runs unit tests using Python 3.12.
test-3.12:
	$(MAKE) test PY_VERSION=3.12

# It runs unit tests using Python 3.13.
test-3.13:
	$(MAKE) test PY_VERSION=3.13


# --- Integration tests goals ---

# It runs integration tests.
it: venv
	$(MAKE) test ARGS=it/

# It runs serverless integration tests.
it_serverless: install_pytest_rally_plugin
	uv run -- pytest -s --log-cli-level=$(LOG_CI_LEVEL) --track-repository-test-directory=it_serverless it/track_repo_compatibility $(ARGS)

# It runs rally_tracks_compat integration tests.
it_tracks_compat: install_pytest_rally_plugin
	uv run -- pytest -s --log-cli-level=$(LOG_CI_LEVEL) it/track_repo_compatibility $(ARGS)

# It runs benchmark tests.
benchmark: venv
	$(MAKE) test ARGS=benchmarks/

# --- Release goals ---

release-checks: venv
	$(VENV_ACTIVATE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: venv release-checks clean docs lint test it
	$(VENV_ACTIVATE); ./release.sh $(release_version) $(next_version)

# This is a shortcut for creating a shell running inside the project virtual environment.
sh:
	$(VENV_ACTIVATE); sh
