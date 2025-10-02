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
VENV_DIR := .venv$(if $(PY_VERSION),-$(PY_VERSION))
VENV_ACTIVATE_FILE := $(VENV_DIR)/bin/activate
VENV_ACTIVATE := source $(VENV_ACTIVATE_FILE)

PRE_COMMIT_HOOK_PATH := .git/hooks/pre-commit

PY_VERSION := $(shell jq -r '.python_versions.DEFAULT_PY_VER' .ci/variables.json)


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
	test-3.10 \
	test-3.11 \
	test-3.12 \
	test-3.13 \
	test-all \
	it \
	check-all \
	benchmark \
	release-checks \
	release

# --- uv goals ---

uv:
	@if [[ ! -x $$(command -v uv) ]]; then \
		printf "Please install uv by running the following outside of a virtual env: [ curl -LsSf https://astral.sh/uv/install.sh | $(SHELL) ]\n"; \
	fi

uv-add:
ifndef ARGS
	$(error Missing arguments. Use make uv-add ARGS="...")
endif
	uv add --python $(PY_VERSION) $$ARGS

uv-lock:
	uv lock --python $(PY_VERSION)


# --- venv goals ---

venv: uv $(VENV_ACTIVATE_FILE)

$(VENV_ACTIVATE_FILE):
	uv venv --allow-existing --seed --python '$(PY_VERSION)' '$(VENV_DIR)'
	$(VENV_ACTIVATE); uv sync --active --managed-python --python '$(PY_VERSION)' --locked --extra=develop
	$(VENV_ACTIVATE); uv pip install 'pytest-rally @ git+https://github.com/elastic/pytest-rally.git'

clean-venv:
	rm -fR '$(VENV_DIR)'

# Old legacy alias goals

install: venv

reinstall: clean-venv
	$(MAKE) venv

venv-destroy: clean-venv


# --- Other goals ---

clean-others: clean-pycache
	rm -rf .benchmarks .eggs .nox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml NOTICE.txt

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
clean-pycache:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;


# --- Linter goals ---

lint: venv
	$(VENV_ACTIVATE); pre-commit run --all-files

pre-commit: venv
	$(VENV_ACTIVATE); pre-commit run

install-pre-commit: $(PRE_COMMIT_HOOK_PATH)

$(PRE_COMMIT_HOOK_PATH):
	mkdir -p $(dir $@)
	echo '#!/usr/bin/env $(SHELL)' > '$@'
	echo 'make pre-commit' >> '$@'
	chmod ugo+x '$@'

# pre-commit run also formats files, but let's keep `make format` for convenience
format: lint

# --- Doc goals ---

docs: venv
	$(VENV_ACTIVATE); $(MAKE) -C docs/ html

serve-docs: venv
	$(VENV_ACTIVATE); $(MAKE) -C docs/ serve

clean-docs:
	$(VENV_ACTIVATE); $(MAKE) -C docs/ clean

# --- Test goals ---

test: venv
	$(VENV_ACTIVATE); pytest -s $(or $(ARGS), tests/)

test-all: test-3.10 test-3.11 test-3.12 test-3.13

test-3.10:
	$(MAKE) test PY_VERSION=3.10

test-3.11:
	$(MAKE) test PY_VERSION=3.11

test-3.12:
	$(MAKE) test PY_VERSION=3.12

test-3.13:
	$(MAKE) test PY_VERSION=3.13

# It checks the recommended python version
it: venv
	$(MAKE) test ARGS=it/

it_serverless:
	$(VENV_ACTIVATE); pytest -s --log-cli-level=INFO --track-repository-test-directory=it_serverless $(or $(ARGS), it/track_repo_compatibility)

benchmark: venv
	$(MAKE) test ARGS=benchmarks/

release-checks: venv
	$(VENV_ACTIVATE); ./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: venv release-checks clean docs lint test it
	$(VENV_ACTIVATE); ./release.sh $(release_version) $(next_version)
