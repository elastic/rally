clean: nondocs-clean docs-clean

nondocs-clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml

docs-clean:
	cd docs && $(MAKE) clean

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-find ./ -name "__pycache__" -exec rm -rf -- \{\} \;
	-find ./ -name ".pyc" -exec rm -rf -- \{\} \;

docs:
	cd docs && $(MAKE) html

test:
	python3 setup.py test

it:
	tox

it34:
	tox -e py34

it35:
	tox -e py35

it36:
	tox -e py36

benchmark:
	python3 setup.py pytest --addopts="-s benchmarks"

coverage:
	coverage run setup.py test
	coverage html

release-checks:
	./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: release-checks clean docs it
	./release.sh $(release_version) $(next_version)

docker-it: nondocs-clean python-caches-clean
	@if ! export | grep UID; then -export UID=$(shell id -u) || export UID; fi ; \
	if ! export | grep USER; then export USER=$(shell echo $$USER); fi ; \
	if ! export | grep PWD; then export PWD=$(shell pwd); fi ; \
	docker-compose build --pull; `# add --pull here to rebuild a fresh image` \
	docker-compose run --rm rally-tests /bin/bash -c "make docs-clean && make it"

.PHONY: clean nondocs-clean docs-clean python-caches-clean docs test docker-it it it34 it35 it36 benchmark coverage release release-checks
