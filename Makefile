clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml
	cd docs && $(MAKE) clean

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

# still run `it` tests locally as Docker on macOS runs in a vm (and is slower)
release-in-docker: release-checks clean docs it
	export UID; \
	export USER; \
	docker-compose build --pull; `# add --pull here to rebuild a fresh image` \
	docker-compose run --rm rally-release ./release.sh $(release_version) $(next_version)

.PHONY: clean docs test it it34 it35 it36 benchmark coverage release release-checks release-in-docker
