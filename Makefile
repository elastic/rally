clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml
	cd docs && $(MAKE) clean

docs:
	cd docs && $(MAKE) html

test:
	python3 setup.py test

integration-test:
	tox

benchmark:
	python3 setup.py pytest --addopts="-s benchmarks"

coverage:
	coverage run setup.py test
	coverage html

.PHONY: clean docs test integration-test benchmark coverage