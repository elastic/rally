clean:
	rm -rf .benchmarks .eggs .tox .rally_it .cache build dist esrally.egg-info logs junit-py*.xml
	cd docs && $(MAKE) clean

test:
	python3 setup.py test

integration-test:
	tox

benchmark:
	python3 setup.py pytest --addopts="-s benchmarks"

coverage:
	coverage run setup.py test
	coverage html

.PHONY: clean test integration-test benchmark coverage