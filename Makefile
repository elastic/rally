test:
	python3 setup.py test

coverage:
	coverage run setup.py test
	coverage html

.PHONY: test coverage