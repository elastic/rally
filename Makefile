test:
	python3 setup.py test

coverage:
	coverage run setup.py test
	
release:
	release.sh
	
.PHONY: test coverage release