test:
	python3 setup.py test
	
release:
	release.sh
	
.PHONY: test release