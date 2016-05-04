# Release checklist:

* Update changelog
* Update version in `setup.py` and `docs/conf.py`
* Install locally for development: `python3 setup.py develop`
* Run tests: `python3 setup.py test`
* Run integration test: `tox`
* Check version with `esrally --version`
* Build new version: `python3 setup.py bdist_wheel`
* Only the first time: `twine register dist/*`
* Upload to PyPI: `twine upload dist/*`
* `git tag -a $VERSION`
* `git push --tags`
* Update version in `setup.py` to next dev version
* Install locally for development: `python3 setup.py develop`