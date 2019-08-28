MODULES=ingest tests
.PHONY: lint test unit-tests

 lint:
	flake8 $(MODULES) *.py --ignore=E501,E731

 test: lint unit-tests

 unit-tests:
	time python -m unittest discover --start-directory tests --verbose
