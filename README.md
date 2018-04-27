# Ingest Common

This repository contain the hca-ingest Python package library which can be shared across ingest services.

## Developer Notes

Requirements for this project are listed in 2 files: `requirements.txt` and `requirements-dev.txt`.
The `requirements-dev.txt` file contains dependencies specific for development, and needs to be 
installed:

    pip install -r requirements-dev.txt

### Running the Tests

To run all the tests, use `nose` package:

    nosetests

### Publish to PyPI

1. Create PyPI Account through the [registration page](https://pypi.org/account/register/).
    
   Take note that PyPI requires email addresses to be verified before publishing.

2. Package the project for distribution.
 
        python setup.py sdist
        
    Take note that `setup.py` is configured to build a distribution with name `hca_ingest`.
    This PyPI project is currently owned privately and may require access rights to change. 
    Alternatively, the project name in `setup.py` can be changed so that it can be built and
    uploaded to a different PyPI entry.
    
3. Install [Twine](https://pypi.org/project/twine/)

        pip install twine        
    
4. Upload the distribution package to PyPI. 

        twine upload dist/*
        
    Running `python setup.py sdist` will create a package in the `dist` directory of the project
    base directory. Specific packages can be chosen if preferred instead of the wildcard `*`:
    
        twine upload dist/hca_ingest-0.1a0.tar.gz
