[![Build Status](https://travis-ci.org/HumanCellAtlas/ingest-common.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/ingest-common)

# Ingest Common

This repository contain the hca-ingest Python package library which can be shared across ingest services.

## Installation

    pip install hca-ingest

## Usage
To use the Ingest API interface in your python script 

    from ingest.api.ingestapi import IngestApi

Configure the ingest url to be used by setting the environment variable for INGEST_API
    
    INGEST_API=http://localhost:8080

## Developer Notes

Requirements for this project are listed in 2 files: `requirements.txt` and `requirements-dev.txt`.
The `requirements-dev.txt` file contains dependencies specific for development, and needs to be 
installed:

    pip install -r requirements.txt
    pip install -r requirements-dev.txt
    

Note: This package is currently only compatible with Python 3. 

### Running the Tests

To run all the tests, use `nose` package:

    nosetests
    
### Developing Code in Editable Mode

Using `pip`'s editable mode, client projects can refer to the latest code in this repository 
directly without installing it through PyPI. This can be done either by manually cloning the code
base:

    pip install -e path/to/ingest-common

or by having `pip` do it automatically by providing a reference to this repository:

    pip install -e \
    git+https://github.com/HumanCellAtlas/ingest-common.git\
    #egg=hca_ingest
    
For more information on version control support with `pip`, refer to the [VCS
support documentation](https://pip.pypa.io/en/stable/reference/pip_install/#vcs-support).

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


