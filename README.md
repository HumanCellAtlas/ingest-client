[![Build Status](https://travis-ci.org/HumanCellAtlas/ingest-client.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/ingest-client)
[![Maintainability](https://api.codeclimate.com/v1/badges/2fba112abcaba6d7bcda/maintainability)](https://codeclimate.com/github/HumanCellAtlas/ingest-client/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/2fba112abcaba6d7bcda/test_coverage)](https://codeclimate.com/github/HumanCellAtlas/ingest-client/test_coverage)
[![PyPI](https://img.shields.io/pypi/v/hca-ingest.svg)](https://pypi.org/project/hca-ingest/)

# Ingest Client

This repository contains the hca-ingest Python package library which can be shared across ingest services.

## Installation

    pip install hca-ingest

## Usage

### API package

To use the Ingest API interface in your python script 

    from ingest.api.ingestapi import IngestApi

Configure the ingest url to be used by setting the environment variable for INGEST_API
    
    INGEST_API=http://localhost:8080

### Schema template package

The schema template package provides convenient lookup of properties in the HCA JSON schema.
Each property in the JSON schema is represented as a simple key that is prefixed with the schema name.

The first element is the short name for the schema followed by the property. e.g the key for the biomaterial_id property in the
donor_organism schema is `donor_organism.biomaterial_core.biomaterial_id`.

The schema template provides access to attributes of each key that is useful for developing schema aware applications that need to query or generate JSON documents against the JSON schema.

| Key | Description | Examples |
| --- |---| --- |
| {key}.schema.high_level_entity | Tells you if the property is part of `type`, `module` or `core` schema | `donor_organism.biomaterial_core.schema.high_level_entity` = core, `donor_organism.schema.high_level_entity` = type, `donor_organism.medical_history.schema.high_level_entity` = module |
| {key}.schema.domain_entity | Tells you if the property is in a `biomaterial`, `file`, `process`, `protocol`, `analysis` or `project` schema | `donor_organism.schema.domain_entity` = biomaterial,  `dissociation_protocol.schema.domain_entity` = protocol,  `dissociation_protocol.schema.domain_entity` = protocol, `sequence_file.schema.domain_entity` = File|
| {key}.schema.module | Tells you the name of the schema where this property is defined | `donor_organism.schema.domain_entity` = biomaterial,  `dissociation_protocol.schema.module` = dissociation_protocol,  `dissociation_protocol.schema.module` = dissociation_protocol, `donor_organism.medical_history.schema.module` = medical_history |
| {key}.schema.url | Gives you the full URL to the schema where this property is defined  |  `donor_organism.medical_history.schema.url` = https://schema.humancellatlas.org/module/biomaterial/5.1.0/medical_history |
| {key}.value_type | Tells you the expected value stype for this property. Can be one of `object`, `string`, `integer`  | `donor_organism.medical_history.medication.value_type` = string, `sequence_file.lane_index.value_type` = integer, `sequence_file.file_core.value_type` = object |
| {key}.multivalue | Tells you in the value is a single value or an array of values  |  `sequence_file.insdc_run.multivalue` = True |
| {key}.user_friendly | The user friendly name for the property  | `sequence_file.insdc_run.multivalue` = INSDC run |
| {key}.description | A short description of the property | `sequence_file.insdc_run.multivalue` = An INSDC (International Nucleotide Sequence Database Collaboration) run accession. Accession must start with DRR, ERR, or SRR. |
| {key}.format | Tell you if the property has a specific format, like a date format  | `project.contact.email.format` = email |
| {key}.required | Tells you if the property is required  | `donor_organism.biomaterial_core.biomaterial_id.required` = True|
| {key}.identifiable | Tells you if the property is an identifiable field for the current entity  | `donor_organism.biomaterial_core.biomaterial_id.identifiable` = True,  `donor_organism.biomaterial_core.biomaterial_name.identifiable` = False |
| {key}.external_reference | Tells you if the property is globaly identifiable and therefore retrievable a retrievable object from ingest   | `donor_organism.uuid.external_reference` = True|
| {key}.example  | An example of the expected value for this property  |  `project.contact.contact_name.example` = John,D,Doe |







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

    pip install -e path/to/ingest-client

or by having `pip` do it automatically by providing a reference to this repository:

    pip install -e \
    git+https://github.com/HumanCellAtlas/ingest-client.git\
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


