#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

import ingest.template.spreadsheet_builder as spreadsheet_builder

schemas = [
    "https://schema.humancellatlas.org/type/project/5.1.0/project",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/cell_suspension",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/specimen_from_organism",
    "https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism",
    "https://schema.humancellatlas.org/type/file/5.1.0/sequence_file",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/collection_process",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/dissociation_process",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/enrichment_process",
    "https://schema.humancellatlas.org/type/process/sequencing/5.1.0/library_preparation_process",
    "https://schema.humancellatlas.org/type/process/sequencing/5.1.0/sequencing_process",
    "https://schema.humancellatlas.org/type/protocol/5.1.0/protocol",
    "https://schema.humancellatlas.org/type/protocol/biomaterial/5.1.0/biomaterial_collection_protocol",
    "https://schema.humancellatlas.org/type/protocol/sequencing/5.1.0/sequencing_protocol",
]

spreadsheet_builder.generate_spreadsheet("human_10x.xlsx", "tabs_human_10x.yaml", schemas)