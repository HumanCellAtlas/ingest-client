#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "08/05/2018"

from ingest.template.spreadsheet_builder import SpreadsheetBuilder


# build a generic spreadsheet from the latest schemas
spreadsheet_builder = SpreadsheetBuilder("generic.xlsx")
spreadsheet_builder.generate_workbook()
spreadsheet_builder.save_workbook()

# build a spreadsheet given a spreadsheet layout given a set of schemas

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
    "https://schema.humancellatlas.org/type/process/1.0.0/process"
]

spreadsheet_builder = SpreadsheetBuilder("human_10x_new_protocols.xlsx")
spreadsheet_builder.generate_workbook(tabs_template="tabs_human_10x.yaml", schema_urls=schemas)
spreadsheet_builder.save_workbook()


# build a spreadsheet given new dev schemas

schemas = [
    "http://schema.dev.data.humancellatlas.org/type/project/latest/project",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/cell_suspension",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/specimen_from_organism",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/donor_organism",
    "http://schema.dev.data.humancellatlas.org/type/file/latest/sequence_file",
    "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/latest/dissociation_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/latest/enrichment_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/latest/library_preparation_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/latest/sequencing_protocol",
]

spreadsheet_builder = SpreadsheetBuilder("new_protocols.xlsx")
spreadsheet_builder.generate_workbook(schema_urls=schemas)
spreadsheet_builder.save_workbook()