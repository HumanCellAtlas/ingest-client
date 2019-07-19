#!/usr/bin/env python
"""
Default/test constants for building generic spreadsheets.
"""

DEFAULT_SCHEMA_LIST = ['https://schema.humancellatlas.org/type/protocol/sequencing/10.0.0/sequencing_protocol',
                       'https://schema.humancellatlas.org/type/protocol/sequencing/6.1.0/library_preparation_protocol',
                       'https://schema.humancellatlas.org/type/protocol/imaging/11.1.1/imaging_protocol',
                       'https://schema.humancellatlas.org/type/protocol/imaging/2.1.0/imaging_preparation_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/3.1.0'
                       '/ipsc_induction_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/3.0.0'
                       '/enrichment_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/6.1.0'
                       '/dissociation_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/2.1.0'
                       '/differentiation_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/9.1.0'
                       '/collection_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/2.0.0'
                       '/aggregate_generation_protocol',
                       'https://schema.humancellatlas.org/type/protocol/biomaterial/5.1.0'
                       '/biomaterial_collection_protocol',
                       'https://schema.humancellatlas.org/type/protocol/analysis/9.0.0/analysis_protocol',
                       'https://schema.humancellatlas.org/type/protocol/7.0.0/protocol',
                       'https://schema.humancellatlas.org/type/project/14.0.0/project',
                       'https://schema.humancellatlas.org/type/process/sequencing/5.1.0/sequencing_process',
                       'https://schema.humancellatlas.org/type/process/sequencing/5.1.0/library_preparation_process',
                       'https://schema.humancellatlas.org/type/process/imaging/5.1.0/imaging_process',
                       'https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/enrichment_process',
                       'https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0'
                       '/dissociation_process',
                       'https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/collection_process',
                       'https://schema.humancellatlas.org/type/process/analysis/11.0.1/analysis_process',
                       'https://schema.humancellatlas.org/type/process/9.1.0/process',
                       'https://schema.humancellatlas.org/type/file/2.0.0/supplementary_file',
                       'https://schema.humancellatlas.org/type/file/9.0.0/sequence_file',
                       'https://schema.humancellatlas.org/type/file/3.0.0/reference_file',
                       'https://schema.humancellatlas.org/type/file/2.0.0/image_file',
                       'https://schema.humancellatlas.org/type/file/6.0.0/analysis_file',
                       'https://schema.humancellatlas.org/type/biomaterial/10.2.0/specimen_from_organism',
                       'https://schema.humancellatlas.org/type/biomaterial/11.1.0/organoid',
                       'https://schema.humancellatlas.org/type/biomaterial/3.1.0/imaged_specimen',
                       'https://schema.humancellatlas.org/type/biomaterial/15.3.0/donor_organism',
                       'https://schema.humancellatlas.org/type/biomaterial/13.1.0/cell_suspension',
                       'https://schema.humancellatlas.org/type/biomaterial/14.3.0/cell_line']

"""
Two additional pieces of information must be provided to a LinkedSpreadsheetBuilder in order to enable linking fields 
between metadata schema in the spreadsheet (i.e. copying over a column from one tab to another tab).

TODO(maniarathi): Clean up the documentation below to clearly signal the purpose and structure of the two data 
structures.

- Make one multitab spreadsheet per backbone (this is not a list as initially discussed envisioned)
- Add all potential protocols from protocol_pairings where matched in backbone
- autofill_scale should be applied to all items in backbone (i.e. sheets)
- Special handling of the last process to ensure bundling is done correctly in exporter
- No support yet for automatically filling in linking to multiple protocols
- Only symmetrical linking between entities can be automatically filled

*** Note: multiple sheet generation is currently not supported and in order to create multiple sheets, 
the spreadsheet generator should be run multiple times.
"""

DEFAULT_BACKBONE = [
    {'donor_organism': 1},
    {'specimen_from_organism': 1},
    {'cell_suspension': 1},
    {'sequence_file': 3}
]

DEFAULT_PROTOCOL_PAIRINGS = {
    "collection_protocol": [
        {
            "source": "donor_organism",
            "output": "specimen_from_organism"
        }
    ],
    "aggregate_generation_protocol": [
        {
            "source": "cell_line",
            "output": "organoid"
        },
        {
            "source": "specimen_from_organism",
            "output": "organoid"
        }
    ],
    "differentiation_protocol": [
        {
            "source": "cell_line",
            "output": "cell_suspension"
        },
        {
            "source": "cell_line",
            "output": "cell_line"
        },
        {
            "source": "cell_line",
            "output": "organoid"
        }
    ],
    "dissociation_protocol": [
        {
            "source": "specimen_from_organism",
            "output": "cell_line"
        },
        {
            "source": "specimen_from_organism",
            "output": "cell_suspension"
        },
        {
            "source": "cell_line",
            "output": "cell_suspension"
        },
        {
            "source": "organoid",
            "output": "cell_suspension"
        }
    ],
    "enrichment_protocol": [
        {
            "source": "specimen_from_organism",
            "output": "cell_line"
        },
        {
            "source": "specimen_from_organism",
            "output": "cell_suspension"
        },
        {
            "source": "cell_line",
            "output": "cell_suspension"
        },
        {
            "source": "organoid",
            "output": "cell_suspension"
        }
    ],
    "ipsc_induction_protocol": [
        {
            "source": "cell_line",
            "output": "cell_line"
        },
        {
            "source": "specimen_from_organism",
            "output": "cell_line"
        }
    ],
    "imaging_preparation_protocol": [
        {
            "source": "specimen_from_organism",
            "output": "imaged_specimen"
        }
    ],
    "imaging_protocol": [
        {
            "source": "imaged_specimen",
            "output": "image_file"
        }
    ],
    "library_preparation_protocol": [
        {
            "source": "cell_suspension",
            "output": "sequence_file"
        }
    ],
    "sequencing_protocol": [
        {
            "source": "cell_suspension",
            "output": "sequence_file"
        }
    ]
}

# Link config helps interpret linking and bundling.
DEFAULT_LINK_CONFIG = [DEFAULT_BACKBONE, DEFAULT_PROTOCOL_PAIRINGS]

# Autofill scale defines how many bundles
DEFAULT_AUTOFILL_SCALE = 1
