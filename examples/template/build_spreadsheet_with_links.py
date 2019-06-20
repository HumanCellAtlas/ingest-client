#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "31/01/2019"

from ingest.template.linked_sheet_builder import LinkedSheetBuilder

'''
Input notes

Expected behaviour:

New argument- take a link_config to interpret linking and bundling.
New argument- take a autofill_scale int to define how many bundles to input

link_config has 2 elements backbone and protocol_pairings
Make one multitab spreadsheet per backbone (this is not a list as initally discussed envisioned)
Add all potential protocols from protocol_pairings where matched in backbone
autofill_scale should be applied to all items in backbone (i.e. sheets)
Special handling of the last process to ensure bundling is done correctly in exporter
No support yet for automatically filling in linking to multiple protocols
Only symmetrical linking between entities can be automatically filled
'''

# NOT SUPPORTED GEN SHEET SHOULD BE RAN MULTIPLE TIMES FOR THIS FUNC
# to make three sheets


# to make one sheet

# backbone = [
#         {'donor_organism': 1},
#         {'specimen_from_organism': 1},
#         {'cell_line': 1},
#         {'cell_line': 1},
#         {'organoid': 5},
#         {'cell_suspension': 1},
#         {'sequence_file': 3}
# ]

backbone = [
    {'donor_organism': 1},
    {'specimen_from_organism': 1},
    {'cell_suspension': 1},
    {'sequence_file': 3}
]

protocol_pairings = {
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

link_config = [backbone, protocol_pairings]
autofill_scale = 1

# build a generic spreadsheet from the latest schemas

# spreadsheet_builder = LinkedSheetBuilder("generic_with_links.xlsx")

spreadsheet_builder = LinkedSheetBuilder("generic_with_links.xlsx", link_config=link_config,
                                         autofill_scale=autofill_scale)

# spreadsheet_builder = LinkedSheetBuilder("generic_with_links.xlsx")
spreadsheet_builder.generate_workbook()
spreadsheet_builder.save_workbook()
