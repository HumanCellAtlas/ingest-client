# Building Spreadsheet template

You can use this python library to build an empty HCA spreadsheet. `build_spreadsheet.py` shows how the python API can be used to create an empty
spreadsheet.

## Build an empty spreadsheet from the latest JSON schema

Use the `SpreadsheetBuilder` class to generate a generic empty spreadsheet. This will  read the latest version of the HCA metadata schema
and save the spreadsheet template to the filename provided.


```python
from ingest.template.spreadsheet_builder import SpreadsheetBuilder

spreadsheet_builder = SpreadsheetBuilder("generic.xlsx")
spreadsheet_builder.generate_workbook()
spreadsheet_builder.save_workbook()
```

Note, this is driven entirely from what is declared in the JSON schema so the spreadsheet will contain every possible submittable type. Also note that
the order with which tabs and columns are laid out is driven by the order with which are declared in the JSON schema. This default order is unlikely to be desirable for users of the spreadsheet. Brokers should
use this as a base template and remove unnecessary tabs/columns before sharing these templates with users (or see below on how to provide a custom template configuration)

**Generic spreadsheets will not contain linking columns e.g. a donor_id column in the speciemn tab, you will need to add these yourself or use a custom template (see below)**


## Building an empty spreadsheet from a specific set of schemas

By default `SpreadsheetBuilder` will query the schemas API in ingest to get the latest schema versions. If you want to build a spreadsheet with a specific set of schemas you can
supply these as a parameter. Note, you can only give `type` schemas to the SpreadsheetBuilder, it will traverse the schema to fetch all the necessary sub-modules.

To build a template from a specific set of schemas or from the latest `develop` schemas, you can pass in a list of schema URLs to `SpreadsheetBuilder` as follows

```python
from ingest.template.spreadsheet_builder import SpreadsheetBuilder

schemas = [
    "http://schema.dev.data.humancellatlas.org/type/project/latest/project",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/cell_suspension",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/specimen_from_organism",
    "http://schema.dev.data.humancellatlas.org/type/biomaterial/latest/donor_organism",
    "http://schema.dev.data.humancellatlas.org/type/file/latest/sequence_file",
    "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/latest/dissociation_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/biomaterial_collection/latest/enrichment_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/latest/library_preparation_protocol",
    "http://schema.dev.data.humancellatlas.org/type/protocol/sequencing/latest/sequencing_protocol"
]

spreadsheet_builder = SpreadsheetBuilder("latest_dev_template.xlsx")
spreadsheet_builder.generate_workbook(schema_urls=schemas)
spreadsheet_builder.save_workbook()
```

## Building an empty spreadsheet with a pre-defined layout

You can create a template for a spreadsheet that defines which tabs/columns to include and specifies the order. We use a simple YAML based representation
for these templates. Use this YAML format to declare tabs and columns and pass this to the `SpreadsheetBuilder`. Here's an exmaple YAML file for a small spreadsheet.

Properties in the schema can be referred to using a DOT notation that represents the location of the property in the metadata hierarchy. The first element is the short name for the schema followed
by the property e.g. The DOT notation for the `ncbi_taxon_id` property from the `biomaterial_core` module in the `donor_organism` schema is `donor_organism.biomaterial_core.ncbi_taxon_id`.

**Linking fields can be addded by linking columns based on the `*_id` field. e.g. to link a `specimen_from_organism` to a `donor` you must place a  `donor_organism.biomaterial_core.biomaterial_id` column in the `speciemen_from_organism` sheet. The same applied when
linking entities (files or biomaterials) to a protocol**


```yaml
tabs:
  - project:
      display_name : Project
      columns:
        - project.project_core.project_shortname
        - project.project_core.project_title
        - project.project_core.project_description
  - contributors:
      display_name : Contact
      columns:
        - project.contributors.contact_name
        - project.contributors.email
        - project.contributors.phone
        - project.contributors.institution
        - project.contributors.laboratory
        - project.contributors.address
        - project.contributors.country
        - project.project_core.project_shortname
  - donor_organism:
      display_name : Donor
      columns:
        - donor_organism.biomaterial_core.biomaterial_id
        - donor_organism.biomaterial_core.biomaterial_name
        - donor_organism.biomaterial_core.ncbi_taxon_id
        - donor_organism.genus_species.text
        - donor_organism.is_living
        - donor_organism.biological_sex
        - donor_organism.weight
        - donor_organism.weight_unit.text
        - donor_organism.weight_unit.ontology
  - specimen_from_organism:
      display_name : "Specimen from organism"
      columns:
        - specimen_from_organism.biomaterial_core.biomaterial_id
        - specimen_from_organism.biomaterial_core.biomaterial_name
        - specimen_from_organism.biomaterial_core.ncbi_taxon_id
        - donor_organism.biomaterial_core.biomaterial_id
        - specimen_from_organism.genus_species.text
        - specimen_from_organism.organ.text
        - specimen_from_organism.organ.ontology
        - specimen_from_organism.organ_part.text
        - process.process_core.process_id
  - cell_suspension:
      display_name : "Cell suspension"
      columns:
        - cell_suspension.biomaterial_core.biomaterial_id
        - cell_suspension.biomaterial_core.biomaterial_name
        - cell_suspension.biomaterial_core.biomaterial_description
        - cell_suspension.biomaterial_core.ncbi_taxon_id
        - specimen_from_organism.biomaterial_core.biomaterial_id
        - cell_suspension.cell_morphology.cell_size
        - cell_suspension.cell_morphology.cell_size_unit.text
        - cell_suspension.cell_morphology.cell_viability
        - cell_suspension.cell_morphology.cell_viability_method
        - cell_suspension.selected_cell_type
        - cell_suspension.total_estimated_cells
        - dissociation_protocol.protocol_core.protocol_id
        - enrichment_protocol.protocol_core.protocol_id
        - process.deviation_from_protocol
        - process.process_core.process_id
  - dissociation_protocol:
      display_name : "Dissociation protocol"
      columns:
        - dissociation_protocol.protocol_core.protocol_id
        - dissociation_protocol.protocol_core.protocol_name
        - dissociation_protocol.protocol_core.protocol_description
        - dissociation_protocol.protocol_core.publication_doi
        - dissociation_protocol.protocol_core.protocols_io_doi
        - dissociation_protocol.protocol_type.text
        - dissociation_protocol.protocol_type.ontology
  - enrichment_protocol:
      display_name : "Enrichment protocol"
      columns:
        - enrichment_protocol.protocol_core.protocol_id
        - enrichment_protocol.protocol_core.protocol_name
        - enrichment_protocol.protocol_core.protocol_description
        - enrichment_protocol.enrichment_method
        - enrichment_protocol.markers
        - enrichment_protocol.min_size_selected
        - enrichment_protocol.max_size_selected
        - enrichment_protocol.protocol_type.text
        - enrichment_protocol.protocol_type.ontology
  - library_preparation_protocol:
      display_name : "Library preparation protocol"
      columns:
        - library_preparation_protocol.protocol_core.protocol_id
        - library_preparation_protocol.protocol_core.protocol_name
        - library_preparation_protocol.protocol_core.protocol_description
        - library_preparation_protocol.cell_barcode.barcode_read
        - library_preparation_protocol.cell_barcode.barcode_offset
        - library_preparation_protocol.cell_barcode.barcode_length
        - library_preparation_protocol.cell_barcode.white_list_file
        - library_preparation_protocol.input_nucleic_acid_molecule.text
        - library_preparation_protocol.input_nucleic_acid_molecule.ontology
        - library_preparation_protocol.library_construction_approach
        - library_preparation_protocol.library_construction_kit.retail_name
        - library_preparation_protocol.library_construction_kit.catalog_number
        - library_preparation_protocol.library_construction_kit.manufacturer
        - library_preparation_protocol.library_construction_kit.batch_number
        - library_preparation_protocol.library_construction_kit.expiry_date
        - library_preparation_protocol.nucleic_acid_conversion_kit.retail_name
        - library_preparation_protocol.nucleic_acid_conversion_kit.catalog_number
        - library_preparation_protocol.nucleic_acid_conversion_kit.manufacturer
        - library_preparation_protocol.nucleic_acid_conversion_kit.batch_number
        - library_preparation_protocol.nucleic_acid_conversion_kit.expiry_date
        - library_preparation_protocol.end_bias
        - library_preparation_protocol.primer
        - library_preparation_protocol.strand
        - library_preparation_protocol.spike_in_kit.retail_name
        - library_preparation_protocol.spike_in_kit.catalog_number
        - library_preparation_protocol.spike_in_kit.manufacturer
        - library_preparation_protocol.spike_in_kit.batch_number
        - library_preparation_protocol.spike_in_kit.expiry_date
        - library_preparation_protocol.spike_in_dilution
        - library_preparation_protocol.umi_barcode.barcode_read
        - library_preparation_protocol.umi_barcode.barcode_offset
        - library_preparation_protocol.umi_barcode.barcode_length
        - library_preparation_protocol.umi_barcode.white_list_file
        - library_preparation_protocol.protocol_core.document
        - library_preparation_protocol.protocol_type.text
        - library_preparation_protocol.protocol_type.ontology
  - sequencing_protocol:
      display_name : "Sequencing protocol"
      columns:
        - sequencing_protocol.protocol_core.protocol_id
        - sequencing_protocol.protocol_core.protocol_name
        - sequencing_protocol.protocol_core.protocol_description
        - sequencing_protocol.instrument_manufacturer_model.text
        - sequencing_protocol.instrument_manufacturer_model.ontology
        - sequencing_protocol.local_machine_name
        - sequencing_protocol.paired_ends
        - sequencing_protocol.protocol_type.text
        - sequencing_protocol.protocol_core.document
        - sequencing_protocol.protocol_type.text
        - sequencing_protocol.protocol_type.ontology
  - sequence_file:
      display_name : "Sequence files"
      columns:
        - sequence_file.file_core.file_name
        - sequence_file.file_core.file_format
        - sequence_file.file_core.checksum
        - sequence_file.read_index
        - sequence_file.lane_index
        - sequence_file.read_length
        - sequence_file.insdc_run
        - cell_suspension.biomaterial_core.biomaterial_id
        - library_preparation_protocol.protocol_core.protocol_id
        - sequencing_protocol.protocol_core.protocol_id
        - process.deviation_from_protocol
        - process.process_core.process_id
```

Save your YAML config to a file and load into `SpreadsheetBuilder` with the `tabs_config` parameter.

```python
from ingest.template.spreadsheet_builder import SpreadsheetBuilder

# build a spreadsheet given new dev schemas and a YAML config

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
    "http://schema.dev.data.humancellatlas.org/type/process/latest/process"
]

spreadsheet_builder = SpreadsheetBuilder("my_custom_tabs.xlsx")
spreadsheet_builder.generate_workbook(tabs_template="my_custom_tabs.yaml", schema_urls=schemas)
spreadsheet_builder.save_workbook()
```