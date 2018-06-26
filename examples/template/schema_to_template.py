#!/usr/bin/env python


from ingest.template.schema_template import SchemaTemplate

schemas = [
    "https://schema.humancellatlas.org/type/project/5.1.0/project",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/cell_suspension",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/specimen_from_organism",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.1/donor_organism",
    "https://schema.humancellatlas.org/type/file/5.1.0/sequence_file",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/collection_process",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/dissociation_process",
    "https://schema.humancellatlas.org/type/process/biomaterial_collection/5.1.0/enrichment_process",
    "https://schema.humancellatlas.org/type/process/sequencing/5.1.0/library_preparation_process",
    "https://schema.humancellatlas.org/type/process/sequencing/5.1.0/sequencing_process",
    "https://schema.humancellatlas.org/type/protocol/5.1.0/protocol",
    "https://schema.humancellatlas.org/type/protocol/biomaterial/5.1.0/biomaterial_collection_protocol",
    "https://schema.humancellatlas.org/type/protocol/sequencing/5.1.0/sequencing_protocol",
    "https://schema.humancellatlas.org/type/process/1.0.0/process",
]

template = SchemaTemplate(list_of_schema_urls=schemas)

# get key from user friendly name

# tabs = TabConfig().load("tabs_human_10x.yaml")

print (template.get_key_for_label("Biomaterial name", tab="Cell suspension"))

# lookup where to submit this entity

print (template.lookup("cell_suspension.schema.domain_entity"))

# lookup text field for donor_organism.human_specific.ethnicity.text

print (template.get_key_for_label("donor_organism.human_specific.ethnicity.text", tab="Donor organism"))



# lookup the schema url for project_core

print (template.lookup("project.project_core.schema.url"))

# get the user friendly name

print (template.lookup("project.project_core.project_title.user_friendly"))

# dump the config in yaml or json

print(template.yaml_dump(tabs_only=True))
# print(data.json_dump())
