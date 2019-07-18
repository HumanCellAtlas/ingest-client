#!/usr/bin/env python


from ingest.template.schema_template import SchemaTemplate, UnknownKeyException

schemas = [
    "https://schema.humancellatlas.org/type/project/5.1.0/project",
    # "https://schema.humancellatlas.org/type/biomaterial/5.1.0/cell_suspension",
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
    "https://schema.dev.data.humancellatlas.org/type/biomaterial/9.0.0/cell_suspension"
]

template = SchemaTemplate(metadata_schema_urls=schemas, migrations_url='https://schema.dev.data.humancellatlas.org/property_migrations')

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

# print(template.yaml_dump(tabs_only=True))
# print(data.json_dump())

try:
    print(template.lookup("cell_suspension.total_estimated_cells"))
except Exception as e:
    print(e)

    print(template.replaced_by("cell_suspension.total_estimated_cells"))

    print(template.replaced_by("cell_suspension.total_estimated_cells.user_friendly"))

    print(template.lookup(template.replaced_by("cell_suspension.total_estimated_cells"))["user_friendly"])

    migration = template.replaced_by("cell_suspension.total_estimated_cells")

    print ("New property: " + migration)



# try:
#     print (template._lookup_migration("cell_suspension.total_estimated_cells", "10.1.3"))
# except UnknownKeyException as e:
#     print(e)