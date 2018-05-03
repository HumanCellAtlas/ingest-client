#!/usr/bin/env python

import ingest.importer.schematemplate as schematemplate

schemas = [
    "https://schema.humancellatlas.org/type/project/5.1.0/project",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/cell_suspension",
    "https://schema.humancellatlas.org/type/biomaterial/5.1.0/specimen_from_organism",
    "https://schema.humancellatlas.org/type/biomaterial/5.0.0/donor_organism",

]

data = schematemplate.get_template_from_schemas_by_url(schemas)

# lookup the schema url for project_core

print (data.lookup("projects.project.project_core.schema.url"))

# get the user friendly name

print (data.lookup("projects.project.project_core.project_title.user_friendly"))

# get key from user friendly name

print (data.get_key_for_label("Project title") )

# dump the config in yaml or json

print(data.yaml_dump())
# print(data.json_dump())
