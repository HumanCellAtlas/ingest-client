#!/usr/bin/env python
"""
This script will read a spreadsheet, generate a manifest, submit all items to the ingest API, 
assign uuid and generate a directory of bundles for the submitted data
"""
from openpyxl.utils.exceptions import InvalidFileException

from spreadsheetUploadError import SpreadsheetUploadError

__author__ = "jupp"
__license__ = "Apache 2.0"

import json, os
from openpyxl import load_workbook
from ingestapi import IngestApi
from optparse import OptionParser
import logging

from itertools import chain
from collections import defaultdict

# these are spreadsheet fields that can be a list
# todo - these should be read out of the json schema at the start
schema_ontologyFields = {"donor" : ["ancestry", "development_stage", "disease", "medication", "strain", "genus_species"],
                    "cell_suspension" : ["target_cell_type", "genus_species"],
                    "death" : ["cause_of_death"],
                    "immortalized_cell_line" : ["cell_type", "disease", "cell_cycle", "genus_species"],
                    "protocol" : ["type"],
                    "primary_cell_line" : ["cell_type", "disease", "cell_cycle", "genus_species"],
                    "specimen_from_organism" : ["body_part", "organ", "genus_species"],
                    "project" : ["experimental_design"],
                    "organoid" : ["model_for_organ", "genus_species"]
                    }


schema_arrayFields = {"seq" : ["insdc_run"],
                "state_of_specimen" : ["gross_image", "microscopic_image", "protocol_ids"],
                "donor" : ["ancestry", "disease", "medication", "strain", "supplementary_files", "protocol_ids"],
                "immortalized_cell_line" : ["supplementary_files", "protocol_ids"],
                "primary_cell_line" : ["supplementary_files", "protocol_ids"],
                "organoid": ["supplementary_files", "protocol_ids"],
                "specimen_from_organism": ["supplementary_files", "protocol_ids"],
                "cell_suspension" : ["target_cell_type", "enrichment", "supplementary_files", "protocol_ids"],
                "publication" : ["authors"],
                "project" : ["supplementary_files", "experimental_design", "experimental_factor_name"]
                  }

schema_timeFields = {"immortalized_cell_line" : ["date_established"],
                 "primary_cell_line" : ["date_established"],
                 "death" : ["time_of_death"]
                }

schema_stringFields = {"donor" : ["age", "weight", "height", "sample_id", "derived_from"],
                   "specimen_from_organism": ["sample_id", "derived_from"],
                   "cell_suspension": ["sample_id", "derived_from"],
                   "immortalized_cell_line": ["sample_id", "derived_from"],
                   "organoid": ["sample_id", "derived_from"],
                   "primary_cell_line": ["sample_id", "derived_from"],
                   "assay": ["assay_id"],
                   "well": ["plate", "row", "col"]
                   }

schema_booleanFields = {"donor_organism": ["is_living"],
                        "sequencing_process": ["paired_ends"],
                        "death": ["cold_perfused"]
                       }

SCHEMA_URL = os.environ.get('SCHEMA_URL', "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/%s/json_schema/")
# SCHEMA_URL = os.path.expandvars(os.environ.get('SCHEMA_URL', SCHEMA_URL))
SCHEMA_VERSION = os.environ.get('SCHEMA_VERSION', '4.6.1')


class SpreadsheetSubmission:

    def __init__(self, dry=False, output=None, schema_version=None):
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # logging.basicConfig(formatter=formatter, level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.dryrun = dry
        self.outputDir = output
        self.ingest_api = None
        self.schema_version = schema_version if schema_version else os.path.expandvars(SCHEMA_VERSION)
        self.schema_url = os.path.expandvars(SCHEMA_URL % self.schema_version)
        if not self.dryrun:
            self.ingest_api = IngestApi()

    def createSubmission(self, token):
        self.logger.info("creating submission...")
        if not self.ingest_api:
            self.ingest_api = IngestApi()

        submissionUrl = self.ingest_api.createSubmission(token)
        self.logger.info("new submission " + submissionUrl)
        return submissionUrl

    # this only works for keys nested to two levels, need something smarter to handle arbitrary
    # depth keys e.g. we support <level1>.<level2> = <value>, where value is either a single value or list
    # this doesn't support <level1>.<level2>.<level3> except if <level3> is "ontology"
    #This function takes a dictionary object, a key/value pair from the spreadsheet input and an object type, and adds
    #the key/value pair to the dictionary. The type is used to assess whether a field is an ontology or an array based on the
    # two lookup dictionaries declared at the top.
    # For || separated strings, the value field is split. For json arrays, the value is put into an array. Json objects of type
    # ontology require some extra formatting, which is also done here.

    def _keyValueToNestedObject(self, obj, key, value, type):
        if "*" in key:
            key = key.replace("*", "")
        d = value
        # If the value contains a double pipe (||) or the key is for a field that can be a list (with or without also being
        # an ontology field), put value into an array (splitting if necessary)
        if "\"" in str(value) or "||" in str(value) \
                or (type in schema_arrayFields.keys() and key.split('.')[-1] in schema_arrayFields[type]) \
                or (type in schema_arrayFields.keys() and key.split('.')[-1] == "ontology"
                    and key.split('.')[-2] in schema_arrayFields[type]):
            if "||" in str(value):
            # d = map(lambda it: it.strip(' "\''), str(value).split("||"))
                d = str(value).split("||")
            else:
                d = [value]

        # Raise an error if the key is too nested
        if len(key.split('.')) > 3:
            raise ValueError('We don\'t support keys nested greater than 3 levels, found:'+key)

        # If the key is in the date_time field list, convert the date time into a string of format YYYY-MM-DDThh:mm:ssZ
        # so it validates
        if type in schema_timeFields.keys():
            if key.split('.')[-1] in schema_timeFields[type]:
                if isinstance(d, list):
                    for i, v in enumerate(d):
                        date_string = v.strftime("%Y-%m-%dT%H:%M:%SZ")
                        d[i] = date_string
                else:
                    d = d.strftime("%Y-%m-%dT%H:%M:%SZ")

        if type in schema_stringFields.keys():
            if key.split('.')[-1] in schema_stringFields[type]:
                d = str(d)

        # If the key is in the ontology field list, or contains "ontology", format it according to the ontology json schema
        # if type in schema_ontologyFields.keys():
        #     if key.split('.')[-1] in schema_ontologyFields[type]:
        #         if isinstance(d, list):
        #             t = []
        #             for index, v in enumerate(d):
        #                 t.append({"text" : d[index]})
        #             d = t
        #         else:
        #             d = {"text" : d}
        #     elif key.split('.')[-1] == "ontology":
        #         if isinstance(d, list):
        #             t = []
        #             for index, v in enumerate(d):
        #                 t.append({"ontology": d[index]})
        #             d = t
        #         else:
        #             d = {"ontology": d}
        #         key = ".". join(key.split('.')[:-1])

        if type in schema_booleanFields.keys():
            if key.split('.')[-1] in schema_booleanFields[type]:
                if d.lower() in ["true", "yes"]:
                   d = True
                elif d in ["false", "no"]:
                    d = False

        # Build up the key-value dictionary
        for part in reversed(key.split('.')):
            d = {part: d}

        return self._mergeDict(obj, d)

    def _mergeDict(self, dict1, dict2):
        dict3 = defaultdict(list)
        for k, v in chain(dict1.items(), dict2.items()):
            if k in dict3:
                if isinstance(v, dict):
                    dict3[k].update(self._mergeDict(dict3[k], v))
                elif isinstance(v, list) and isinstance(dict3[k], list) and len(v) == len(dict3[k]):
                    for index, e in enumerate(v):
                        dict3[k][index].update(self._mergeDict(dict3[k][index], e))
                else:
                    dict3[k].update(v)
            else:
                dict3[k] = v
        return dict3

    #sheets with one or more data rows and properties in row 1
    def _multiRowToObjectFromSheet(self, type, sheet):
        objs = []
        for row in sheet.iter_rows(row_offset=1, max_row=(sheet.max_row - 1)):
            obj = {}
            hasData = False
            for cell in row:
                # WARNING: remove long from the checks if using python 3!
                if not cell.value and not isinstance(cell.value, (int, float, long)):
                    continue
                hasData = True
                cellCol = cell.col_idx
                propertyValue = sheet.cell(row=1, column=cellCol).value

                d = self._keyValueToNestedObject(obj, propertyValue, cell.value, type)
                obj.update(d)
            if hasData:
                self.logger.debug(json.dumps(obj))
                objs.append(obj)

        return objs

    # sheets that represent one entity where the properties are in column 0
    def _sheetToObject(self, type, sheet):
        obj = {}
        for row in sheet.iter_rows():
            if len(row) > 1:
                propertyCell = row[0].value
                valueCell = row[1].value
                if valueCell:
                    obj = self._keyValueToNestedObject(obj, propertyCell, valueCell, type)
                    # obj.update(d)
        self.logger.debug(json.dumps(obj))
        return obj

    def completeSubmission(self):
        self.ingest_api.finishSubmission()

    def submit(self, pathToSpreadsheet, submissionUrl, token=None, project_id=None):
        try:
            self._process(pathToSpreadsheet, submissionUrl, token, project_id)
        except ValueError as e:
            self.logger.error("Error:"+str(e))
            raise e

    def dumpJsonToFile(self, object, projectId, name):
        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + str(projectId) + "_" + str(name) + ".json", "w")
            tmpFile.write(json.dumps(object, indent=4))
            tmpFile.close()

    def _process(self, pathToSpreadsheet, submissionUrl, token, existing_project_id):

        # parse the spreadsheet
        try:
            wb = load_workbook(filename=pathToSpreadsheet)
        except InvalidFileException:
            raise SpreadsheetUploadError(400, "The uploaded file is not a valid XLSX spreadsheet", "")
        # This code section deals with cases where the project has already been submitted
        # ASSUMPTION: now additional project information (publications, contributors etc) is added via
        # the spreadsheet if the project already exists

        projectSheet = wb.create_sheet()
        projectPubsSheet = wb.create_sheet()
        contributorSheet = wb.create_sheet()

        if existing_project_id is None:
            projectSheet = wb.get_sheet_by_name("project")

            if "project.publications" in wb.sheetnames:
                projectPubsSheet = wb.get_sheet_by_name("project.publications")
            if "contact" in wb.sheetnames:
                contributorSheet = wb.get_sheet_by_name("contact")

        specimenSheet = wb.create_sheet()
        donorSheet = wb.create_sheet()
        cellSuspensionSheet = wb.create_sheet()
        familialRelationshipSheet = wb.create_sheet()

        if "specimen_from_organism" in wb.sheetnames:
            specimenSheet = wb.get_sheet_by_name("specimen_from_organism")
        if "donor_organism" in wb.sheetnames:
            donorSheet = wb.get_sheet_by_name("donor_organism")
        if "cell_suspension" in wb.sheetnames:
            cellSuspensionSheet = wb.get_sheet_by_name("cell_suspension")
        if "familial_relationship" in wb.sheetnames:
            familialRelationshipSheet = wb.get_sheet_by_name("familial_relationship")

        organoidSheet = wb.create_sheet()
        if "sample.organoid" in wb.sheetnames:
            organoidSheet = wb.get_sheet_by_name("organoid")

        clSheet = wb.create_sheet()
        if "cell_line" in wb.sheetnames:
            clSheet = wb.get_sheet_by_name("cell_line")

        clPublicationSheet = wb.create_sheet()
        if "cell_line.publications" in wb.get_sheet_names:
            clPublicationSheet = wb.get_sheet_by_name("cell_line.publications")


        protocolSheet = wb.create_sheet()
        collectionSheet = wb.create_sheet()
        dissociationSheet = wb.create_sheet()
        enrichmentSheet = wb.create_sheet()
        libraryPrepSheet = wb.create_sheet()
        sequencingSheet = wb.create_sheet()
        reagentsSheet = wb.create_sheet()
        filesSheet = wb.create_sheet()

        if "protocol" in wb.sheetnames:
            protocolSheet = wb.get_sheet_by_name("protocol")
        if "enrichment_process" in wb.sheetnames:
            enrichmentSheet = wb.get_sheet_by_name("enrichment_process")
        if "collection_process" in wb.sheetnames:
            collectionSheet = wb.get_sheet_by_name("collection_process")
        if "dissociation_process" in wb.sheetnames:
            dissociationSheet = wb.get_sheet_by_name("dissociation_process")
        if "library_preparation_process" in wb.sheetnames:
            libraryPrepSheet = wb.get_sheet_by_name("library_preparation_process")
        if "sequencing_process" in wb.sheetnames:
            sequencingSheet = wb.get_sheet_by_name("sequencing_process")
        if "purchased_reagents" in wb.sheetnames:
            reagentsSheet = wb.get_sheet_by_name("purchased_reagents")
        if "sequence_file" in wb.sheetnames:
            filesSheet = wb.get_sheet_by_name("sequence_file")


        # convert data in sheets back into dict
        project = self._multiRowToObjectFromSheet("project", projectSheet)
        enrichment = self._multiRowToObjectFromSheet("enrichment_process", enrichmentSheet)
        collection = self._multiRowToObjectFromSheet("collection_process", collectionSheet)
        dissociation = self._multiRowToObjectFromSheet("dissociation_process", dissociationSheet)
        reagents = self._multiRowToObjectFromSheet("purchased_reagents", reagentsSheet)
        libraryPrep = self._multiRowToObjectFromSheet("library_preparation_process", libraryPrepSheet)
        sequencing = self._multiRowToObjectFromSheet("sequencing_process", sequencingSheet)

        protocols = self._multiRowToObjectFromSheet("protocol", protocolSheet)
        donors = self._multiRowToObjectFromSheet("donor_organism", donorSheet)
        familialRelationships = self._multiRowToObjectFromSheet("familial_relationship", familialRelationshipSheet)
        publications = self._multiRowToObjectFromSheet("project.publications", projectPubsSheet)
        contributors = self._multiRowToObjectFromSheet("contributor", contributorSheet)
        specimens = self._multiRowToObjectFromSheet("specimen_from_organism", specimenSheet)
        cell_suspension = self._multiRowToObjectFromSheet("cell_suspension", cellSuspensionSheet)
        organoid = self._multiRowToObjectFromSheet("organoid", organoidSheet)
        cell_line = self._multiRowToObjectFromSheet("cell_line", clSheet)
        cell_line_publications = self._multiRowToObjectFromSheet("cell_line.publications", clPublicationSheet)
        files = self._multiRowToObjectFromSheet("sequence_file", filesSheet)


        biomaterials = []
        biomaterials.extend(donors)
        biomaterials.extend(specimens)
        biomaterials.extend(cell_suspension)
        biomaterials.extend(organoid)
        biomaterials.extend(cell_line)

        processes = []
        processes.extend(collection)
        processes.extend(dissociation)
        processes.extend(enrichment)
        processes.extend(libraryPrep)
        processes.extend(sequencing)




        # creating submission
        #
        if not self.dryrun and not submissionUrl:
            submissionUrl = self.createSubmission(token)

        linksList = []

        # post objects to the Ingest API after some basic validation
        if existing_project_id is None:
            self.logger.info("Creating a new project for the submission")
            if "project_shortname" not in project["project_core"]:
                raise ValueError('Project must have an id attribute')
            projectId = project["project_core"]["project_shortname"]

             # embedd contact & publication into into project for now
            pubs = []
            for index, publication in enumerate(publications):
                pubs.append(publication)
            project["publications"] = pubs

            cont = []
            for index, contributor in enumerate(contributors):
                cont.append(contributor)
            project["contributors"] = cont

            project.update({"schema_type": "project",
                               "describedBy": self.schema_url + "project.json",
                               "schema_version": self.schema_version})

            self.dumpJsonToFile(project, projectId, "project")

            projectIngest = None
            if not self.dryrun:
                projectIngest = self.ingest_api.createProject(submissionUrl, json.dumps(project), token)

        else:
            if not self.dryrun:
                self.logger.info("Retreiving existing project: " + existing_project_id)
                projectIngest = self.ingest_api.getProjectById(existing_project_id)
                submissionEnvelope = self.ingest_api.getSubmissionEnvelope(submissionUrl)
                self.ingest_api.linkEntity(projectIngest, submissionEnvelope, "submissionEnvelopes")
            else:
                projectIngest = {"content" :
                                     {"project_core":
                                     {"project_shortname" : "dummy_project_id"}}}

            projectId = projectIngest["content"]["project_core"]["project_shortname"]

            self.dumpJsonToFile(projectIngest, projectId, "existing_project")

        protocolMap = {}
        for index, protocol in enumerate(protocols):
            if "protocol_id" not in protocol["protocol_core"]:
                raise ValueError('Protocol must have an id attribute')

            protocol.update({"type": "protocol",
                                "describedBy": self.schema_url + "protocol.json",
                               "schema_version": self.schema_version})
            self.dumpJsonToFile(protocol, projectId, "protocol_" + str(index))
            protocolMap[protocol["protocol_core"]["protocol_id"]] = protocol
            if not self.dryrun:
                protocolIngest = self.ingest_api.createProtocol(submissionUrl, json.dumps(protocol))
                # self.ingest_api.linkEntity(protocolIngest, projectIngest, "projects")
                protocolMap[protocol["protocol_core"]["protocol_id"]] = protocolIngest
            # else:
            #     linksList.append("protocol_"+protocol["protocol_id"]+"-project_"+projectId)

        biomaterialMap = {}

        # for index, donor in enumerate(donors):
        #     if "sample_id" not in donor:
        #         raise ValueError('Sample of type donor must have an id attribute')
        #     sample_id = donor["sample_id"]
        #
        #     # removing the explicit check for donor absence as this should be picked up by the validator
        #     # if "donor" not in donor:
        #         # Returns ValueError if there are no other donor.fields and donor.is_living is missing
        #         # raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')
        #     # else:
        #     if "donor" in donor:
        #         if "is_living" in donor["donor"]:
        #             if donor["donor"]["is_living"].lower()in ["true", "yes"]:
        #                 donor["donor"]["is_living"] = True
        #             elif donor["donor"]["is_living"].lower() in ["false", "no"]:
        #                 donor["donor"]["is_living"] = False
        #             '''
        #             # Commented out because we shouldn't be doing content validation in the converter
        #             else:
        #                 # Returns ValueError if donor.is_living isn't true,yes,false,no
        #                 raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')
        #             '''
        #         # Removing the ValueError if is_living is absent or in the wrong format as this should be flagged by the validator
        #         # else:
        #             # Returns ValueError if there are other donor.fields but donor.is_living is empty
        #             # raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')
        #
        #     # Removing ValueError for absence of ncbi_taxon_id as this should be caught by the validator
        #     # if "ncbi_taxon_id" not in donor:
        #         # Returns ValueError if donor.ncbi_taxon_id is empty
        #         # raise ValueError('Field ncbi_taxon_id for sample ' + sample_id + ' is a required field and must contain a valid NCBI Taxon ID')
        #
        #     if "ncbi_taxon_id" in donor and  "genus_species" in donor:
        #         donor["genus_species"]["ontology"] = "NCBITaxon:" + str(donor["ncbi_taxon_id"])
        #
        #
        #     sampleMap[sample_id] = donor
        #     donorIds.append(sample_id)
        #
        #     sampleProtocols = []
        #     if "protocol_ids" in donor:
        #         for sampleProtocolId in donor["protocol_ids"]:
        #             if sampleProtocolId not in protocolMap:
        #                 raise ValueError('Sample ' + sample_id
        #                                  + ' references a protocol ' + sampleProtocolId + ' that isn\'t in the protocol worksheet')
        #         sampleProtocols = donor["protocol_ids"]
        #         del donor["protocol_ids"]
        #
        #     donor["core"] = {"type" : "sample",
        #                      "describedBy": self.schema_url + "sample.json",
        #                     "schema_version": self.schema_version}
        #
        #     self.dumpJsonToFile(donor, projectId, "donor_" + str(index))
        #     if not self.dryrun:
        #         sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(donor))
        #         self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
        #         sampleMap[sample_id] = sampleIngest
        #
        #         if sampleProtocols:
        #             for sampleProtocolId in sampleProtocols:
        #                 self.ingest_api.linkEntity(sampleIngest, protocolMap[sampleProtocolId], "protocols")
        #     else:
        #         linksList.append("sample_" + str(sample_id) + "-project_" + str(projectId))
        #         if sampleProtocols:
        #             for sampleProtocolId in sampleProtocols:
        #                 linksList.append("sample_" + str(sample_id) + "-protocol_" + str(sampleProtocolId))


        for index, biomaterial in enumerate(biomaterials):
            if "biomaterial_id" not in biomaterial["biomaterial_core"]:
                raise ValueError('Biomaterial must have an id attribute')
            biomaterialMap[biomaterial["biomaterial_core"]["biomaterial_id"]] = biomaterial
            biomaterial_id = biomaterial["biomaterial_core"]["biomaterial_id"]

            # if "ncbi_taxon_id" not in sample:
                # Returns ValueError if donor.ncbi_taxon_id is empty
                # raise ValueError(
                #     'Field ncbi_taxon_id for sample ' + sample_id + ' is a required field and must contain a valid NCBI Taxon ID')

            if "ncbi_taxon_id" in biomaterial and "genus_species" in biomaterial:
                biomaterial["genus_species"]["ontology"] = "NCBITaxon:" + str(biomaterial["ncbi_taxon_id"])

        # add dependent information to various sample types

        for publication in cell_line_publications:
            if "biomaterial_id" in publication["biomaterial_core"]:
                bio_id = publication["biomaterial_core"]["biomaterial_id"]
                del publication["biomaterial_core"]

                if "publications" not in biomaterialMap[bio_id]:
                    biomaterialMap[bio_id]["publications"] = []
                biomaterialMap[bio_id]["publications"].append(publication)

        for familialRel in familialRelationships:
            if "biomaterial_id" in familialRel["biomaterial_core"]:
                bio_id = familialRel["biomaterial_core"]["biomaterial_id"]
                del familialRel["biomaterial_core"]

                if "familial_relationship" not in biomaterialMap[bio_id]:
                    biomaterialMap[bio_id]["familial_relationship"] = []
                biomaterialMap[bio_id]["familial_relationship"].append(familialRel)


        # submit samples to ingest and link to project and protocols
        for index, biomaterial_id in enumerate(biomaterialMap.keys()):
            biomaterial = biomaterialMap[biomaterial_id]
            if "has_input_biomaterial" in biomaterial:
                if biomaterial["has_input_biomaterial"] not in biomaterialMap.keys():
                    raise ValueError('Sample '+ str(biomaterial_id) +' references another sample '+ str(biomaterial["has_input_biomaterial"]) +' that isn\'t in the spraedsheet')
            sampleProtocols = []
          

            biomaterial.update({"type" : "biomaterial",
                              "describedBy": self.schema_url + "sample.json",
                            "schema_version": self.schema_version})

            self.dumpJsonToFile(biomaterial, projectId, "biomaterial_" + str(index))
            if not self.dryrun:
                biomaterialIngest = self.ingest_api.createSample(submissionUrl, json.dumps(biomaterial))
                self.ingest_api.linkEntity(biomaterialIngest, projectIngest, "projects")
                biomaterialMap[biomaterial["biomaterial_core"]["biomaterial_id"]] = biomaterialIngest
                # if sampleProtocols:
                #     for sampleProtocolId in sampleProtocols:
                #         self.ingest_api.linkEntity(biomaterialIngest, protocolMap[sampleProtocolId], "protocols")
            else:
                linksList.append("sample_" + str(biomaterial_id) + "-project_" + str(projectId))
                # if sampleProtocols:
                #     for sampleProtocolId in sampleProtocols:
                #         linksList.append("sample_" + str(sample_id) + "-protocol_" + str(sampleProtocolId))

        # create has_input_biomaterial links between samples separately to make sure all samples are submitted
        for index, biomaterial_id in enumerate(biomaterialMap.keys()):
            if not self.dryrun:
                if "has_input_biomaterial" in biomaterialMap[biomaterial_id]['content']["biomaterial_core"]:
                    self.ingest_api.linkEntity(biomaterialMap[biomaterial_id],
                                               biomaterialMap[biomaterialMap[biomaterial_id]['content']["biomaterial_core"]["has_input_biomaterial"]],
                                               "derivedFromSamples")

            else:
                if "has_input_biomaterial" in biomaterialMap[biomaterial_id]:
                    linksList.append(
                        "sample_" + str(biomaterial_id) + "-derivedFromSamples_" + str(biomaterialMap[biomaterial_id]["biomaterial_core"]["has_input_biomaterial"]))

        #build the process map from the different types of assay infromation
        processMap={}

        for index, process in enumerate(processes):
            if "process_id" not in process["process_core"]:
                raise ValueError('Process must have an id attribute')
            process.update({"type" : "process",
                              "describedBy": self.schema_url + "process.json",
                            "schema_version": self.schema_version})
            processMap[process["process_core"]["process_id"]] = process



        filesMap={}
        for index, file in enumerate(files):
            if "filename" not in file:
                raise ValueError('Files must have a name')
            if "assay_id" not in file:
                raise ValueError('Files must be linked to an assay')
            assay = file["assay_id"]
            seqFile = file["seq"]
            biomaterial = file["sample_id"]
            del file["assay_id"]
            del file["seq"]
            del file["sample_id"]
            # self.dumpJsonToFile({"fileName" : file["filename"], "content" : file}, projectId, "files_" + str(index))
            filesMap[file["filename"]] = file





            processMap[assay]["files"].append(file["filename"])




       

            file["core"] = {"type" : "file",
                            "schema_url": self.schema_url + "file.json",
                           "schema_version": self.schema_version}

            self.dumpJsonToFile(file, projectId, "files_" + str(index))
            if not self.dryrun:
                fileIngest = self.ingest_api.createFile(submissionUrl, file["filename"], json.dumps(file))
                filesMap[file["filename"]] = fileIngest

            #     if sample in sampleMap:
            #         self.ingest_api.linkEntity(fileIngest, sampleMap[sample], "samples")
            # else:
            #     if sample in sampleMap:
            #         linksList.append("file_" + file["filename"] + "-sample_" + sample)

        for index, assay in enumerate(processMap.values()):
            if "assay_id" not in assay:
                raise ValueError('Each assay must have an id attribute' + str(assay))
            if "files" not in assay:
                raise ValueError('Each assay must list associated files using the files attribute')
            else:
                for file in assay["files"]:
                    if file not in filesMap:
                        raise ValueError('Assay references file '+file+' that isn\'t defined in the files sheet')
            files = assay["files"]
            del assay["files"]

            if "sample_id" not in assay:
                raise ValueError("Every assay must reference a sample using the sample_id attribute")
            elif assay["sample_id"] not in biomaterialMap:
                raise ValueError('An assay references a sample '+assay["sample_id"]+' that isn\'t in the samples worksheet')
            biomaterials = assay["sample_id"]
            del assay["sample_id"]

            assay["core"] = {"type" : "assay",
                             "schema_url": self.schema_url + "assay.json",
                             "schema_version": self.schema_version}
            self.dumpJsonToFile(assay, projectId, "assay_" + str(index))

            if not self.dryrun:
                assayIngest = self.ingest_api.createAssay(submissionUrl, json.dumps(assay))
                self.ingest_api.linkEntity(assayIngest, projectIngest, "projects")

                if biomaterials in biomaterialMap:
                    self.ingest_api.linkEntity(assayIngest, biomaterialMap[biomaterials], "samples")

                for file in files:
                    self.ingest_api.linkEntity(assayIngest, filesMap[file], "files")
            else:
                linksList.append("assay_" + str(assay["assay_id"]) + "-project_" + str(projectId))

                if biomaterials in biomaterialMap:
                    linksList.append("assay_" + str(assay["assay_id"]) + "-sample_" + str(biomaterials))

                for file in files:
                    linksList.append("assay_" + str(assay["assay_id"]) + "-file_" + str(file))

        self.dumpJsonToFile(linksList, projectId, "dry_run_links")
        self.logger.info("All done!")
        wb.close()
        return submissionUrl

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path",
                      help="path to HCA example data bundles", metavar="FILE")
    parser.add_option("-d", "--dry", help="doa dry run without submitting to ingest", action="store_true", default=False)
    parser.add_option("-o", "--output", dest="output",
                      help="output directory where to dump json files submitted to ingest", metavar="FILE", default=None)
    parser.add_option("-i", "--id", dest="project_id",
                      help="The project_id for an existing submission", default=None)
    parser.add_option("-v", "--version", dest="schema_version", help="Metadata schema version", default=None)


    (options, args) = parser.parse_args()
    if not options.path:
        print ("You must supply path to the HCA bundles directory")
        exit(2)
    submission = SpreadsheetSubmission(options.dry, options.output, options.schema_version)
    submission.submit(options.path, None, None, options.project_id)
