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
v4_ontologyFields = {"donor" : ["ancestry", "development_stage", "disease", "medication", "strain", "genus_species"],
                    "cell_suspension" : ["target_cell_type", "genus_species"],
                    "death" : ["cause_of_death"],
                    "immortalized_cell_line" : ["cell_type", "disease", "cell_cycle", "genus_species"],
                    "protocol" : ["type"],
                    "primary_cell_line" : ["cell_type", "disease", "cell_cycle", "genus_species"],
                    "specimen_from_organism" : ["body_part", "organ", "genus_species"],
                    "project" : ["experimental_design"],
                    "organoid" : ["model_for_organ", "genus_species"]
                    }


v4_arrayFields = {"seq" : ["insdc_run"],
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

v4_timeFields = {"immortalized_cell_line" : ["date_established"],
                 "primary_cell_line" : ["date_established"],
                 "death" : ["time_of_death"]
                }

v4_stringFields = {"donor" : ["age", "weight", "height"]}

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
                or (type in v4_arrayFields.keys() and key.split('.')[-1] in v4_arrayFields[type]) \
                or (type in v4_arrayFields.keys() and key.split('.')[-1] == "ontology"
                    and key.split('.')[-2] in v4_arrayFields[type]):
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
        if type in v4_timeFields.keys():
            if key.split('.')[-1] in v4_timeFields[type]:
                if isinstance(d, list):
                    for i, v in enumerate(d):
                        date_string = v.strftime("%Y-%m-%dT%H:%M:%SZ")
                        d[i] = date_string
                else:
                    d = d.strftime("%Y-%m-%dT%H:%M:%SZ")

        if type in v4_stringFields.keys():
            if key.split('.')[-1] in v4_stringFields[type]:
                d = str(d)

        # If the key is in the ontology field list, or contains "ontology", format it according to the ontology json schema
        if type in v4_ontologyFields.keys():
            if key.split('.')[-1] in v4_ontologyFields[type]:
                if isinstance(d, list):
                    t = []
                    for index, v in enumerate(d):
                        t.append({"text" : d[index]})
                    d = t
                else:
                    d = {"text" : d}
            elif key.split('.')[-1] == "ontology":
                if isinstance(d, list):
                    t = []
                    for index, v in enumerate(d):
                        t.append({"ontology": d[index]})
                    d = t
                else:
                    d = {"ontology": d}
                key = ".". join(key.split('.')[:-1])

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
                if not cell.value and not isinstance(cell.value, (int, float)):
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
            tmpFile = open(dir + "/" + projectId+"_"+name + ".json", "w")
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
        submitterSheet = wb.create_sheet()
        contributorSheet = wb.create_sheet()

        if existing_project_id is None:
            projectSheet = wb.get_sheet_by_name("project")

            if "project.publications" in wb.sheetnames:
                projectPubsSheet = wb.get_sheet_by_name("project.publications")
            if "contact.submitter" in wb.sheetnames:
                submitterSheet = wb.get_sheet_by_name("contact.submitter")
            if "contact.contributors" in wb.sheetnames:
                contributorSheet = wb.get_sheet_by_name("contact.contributors")

        specimenSheet = wb.create_sheet()
        specimenStateSheet = wb.create_sheet()
        donorSheet = wb.create_sheet()
        deathSheet = wb.create_sheet()
        cellSuspensionSheet = wb.create_sheet()
        cellSuspensionEnrichmentSheet = wb.create_sheet()
        cellSuspensionWellSheet = wb.create_sheet()

        if "sample.specimen_from_organism" in wb.sheetnames:
            specimenSheet = wb.get_sheet_by_name("sample.specimen_from_organism")
        if "state_of_specimen" in wb.sheetnames:
            specimenStateSheet = wb.get_sheet_by_name("state_of_specimen")
        if "sample.donor" in wb.sheetnames:
            donorSheet = wb.get_sheet_by_name("sample.donor")
        if "sample.donor.death" in wb.sheetnames:
            deathSheet = wb.get_sheet_by_name("sample.donor.death")
        if "sample.cell_suspension" in wb.sheetnames:
            cellSuspensionSheet = wb.get_sheet_by_name("sample.cell_suspension")
        if "cell_suspension.enrichment" in wb.sheetnames:
            cellSuspensionEnrichmentSheet = wb.get_sheet_by_name("cell_suspension.enrichment")
        if "sample.cell_suspension.well" in wb.sheetnames:
            cellSuspensionWellSheet = wb.get_sheet_by_name("sample.cell_suspension.well")

        organoidSheet = wb.create_sheet()
        if "sample.organoid" in wb.sheetnames:
            organoidSheet = wb.get_sheet_by_name("sample.organoid")

        immortalizedCLSheet = wb.create_sheet()
        if "sample.immortalized_cell_line" in wb.sheetnames:
            immortalizedCLSheet = wb.get_sheet_by_name("sample.immortalized_cell_line")

        primaryCLSheet = wb.create_sheet()
        if "sample.primary_cell_line" in wb.sheetnames:
            primaryCLSheet = wb.get_sheet_by_name("sample.primary_cell_line")

        protocolSheet = wb.create_sheet()
        singleCellSheet = wb.create_sheet()
        singleCellBarcodeSheet = wb.create_sheet()
        rnaSheet = wb.create_sheet()
        seqSheet = wb.create_sheet()
        seqBarcodeSheet = wb.create_sheet()
        filesSheet = wb.create_sheet()

        if "protocols" in wb.sheetnames:
            protocolSheet = wb.get_sheet_by_name("protocols")
        if "single_cell" in wb.sheetnames:
            singleCellSheet = wb.get_sheet_by_name("single_cell")
        if "single_cell.cell_barcode" in wb.sheetnames:
            singleCellBarcodeSheet = wb.get_sheet_by_name("single_cell.cell_barcode")
        if "rna" in wb.sheetnames:
            rnaSheet = wb.get_sheet_by_name("rna")
        if "seq" in wb.sheetnames:
            seqSheet = wb.get_sheet_by_name("seq")
        if "seq.umi_barcode" in wb.sheetnames:
            seqBarcodeSheet = wb.get_sheet_by_name("seq.umi_barcode")
        if "file" in wb.sheetnames:
            filesSheet = wb.get_sheet_by_name("file")


        # convert data in sheets back into dict
        project = self._sheetToObject("project", projectSheet)
        enrichment = self._multiRowToObjectFromSheet("enrichment", cellSuspensionEnrichmentSheet)
        well = self._multiRowToObjectFromSheet("well", cellSuspensionWellSheet)
        single_cell = self._multiRowToObjectFromSheet("single_cell", singleCellSheet)
        single_cell_barcode = self._multiRowToObjectFromSheet("barcode", singleCellBarcodeSheet)
        rna = self._multiRowToObjectFromSheet("rna", rnaSheet)
        seq = self._multiRowToObjectFromSheet("seq", seqSheet)
        seq_barcode = self._multiRowToObjectFromSheet("barcode", seqBarcodeSheet)

        protocols = self._multiRowToObjectFromSheet("protocol", protocolSheet)
        donors = self._multiRowToObjectFromSheet("donor", donorSheet)
        death = self._multiRowToObjectFromSheet("death", deathSheet)
        publications = self._multiRowToObjectFromSheet("publication", projectPubsSheet)
        submitters = self._multiRowToObjectFromSheet("submitter", submitterSheet)
        contributors = self._multiRowToObjectFromSheet("contributor", contributorSheet)
        specimens = self._multiRowToObjectFromSheet("specimen_from_organism", specimenSheet)
        specimen_state = self._multiRowToObjectFromSheet("state_of_specimen", specimenStateSheet)
        cell_suspension = self._multiRowToObjectFromSheet("cell_suspension", cellSuspensionSheet)
        organoid = self._multiRowToObjectFromSheet("organoid", organoidSheet)
        immortalized_cell_line = self._multiRowToObjectFromSheet("immortalized_cell_line", immortalizedCLSheet)
        primary_cell_line = self._multiRowToObjectFromSheet("primary_cell_line", primaryCLSheet)
        files = self._multiRowToObjectFromSheet("file", filesSheet)


        samples = []
        # samples.extend(donors)
        samples.extend(specimens)
        samples.extend(cell_suspension)
        samples.extend(organoid)
        samples.extend(immortalized_cell_line)
        samples.extend(primary_cell_line)

        # creating submission
        #
        if not self.dryrun and not submissionUrl:
            submissionUrl = self.createSubmission(token)

        linksList = []

        # post objects to the Ingest API after some basic validation
        if existing_project_id is None:
            self.logger.info("Creating a new project for the submission")
            if "project_id" not in project:
                raise ValueError('Project must have an id attribute')
            projectId = project["project_id"]

             # embedd contact & publication into into project for now
            pubs = []
            for index, publication in enumerate(publications):
                pubs.append(publication)
            project["publications"] = pubs

            subs = []
            for index, submitter in enumerate(submitters):
                subs.append(submitter)
            project["submitters"] = subs

            cont = []
            for index, contributor in enumerate(contributors):
                cont.append(contributor)
            project["contributors"] = cont

            project["core"] = {"type": "project",
                               "schema_url": self.schema_url + "project.json",
                               "schema_version": self.schema_version}

            self.dumpJsonToFile(project, projectId, "project")

            projectIngest = None
            if not self.dryrun:
                projectIngest = self.ingest_api.createProject(submissionUrl, json.dumps(project))

        else:
            if not self.dryrun:
                self.logger.info("Retreiving existing project: " + existing_project_id)
                projectIngest = self.ingest_api.getProjectById(existing_project_id)
                submissionEnvelope = self.ingest_api.getSubmissionEnvelope(submissionUrl)
                self.ingest_api.linkEntity(projectIngest, submissionEnvelope, "submissionEnvelopes")
            else:
                projectIngest = {"content" :
                                     {"project_id" : "dummy_project_id"}}

            projectId = projectIngest["content"]["project_id"]

            self.dumpJsonToFile(projectIngest, projectId, "existing_project")

        protocolMap = {}
        for index, protocol in enumerate(protocols):
            if "protocol_id" not in protocol:
                raise ValueError('Protocol must have an id attribute')

            protocol["core"] = {"type": "protocol",
                                "schema_url": self.schema_url + "protocol.json",
                               "schema_version": self.schema_version}
            self.dumpJsonToFile(protocol, projectId, "protocol_" + str(index))
            protocolMap[protocol["protocol_id"]] = protocol
            if not self.dryrun:
                protocolIngest = self.ingest_api.createProtocol(submissionUrl, json.dumps(protocol))
                # self.ingest_api.linkEntity(protocolIngest, projectIngest, "projects")
                protocolMap[protocol["protocol_id"]] = protocolIngest
            # else:
            #     linksList.append("protocol_"+protocol["protocol_id"]+"-project_"+projectId)

        sampleMap = {}
        donorIds = []
        deathMap = {}

        for d in death:
            if "sample_id" in d:
                deathMap[d["sample_id"]] = d

        for index, donor in enumerate(donors):
            if "sample_id" not in donor:
                raise ValueError('Sample of type donor must have an id attribute')
            sample_id = donor["sample_id"]

            # removing the explicit check for donor absence as this should be picked up by the validator
            # if "donor" not in donor:
                # Returns ValueError if there are no other donor.fields and donor.is_living is missing
                # raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')
            # else:
            if "donor" in donor:
                if "is_living" in donor["donor"]:
                    if donor["donor"]["is_living"].lower()in ["true", "yes"]:
                        donor["donor"]["is_living"] = True
                    elif donor["donor"]["is_living"].lower() in ["false", "no"]:
                        donor["donor"]["is_living"] = False
                    '''
                    # Commented out because we shouldn't be doing content validation in the converter
                    else:
                        # Returns ValueError if donor.is_living isn't true,yes,false,no
                        raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')
                    '''
                # Removing the ValueError if is_living is absent or in the wrong format as this should be flagged by the validator
                # else:
                    # Returns ValueError if there are other donor.fields but donor.is_living is empty
                    # raise ValueError('Field is_living for sample ' + sample_id + ' is a required field and must either contain one of yes, true, no, or false')

            # Removing ValueError for absence of ncbi_taxon_id as this should be caught by the validator
            # if "ncbi_taxon_id" not in donor:
                # Returns ValueError if donor.ncbi_taxon_id is empty
                # raise ValueError('Field ncbi_taxon_id for sample ' + sample_id + ' is a required field and must contain a valid NCBI Taxon ID')

            if "ncbi_taxon_id" in donor and  "genus_species" in donor:
                donor["genus_species"]["ontology"] = "NCBITaxon:" + str(donor["ncbi_taxon_id"])

            if sample_id in deathMap.keys():
                donor["donor"]["death"] = d["death"]

            sampleMap[sample_id] = donor
            donorIds.append(sample_id)

            sampleProtocols = []
            if "protocol_ids" in donor:
                for sampleProtocolId in donor["protocol_ids"]:
                    if sampleProtocolId not in protocolMap:
                        raise ValueError('Sample ' + sample_id
                                         + ' references a protocol ' + sampleProtocolId + ' that isn\'t in the protocol worksheet')
                sampleProtocols = donor["protocol_ids"]
                del donor["protocol_ids"]

            donor["core"] = {"type" : "sample",
                             "schema_url": self.schema_url + "sample.json",
                            "schema_version": self.schema_version}

            self.dumpJsonToFile(donor, projectId, "donor_" + str(index))
            if not self.dryrun:
                sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(donor))
                self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
                sampleMap[sample_id] = sampleIngest

                if sampleProtocols:
                    for sampleProtocolId in sampleProtocols:
                        self.ingest_api.linkEntity(sampleIngest, protocolMap[sampleProtocolId], "protocols")
            else:
                linksList.append("sample_" + sample_id + "-project_" + projectId)
                if sampleProtocols:
                    for sampleProtocolId in sampleProtocols:
                        linksList.append("sample_" + sample_id + "-protocol_" + sampleProtocolId)


        for index, sample in enumerate(samples):
            if "sample_id" not in sample:
                raise ValueError('Sample must have an id attribute')
            sampleMap[sample["sample_id"]] = sample
            sample_id = sample["sample_id"]

            # if "ncbi_taxon_id" not in sample:
                # Returns ValueError if donor.ncbi_taxon_id is empty
                # raise ValueError(
                #     'Field ncbi_taxon_id for sample ' + sample_id + ' is a required field and must contain a valid NCBI Taxon ID')

            if "ncbi_taxon_id" in sample and "genus_species" in sample:
                sample["genus_species"]["ontology"] = "NCBITaxon:" + str(sample["ncbi_taxon_id"])

        # add dependent information to various sample types
        for state in specimen_state:
            if "sample_id" in state:
                sampleMap[state["sample_id"]]["specimen_from_organism"]["state_of_specimen"] = state["state_of_specimen"]

        if enrichment:
            for e in enrichment:
                if "sample_id" in e:
                    if "cell_suspension" in sampleMap[e["sample_id"]] and "enrichment" in sampleMap[e["sample_id"]]["cell_suspension"]:
                        sampleMap[e["sample_id"]]["cell_suspension"]["enrichment"].append(e["enrichment"])
                    else:
                        sampleMap[e["sample_id"]]["cell_suspension"] = {}
                        sampleMap[e["sample_id"]]["cell_suspension"]["enrichment"] = [e["enrichment"]]
                else:
                    for index, sample_id in enumerate(sampleMap.keys()):
                        if "cell_suspension" in sampleMap[sample_id]:
                            if "enrichment" in sampleMap[sample_id]["cell_suspension"]:
                                sampleMap[sample_id]["cell_suspension"]["enrichment"].append(e["enrichment"])
                            else:
                                sampleMap[sample_id]["cell_suspension"]= {}
                                sampleMap[sample_id]["cell_suspension"]["enrichment"] = [e["enrichment"]]


        if well:
            for w in well:
                if "sample_id" in w:
                    if "cell_suspension" not in sampleMap[w["sample_id"]]:
                        sampleMap[w["sample_id"]]["cell_suspension"] = {}
                    sampleMap[w["sample_id"]]["cell_suspension"]["well"] = w["well"]

        # submit samples to ingest and link to project and protocols
        for index, sample_id in enumerate(sampleMap.keys()):
            if sample_id not in donorIds:
                sample = sampleMap[sample_id]
                if "derived_from" in sample:
                    if sample["derived_from"] not in sampleMap.keys():
                        raise ValueError('Sample '+ sample_id +' references another sample '+ sample["derived_from"] +' that isn\'t in the donor worksheet')
                sampleProtocols = []
                if "protocol_ids" in sample:
                    for sampleProtocolId in sample["protocol_ids"]:
                        if sampleProtocolId not in protocolMap:
                            raise ValueError('Sample ' + sample["sample_id"] + ' references a protocol ' + sampleProtocolId + ' that isn\'t in the protocol worksheet')
                    sampleProtocols = sample["protocol_ids"]
                    del sample["protocol_ids"]

                sample["core"] = {"type" : "sample",
                                  "schema_url": self.schema_url + "sample.json",
                                "schema_version": self.schema_version}

                self.dumpJsonToFile(sample, projectId, "sample_" + str(index))
                if not self.dryrun:
                    sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(sample))
                    self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
                    sampleMap[sample["sample_id"]] = sampleIngest
                    if sampleProtocols:
                        for sampleProtocolId in sampleProtocols:
                            self.ingest_api.linkEntity(sampleIngest, protocolMap[sampleProtocolId], "protocols")
                else:
                    linksList.append("sample_" + sample_id + "-project_" + projectId)
                    if sampleProtocols:
                        for sampleProtocolId in sampleProtocols:
                            linksList.append("sample_" + sample_id + "-protocol_" + sampleProtocolId)

        # create derived_from links between samples separately to make sure all samples are submitted
        for index, sample_id in enumerate(sampleMap.keys()):
            if not self.dryrun:
                if "derived_from" in sampleMap[sample_id]['content']:
                    self.ingest_api.linkEntity(sampleMap[sample_id],
                                               sampleMap[sampleMap[sample_id]['content']["derived_from"]],
                                               "derivedFromSamples")

            else:
                if "derived_from" in sampleMap[sample_id]:
                    linksList.append(
                        "sample_" + sample_id + "-derivedFromSamples_" + sampleMap[sample_id]["derived_from"])

        #build the assay map from the different types of assay infromation
        assayMap={}

        for index, s in enumerate(seq):
            if "paired_ends" in s["seq"]:
                val = s["seq"]["paired_ends"]
                if val.lower() in ["true", "yes"]:
                    s["seq"]["paired_ends"] = True
                elif val.lower() in ["false", "no"]:
                    s["seq"]["paired_ends"] = False
                # Removing ValueError for absence or incorrectly formatted paired_ends field as this should be picked up by the validator
                # else:
                #     raise ValueError(
                #         'Field paired_ends in tab seq must either contain one of yes, true, no or false')

            if "assay_id" in s:
                id = s["assay_id"]
                del s["assay_id"]
                assayMap[id]["seq"] = s["seq"]
            else:
                seqObj = s["seq"]

        for sb in seq_barcode:
            if "assay_id" in sb:
                id = sb["assay_id"]
                del sb["assay_id"]
                assayMap[id]["seq"]["umi_barcode"] = sb["umi_barcode"]
            else:
                seqObj["umi_barcode"] = sb["umi_barcode"]


        for index, sc in enumerate(single_cell):
            if "assay_id" in sc:
                id = sc["assay_id"]
                del sc["assay_id"]
                assayMap[id]["single_cell"] = sc["single_cell"]
            else:
                scObj = sc["single_cell"]

        for scb in single_cell_barcode:
            if "assay_id" in scb:
                id = scb["assay_id"]
                del scb["assay_id"]
                assayMap[id]["single_cell"]["cell_barcode"] = scb["cell_barcode"]
            else:
                scObj["cell_barcode"] = scb["cell_barcode"]

        for index, r in enumerate(rna):
            if "assay_id" in r:
                id = r["assay_id"]
                del r["assay_id"]
                assayMap[id]["rna"] = r["rna"]
            else:
                rnaObj = r["rna"]


        filesMap={}
        for index, file in enumerate(files):
            if "filename" not in file:
                raise ValueError('Files must have a name')
            if "assay_id" not in file:
                raise ValueError('Files must be linked to an assay')
            assay = file["assay_id"]
            seqFile = file["seq"]
            sample = file["sample_id"]
            del file["assay_id"]
            del file["seq"]
            del file["sample_id"]
            # self.dumpJsonToFile({"fileName" : file["filename"], "content" : file}, projectId, "files_" + str(index))
            filesMap[file["filename"]] = file


            if assay not in assayMap:
                assayMap[assay] = {}
                assayMap[assay]["files"] = []
                assayMap[assay]["rna"] = rnaObj.copy()
                assayMap[assay]["single_cell"] = scObj.copy()
                assayMap[assay]["seq"] = seqObj.copy()
                assayMap[assay]["seq"]["lanes"] = []
                assayMap[assay]["sample_id"] = sample
                assayMap[assay]["assay_id"] = assay
            elif "rna" not in assayMap[assay]:
                assayMap[assay]["rna"] = rnaObj.copy()
            elif "single_cell" not in assayMap[assay].keys():
                assayMap[assay]["single_cell"] = scObj.copy()
            elif "seq" not in assayMap[assay]:
                assayMap[assay]["seq"] = seqObj.copy()
                assayMap[assay]["seq"]["lanes"] = []
            elif "sample_id" not in assayMap[assay]:
                assayMap[assay]["sample_id"] = sample
            elif "files" not in assayMap[assay]:
                assayMap[assay]["files"] = []
            elif "assay_id" not in assayMap[assay]:
                assayMap[assay]["assay_id"] = assay


            assayMap[assay]["files"].append(file["filename"])

            if "lanes" in seqFile:
                # if there are lane numbers, base the creation of each lane object around them
                if "number" in seqFile["lanes"]:
                    added = False
                    # if there is already a lanes object, check if the current lane number
                    #  matches the lane number in the object
                    if len(assayMap[assay]["seq"]["lanes"]) > 0:
                        for lane in assayMap[assay]["seq"]["lanes"]:
                            if lane["number"] == seqFile["lanes"]["number"]:
                                if "run" in seqFile["lanes"]:
                                    run = seqFile["lanes"]["run"].lower()
                                    lane[run] = file["filename"]
                                    added = True
                    # if nothing was added as part of the above clauses, append a new lane object to the lanes array
                    if added == False:
                        if "run" in seqFile["lanes"]:
                            run = seqFile["lanes"]["run"].lower()
                            assayMap[assay]["seq"]["lanes"].append({"number": seqFile["lanes"]["number"],
                                                                    run : file["filename"]})
                # ASSUMPTION: if there are no lane numbers provide in the s/sheet, there is only one lane per assay
                else:
                    if "run" in seqFile["lanes"]:
                        run = seqFile["lanes"]["run"].lower()
                        # if the lanes object is currently empty, put a new dictionary with the run name into position [0]
                        if assayMap[assay]["seq"]["lanes"]:
                            assayMap[assay]["seq"]["lanes"][0][run] = file["filename"]
                        # if there is already an object in the lanes array for this assay, append the new run
                        else:
                            assayMap[assay]["seq"]["lanes"].append({run: file["filename"]})



            if "insdc_experiment" in seqFile:
                assayMap[assay]["seq"]["insdc_experiment"] = seqFile["insdc_experiment"]

            if "insdc_run" in seqFile:
                assayMap[assay]["seq"]["insdc_run"] = []
                assayMap[assay]["seq"]["insdc_run"].append(seqFile["insdc_run"])

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

        for index, assay in enumerate(assayMap.values()):
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
            elif assay["sample_id"] not in sampleMap:
                raise ValueError('An assay references a sample '+assay["sample_id"]+' that isn\'t in the samples worksheet')
            samples = assay["sample_id"]
            del assay["sample_id"]

            assay["core"] = {"type" : "assay",
                             "schema_url": self.schema_url + "assay.json",
                             "schema_version": self.schema_version}
            self.dumpJsonToFile(assay, projectId, "assay_" + str(index))

            if not self.dryrun:
                assayIngest = self.ingest_api.createAssay(submissionUrl, json.dumps(assay))
                self.ingest_api.linkEntity(assayIngest, projectIngest, "projects")

                if samples in sampleMap:
                    self.ingest_api.linkEntity(assayIngest, sampleMap[samples], "samples")

                for file in files:
                    self.ingest_api.linkEntity(assayIngest, filesMap[file], "files")
            else:
                linksList.append("assay_" + assay["assay_id"] + "-project_" + projectId)

                if samples in sampleMap:
                    linksList.append("assay_" + assay["assay_id"] + "-sample_" + samples)

                for file in files:
                    linksList.append("assay_" + assay["assay_id"] + "-file_" + file)

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
