#!/usr/bin/env python
"""
This script will read a spreadsheet, generate a manifest, submit all items to the ingest API, 
assign uuid and generate a directory of bundles for the submitted data
"""
__author__ = "jupp"
__license__ = "Apache 2.0"


import glob, json, os, urllib, requests
from openpyxl import load_workbook
from ingestapi import IngestApi
from optparse import OptionParser
import logging

from itertools import chain
from collections import defaultdict

# these are spreadsheet fields that can be a list
# todo - these should be read out of the json schema at the start
hca_v3_lists = ['seq.lanes']

v4_ontologyFields = {"donor" : ["ancestry", "development_stage", "disease", "medication", "strain"],
                  "cell_suspension" : ["target_cell_type"],
                  "death" : ["cause_of_death"],
                  "immortalized_cell_line" : ["cell_type", "disease", "cell_cycle"],
                  "protocol" : ["type"],
                  "primary_cell_line" : ["cell_type", "disease", "cell_cycle"],
                  "sample" : ["genus_species"],
                  "specimen_from_organism" : ["body_part", "organ"],
                     "project" : ["experimental_design"],
                     "organoid" : ["model_for_organ"]
                     }


v4_arrayFields = {"seq" : ["insdc_run"],
                "state_of_specimen" : ["gross_image", "microscopic_image"],
                "donor" : ["ancestry", "disease", "medication", "strain"],
                "sample" : ["supplementary_files"],
                "cell_suspension" : ["target_cell_type", "enrichment"],
                "publication" : ["authors"],
                "project" : ["supplementary_files", "experimental_design", "experimental_factor_name"]
                  }


class SpreadsheetSubmission:

    def __init__(self, dry=False, output=None):
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # logging.basicConfig(formatter=formatter, level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.dryrun = dry
        self.outputDir = output
        self.ingest_api = None
        if not self.dryrun:
            self.ingest_api = IngestApi()

    def createSubmission(self):
        self.logger.info("creating submission...")
        if not self.ingest_api:
            self.ingest_api = IngestApi()

        submissionUrl = self.ingest_api.createSubmission()
        self.logger.info("new submission " + submissionUrl)
        return submissionUrl

    # this only works for keys nested to two levels, need something smarter to handle arbitrary
    # depth keys e.g. we support <level1>.<level2> = <value>, where value is either a single value or list
    # this doesn't support <level1>.<level2>.<level3>
    def _keyValueToNestedObject(self, obj, key, value, type):
        d = value
        if "\"" in str(value) or "||" in str(value) or key in hca_v3_lists or (type in v4_arrayFields.keys() and key.split('.')[-1] in v4_arrayFields[type]):
            if "||" in str(value):
            # d = map(lambda it: it.strip(' "\''), str(value).split("||"))
                d = str(value).split("||")
            else:
                d = [value]

        if len(key.split('.')) > 3:
            raise ValueError('We don\'t support keys nested greater than 3 levels, found:'+key)

        if type in v4_ontologyFields.keys() and key.split('.')[-1] in v4_ontologyFields[type]:
            if isinstance(d, list):
                t = []
                for index, v in enumerate(d):
                    t.append({"text" : d[index]})
                d = t
            else:
                d = {"text" : d}

        for part in reversed(key.split('.')):
            d = {part: d}

        return self._mergeDict(obj, d)

    def _mergeDict(self, dict1, dict2):
        dict3 = defaultdict(list)
        for k, v in chain(dict1.items(), dict2.items()):
            if k in dict3:
                if isinstance(v, dict):
                    dict3[k].update(self._mergeDict(dict3[k], v))
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

    def submit(self, pathToSpreadsheet, submissionUrl):
        try:
            self._process(pathToSpreadsheet, submissionUrl)
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

    def _process(self, pathToSpreadsheet, submissionUrl):

        # parse the spreadsheet
        wb = load_workbook(filename=pathToSpreadsheet)
        projectSheet = wb.get_sheet_by_name("project")
        projectPubsSheet = wb.get_sheet_by_name("project.publications")
        submitterSheet = wb.get_sheet_by_name("contact.submitter")
        contributorSheet = wb.get_sheet_by_name("contact.contributors")
        specimenSheet = wb.get_sheet_by_name("sample.specimen_from_organism")
        specimenStateSheet = wb.get_sheet_by_name("state_of_specimen")
        donorSheet = wb.get_sheet_by_name("sample.donor")
        cellSuspensionSheet = wb.get_sheet_by_name("sample.cell_suspension")
        cellSuspensionEnrichmentSheet = wb.get_sheet_by_name("cell_suspension.enrichment")
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
        protocolSheet = wb.get_sheet_by_name("protocols")
        # assaySheet = wb.get_sheet_by_name("assay")
        singleCellSheet = wb.get_sheet_by_name("single_cell")
        singleCellBarcodeSheet = wb.get_sheet_by_name("single_cell.barcode")
        rnaSheet = wb.get_sheet_by_name("rna")
        seqSheet = wb.get_sheet_by_name("seq")
        seqBarcodeSheet = wb.get_sheet_by_name("seq.barcode")
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

        # samples = self._multiRowToObjectFromSheet("sample", sampleSheet)
        # assays = self._multiRowToObjectFromSheet("assay", assaySheet)
        # lanes = self._multiRowToObjectFromSheet("lanes", lanesSheet)

        protocols = self._multiRowToObjectFromSheet("protocol", protocolSheet)
        donors = self._multiRowToObjectFromSheet("donor", donorSheet)
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


        # post objects to the Ingest API after some basic validation
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


        linksList = []
        # creating submission
        #
        if not self.dryrun and not submissionUrl:
            submissionUrl = self.createSubmission()

        project["core"] = {"type": "project"}

        self.dumpJsonToFile(project, projectId, "project")

        projectIngest = None
        if not self.dryrun:
            projectIngest = self.ingest_api.createProject(submissionUrl, json.dumps(project))

        protocolMap = {}
        for index, protocol in enumerate(protocols):
            if "protocol_id" not in protocol:
                raise ValueError('Protocol must have an id attribute')

            protocol["core"] = {"type": "protocol"}
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

        for index, donor in enumerate(donors):
            if "sample_id" not in donor:
                raise ValueError('Sample of type donor must have an id attribute')
            sample_id = donor["sample_id"]

            if "is_living" in donor:
                if str.lower(donor["is_living"]) == "true" or str.lower(donor["is_living"]) == "yes":
                    donor["is_living"] = True
                elif str.lower(donor["is_living"]) == "false" or str.lower(donor["is_living"]) == "no":
                    donor["is_living"] = False
                else:
                    raise ValueError('Field is_living in sample ' + sample_id + ' must either contain one of yes, true, no or false')

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

            donor["core"] = {"type" : "sample"}

            self.dumpJsonToFile(donor, projectId, "sample_" + str(index))
            if not self.dryrun:
                sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(donor))
                self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
                sampleMap[sample_id] = sampleIngest

                if sampleProtocols:
                    for sampleProtocolId in sampleProtocols:
                        self.ingest_api.linkEntity(sampleIngest, protocolMap[sampleProtocolId], "protocols")
            else:
                 if sampleProtocols:
                    for sampleProtocolId in sampleProtocols:
                        linksList.append("sample_" + sample_id + "-protocol_" + sampleProtocolId)


        for index, sample in enumerate(samples):
            if "sample_id" not in sample:
                raise ValueError('Sample must have an id attribute')
            sampleMap[sample["sample_id"]] = sample

        # add dependent information to various sample types
        for state in specimen_state:
            if "sample_id" in state:
                sampleMap[state["sample_id"]]["specimen_from_organism"]["state_of_specimen"] = state["state_of_specimen"]

        for e in enrichment:
            if "sample_id" in e:
                if "enrichment" in sampleMap[state["sample_id"]]["cell_suspension"]:
                    sampleMap[state["sample_id"]]["cell_suspension"]["enrichment"].append(e["enrichment"])
                else:
                    sampleMap[state["sample_id"]]["cell_suspension"]["enrichment"] = [e["enrichment"]]

        for w in well:
            if "sample_id" in w:
                sampleMap[state["sample_id"]]["cell_suspension"]["well"] = w["well"]

        # create derived_from links between samples
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

                sample["core"] = {"type" : "sample"}

                self.dumpJsonToFile(sample, projectId, "sample_" + str(index))
                if not self.dryrun:
                    sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(sample))
                    self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
                    sampleMap[sample["sample_id"]] = sampleIngest

                    if "derived_from" in sampleMap[sample_id]['content']:
                        self.ingest_api.linkEntity(sampleMap[sample_id], sampleMap[sampleMap[sample_id]['content']["derived_from"]], "derivedFromSamples")

                    if sampleProtocols:
                        for sampleProtocolId in sampleProtocols:
                            self.ingest_api.linkEntity(sampleIngest, protocolMap[sampleProtocolId], "protocols")
                else:
                    if "derived_from" in sampleMap[sample_id]:
                        linksList.append("sample_" + sample_id + "-derivedFromSamples_" + sampleMap[sample_id]["derived_from"])

                    if sampleProtocols:
                        for sampleProtocolId in sampleProtocols:
                            linksList.append("sample_" + sample_id + "-protocol_" + sampleProtocolId)


        #build the assay map from the different types of assay infromation
        assayMap={}

        for index, s in enumerate(seq):
            if "paired_ends" in s["seq"]:
                val = s["seq"]["paired_ends"]
                if val.lower() in ["true", "yes"]:
                    s["seq"]["paired_ends"] = True
                elif val.lower() in ["false", "no"]:
                    s["seq"]["paired_ends"] = False
                else:
                    raise ValueError(
                        'Field paired_ends in tab seq must either contain one of yes, true, no or false')

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
                assayMap[id]["single_cell"]["barcode"] = scb["barcode"]
            else:
                scObj["barcode"] = scb["barcode"]

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
                if "number" in seqFile["lanes"]:
                    added = False
                    if len(assayMap[assay]["seq"]["lanes"]) > 0:
                        for lane in assayMap[assay]["seq"]["lanes"]:
                            if lane["number"] == seqFile["lanes"]["number"]:
                                if "run" in seqFile["lanes"]:
                                    lane[seqFile["lanes"]["run"]] = file["filename"]
                                    added = True
                    if added == False:
                        if "run" in seqFile["lanes"]:
                            assayMap[assay]["seq"]["lanes"].append({"number": seqFile["lanes"]["number"],
                                                                    seqFile["lanes"]["run"] : file["filename"]})
                else:
                    if "run" in seqFile["lanes"]:
                        assayMap[assay]["seq"]["lanes"].append({seqFile["lanes"]["run"]: file["filename"]})



            if "insdc_experiment" in seqFile:
                assayMap[assay]["seq"]["insdc_experiment"] = seqFile["insdc_experiment"]

            if "insdc_run" in seqFile:
                assayMap[assay]["seq"]["insdc_run"] = seqFile["insdc_run"]

            file["core"] = {"type" : "file"}

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

            assay["core"] = {"type" : "assay"}
            self.dumpJsonToFile(assay, projectId, "assay_" + str(index))

            if not self.dryrun:
                assayIngest = self.ingest_api.createAssay(submissionUrl, json.dumps(assay))
                self.ingest_api.linkEntity(assayIngest, projectIngest, "projects")

                if samples in sampleMap:
                    self.ingest_api.linkEntity(assayIngest, sampleMap[samples], "samples")

                for file in files:
                    self.ingest_api.linkEntity(assayIngest, filesMap[file], "files")
            else:
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

    (options, args) = parser.parse_args()
    if not options.path:
        print ("You must supply path to the HCA bundles directory")
        exit(2)
    submission = SpreadsheetSubmission(options.dry, options.output)
    submission.submit(options.path, None)
