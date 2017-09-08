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

class SpreadsheetSubmission:

    def __init__(self, dry=False, output=None):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter, level=logging.INFO)
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

    def _keyValueToNestedObject(self, key, value):
        d = value
        if "\"" in unicode(value) or "||" in unicode(value):
            d = map(lambda it: it.strip(' "\''), value.split("||"))
        for part in reversed(key.split('.')):
            d = {part: d}
        return d

    def _multiRowToObjectFromSheet(self, type, sheet):
        objs = []
        for row in sheet.iter_rows(row_offset=1, max_row=(sheet.max_row - 1)):

            obj = {}
            hasData = False
            for cell in row:
                if not cell.value:
                    continue
                hasData = True
                cellCol = cell.col_idx
                propertyValue = sheet.cell(row=1, column=cellCol).value
                d = self._keyValueToNestedObject(propertyValue, cell.value)
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
                    d = self._keyValueToNestedObject(propertyCell, valueCell)
                    obj.update(d)
        self.logger.debug(json.dumps(obj))
        return obj

    def completeSubmission(self):
        self.ingest_api.finishSubmission()

    def submit(self, pathToSpreadsheet, submissionUrl):
        try:
            self._process(pathToSpreadsheet, submissionUrl)
        except Exception, e:
            self.logger.error("Error:"+str(e))
            return e

    def dumpJsonToFile(self, object, projectId, name):
        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + projectId+"_"+name + ".json", "w")
            tmpFile.write(object)
            tmpFile.close()

    def _process(self, pathToSpreadsheet, submissionUrl):

        # parse the spreadsheet
        wb = load_workbook(filename=pathToSpreadsheet)
        projectSheet = wb.get_sheet_by_name("project")
        contactSheet = wb.get_sheet_by_name("contact")
        sampleSheet = wb.get_sheet_by_name("sample")
        donorSheet = wb.get_sheet_by_name("donor")
        protocolSheet = wb.get_sheet_by_name("protocols")
        assaySheet = wb.get_sheet_by_name("assay")
        filesSheet = wb.get_sheet_by_name("files")

        # convert data in sheets back into dict
        project = self._sheetToObject("project", projectSheet)
        contact = self._sheetToObject("contact", contactSheet)
        # embedd contact into into project for now
        project["contact"] = contact

        samples = self._multiRowToObjectFromSheet("sample", sampleSheet)
        protocols = self._multiRowToObjectFromSheet("protocol", protocolSheet)
        donors = self._multiRowToObjectFromSheet("donor", donorSheet)
        assays = self._multiRowToObjectFromSheet("assay", assaySheet)
        files = self._multiRowToObjectFromSheet("files", filesSheet)

        # post objects to the Ingest API after some basic validation

        if "id" not in project:
            raise ValueError('Project must have an id attribute')
        projectId = project["id"]

        # creating submission
        #
        if not self.dryrun and not submissionUrl:
            submissionUrl = self.createSubmission()

        self.dumpJsonToFile(json.dumps(project), projectId, "project")

        projectIngest = None
        if not self.dryrun:
            projectIngest = self.ingest_api.createProject(submissionUrl, json.dumps(project))

        donorMap = {}
        for index, donor in enumerate(donors):
            self.dumpJsonToFile(json.dumps(donor), projectId, "donor_" + str(index))
            if "id" not in donor:
                raise ValueError('Donor must have an id attribute')
            donorMap[donor["id"]] = donor
            if not self.dryrun:
                donorIngest = self.ingest_api.createDonor(submissionUrl, json.dumps(donor))
                self.ingest_api.linkEntity(donorIngest, projectIngest, "projects")
                donorMap[donor["id"]] = donorIngest

        # sample id to created object
        sampleMap = {}
        for index, sample in enumerate(samples):
            sample["protocols"] = protocols
            # sampleObj = MetadataDocument(sample)
            self.dumpJsonToFile(json.dumps(sample), projectId, "sample_" + str(index))
            if "id" not in sample:
                raise ValueError('Samples must have an id attribute')
            sampleMap[sample["id"]] = sample
            if "donor_id" in sample:
                if sample["donor_id"] not in donorMap:
                    raise ValueError('Sample '+sample["id"]+' references a donor '+sample["donor_id"]+' that isn\'t in the donor worksheet')
            if not self.dryrun:
                sampleIngest = self.ingest_api.createSample(submissionUrl, json.dumps(sample))
                self.ingest_api.linkEntity(sampleIngest, projectIngest, "projects")
                sampleMap[sample["id"]] = sampleIngest
                if "donor_id" in sample:
                    if sample["donor_id"] in donorMap:
                        self.ingest_api.linkEntity(sampleIngest, donorMap[sample["donor_id"]], "derivedFromSamples")

        filesMap={}
        for index, file in enumerate(files):
            if "name" not in file:
                raise ValueError('Files must have a name')
            self.dumpJsonToFile(json.dumps({"fileName" : file["name"], "content" : file}), projectId, "files_" + str(index))
            filesMap[file["name"]] = file
            if not self.dryrun:
                fileIngest = self.ingest_api.createFile(submissionUrl, file["name"], json.dumps(file))
                filesMap[file["name"]] = fileIngest

        for index, assay in enumerate(assays):
            # assayObj = MetadataDocument(assay)
            self.dumpJsonToFile(json.dumps(assay), projectId, "assay_" + str(index))
            if "id" not in assay:
                raise ValueError('Each assays must have an id attribute')
            if "files" not in assay:
                raise ValueError('Each assay must list associated files using the files attribute')
            else:
                for file in assay["files"]:
                    if file not in filesMap:
                        raise ValueError('Assay references file '+file+' that isn\'t defined in the files sheet')
            if "sample_id" not in assay:
                raise ValueError("Every assay must reference a sample using the sample_id attribute")
            elif assay["sample_id"] not in sampleMap:
                raise ValueError('An assay references a sample '+assay["sample_id"]+' that isn\'t in the samples worksheet')

            if not self.dryrun:
                assayIngest = self.ingest_api.createAssay(submissionUrl, json.dumps(assay))
                self.ingest_api.linkEntity(assayIngest, projectIngest, "projects")

                if assay["sample_id"] in sampleMap:
                    self.ingest_api.linkEntity(assayIngest, sampleMap[assay["sample_id"]], "samples")

                for file in assay["files"]:
                    self.ingest_api.linkEntity(assayIngest, filesMap[file], "files")



        self.logger.info("All done!")
        wb.close()

        if not self.dryrun:
            self.ingest_api.finishedForNow(submissionUrl)
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
        print "You must supply path to the HCA bundles directory"
        exit(2)
    submission = SpreadsheetSubmission(options.dry, options.output)
    submission.submit(options.path, None)
