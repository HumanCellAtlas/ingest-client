import glob, json, os, urllib, requests
from openpyxl import load_workbook
from ingestapi import IngestApi
from optparse import OptionParser


# This script will read a spreadsheet, generate a manifest, submit all items to the ingest API, assign uuid and generate a directory of bundles for the
# submitted data

class SpreadsheetSubmission:
    def __init__(self, dry=False):

        self.dryrun = dry
        if not self.dryrun:
            self.ingest_api = IngestApi()

    def createSubmission(self):
        print "creating submission..."

        submissionUrl = self.ingest_api.createSubmission()
        print "new submission " + submissionUrl
        return submissionUrl

    def _keyValueToNestedObject(self, key, value):
        d = value
        if "\"" in unicode(value) or "||" in unicode(value):
            d = map(lambda it: it.strip("\""), value.split("||"))
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
                print json.dumps(obj)
                objs.append(obj)

        return objs

    # sheets that represent one entity where the properties are in column 0
    def _sheetToObject(self, type, sheet):
        obj = {}
        for row in sheet.iter_rows():
            propertyCell = row[0].value
            valueCell = row[1].value
            if valueCell:
                d = self._keyValueToNestedObject(propertyCell, valueCell)
                obj.update(d)
        print json.dumps(obj)
        return obj

    def completeSubmission(self):
        self.ingest_api.finishSubmission()

    def submit(self, pathToSpreadsheet, submissionUrl):
        try:
            self._process(pathToSpreadsheet, submissionUrl)
        except Exception, e:
            print "This is an error message!"+str(e)


    def _process(self, pathToSpreadsheet, submissionUrl):
        wb = load_workbook(filename=pathToSpreadsheet)

        projectSheet = wb.get_sheet_by_name("project")
        contactSheet = wb.get_sheet_by_name("contact")
        sampleSheet = wb.get_sheet_by_name("sample")
        donorSheet = wb.get_sheet_by_name("donor")
        protocolSheet = wb.get_sheet_by_name("protocols")
        assaySheet = wb.get_sheet_by_name("assay")

        project = self._sheetToObject("project", projectSheet)
        contact = self._sheetToObject("contact", contactSheet)
        project["contact"] = contact

        samples = self._multiRowToObjectFromSheet("sample", sampleSheet)
        protocols = self._multiRowToObjectFromSheet("protocol", protocolSheet)

        donors = self._multiRowToObjectFromSheet("donor", donorSheet)

        assays = self._multiRowToObjectFromSheet("assay", assaySheet)

        projectId = project["id"]

        dir = "./pre-ingest-example-json/" + projectId
        if not os.path.exists(dir):
            os.makedirs(dir)

        def dumpJsonToFile(object, dir, name):
            tmpFile = open(dir + "/" + name + ".json", "w")
            tmpFile.write(object)
            tmpFile.close()


        # creating submission
        #
        if not self.dryrun and not submissionUrl:
            submissionUrl = self.createSubmission()

        dumpJsonToFile(json.dumps(project), dir, "project")

        if not self.dryrun:
            self.ingest_api.createProject(submissionUrl, json.dumps(project))

        for index, sample in enumerate(samples):
            sample["protocols"] = protocols
            # sampleObj = MetadataDocument(sample)
            dumpJsonToFile(json.dumps(sample), dir, "sample_" + str(index))
            if not self.dryrun:
                self.ingest_api.createSample(submissionUrl, json.dumps(sample))

        for index, donor in enumerate(donors):
            # donorObj = MetadataDocument(donor)
            dumpJsonToFile(json.dumps(donor), dir, "donor_" + str(index))
            if not self.dryrun:
                self.ingest_api.createDonor(submissionUrl, json.dumps(donor))

        for index, assay in enumerate(assays):
            # assayObj = MetadataDocument(assay)
            dumpJsonToFile(json.dumps(assay), dir, "assay_" + str(index))
            if not self.dryrun:
                self.ingest_api.createAssay(submissionUrl, json.dumps(assay))

        print "All done!"
        wb.close()

        if not self.dryrun:
            self.ingest_api.finishedForNow(submissionUrl)
            return submissionUrl

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path",
                      help="path to HCA example data bundles", metavar="FILE")
    parser.add_option("-d", "--dry", help="doa dry run without submitting to ingest", action="store_true", default=False)

    (options, args) = parser.parse_args()
    if not options.path:
        print "You must supply path to the HCA bundles directory"
        exit(2)
    submission = SpreadsheetSubmission(options.dry)
    submission.submit(options.path)
