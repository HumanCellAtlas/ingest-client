#!/usr/bin/env python
"""
Read a bundles directory and turn data into a normalised Excel spreadsheet with multiple worksheets
"""
__author__ = "jupp"
__license__ = "Apache 2.0"


import glob, json
import xlsxwriter

from optparse import OptionParser

class Bundle2Xls:
    def __init__(self, path):

        self.path = path

        # some globals for caching entities as we find them
        self.project = {}
        self.contact = {}
        self.protocols = []
        self.assays = []
        self.manifest = {}
        self.donors = {}
        self.files = []
        self.cell = {}
        self.samples = []
        self.types = {}

        self.run()

    # function to flatten JSON trees into flat key-value pairs
    # list values are flattened into a single cell using || and quoted separated values
    def flatten (self, raw):
        obj = {}

        for key, value in raw.iteritems():
            vt = type(value)
            self.types[vt] = 1

            if key == "uuid":
                continue
            elif key == "contact":
                self.contact = value
            elif key == "protocols":
                for row in value:
                    protObj = {}
                    for key2, value2 in row.iteritems():
                        protObj[key2] = value2
                    seen = False
                    for seenProts in self.protocols:
                        if seenProts["type"] == protObj["type"]:
                            seen = True
                    if not seen:
                        self.protocols.append(protObj)
            elif key == "donor":
                self.donors[raw["donor"]["uuid"]] = value
                del self.donors[raw["donor"]["uuid"]]["uuid"]
                obj["donor_id"] = raw["donor"]["id"]
            elif key == "files":
                tmpFileList = []
                for file in value:
                    if "name" in file:
                        # hack for E-MTAB-5061 as we want just file name for URL
                        fileName = file["name"].rsplit('/', 1)[-1]
                        tmpFileList.append(fileName)
                        file["name"] = fileName
                    self.files.append(file)
                obj["files"] = tmpFileList
                # for row in value:
                #     for key2, value2 in row.iteritems():
                #         obj[key+"."+key2] = value2
            elif isinstance(value, unicode) or isinstance(value, float):
                obj[key] = value
            elif isinstance(value, dict):
                for key1, value1 in value.iteritems():
                    obj[key + "." + key1] = value1
            elif isinstance(value, list):
                obj[key]= value
        return obj

    # create a worksheet from a map of key/values where each key is a row
    def writeSingleEntityWorksheet(self, workbook, sheetName, dict):
        row = 0
        worksheet = workbook.add_worksheet(sheetName)
        for key, value in dict.iteritems():
            worksheet.write(row, 0, key)
            entry = value
            if isinstance(value, list):
                entry = "||".join(['"%s"' % w for w in value])
            worksheet.write(row, 1, entry)
            row += 1
            print(sheetName + "." + key + "\t" + str(value))

    # create a worksheet from a list of map of key/values where each key is a column heading
    def writeMultiEntityWorksheet(self, workbook, sheetName, items):
        row = 1
        col = 0
        sheet = workbook.add_worksheet(sheetName)

        init = True
        for item in items:
            for key, value in item.iteritems():
                if init:
                    sheet.write(0, col, key)
                entry = value
                if isinstance(value, list):
                    entry = ",".join(['"%s"' % w for w in value])
                sheet.write(row, col, entry)
                col += 1
                print(sheetName + "." + key + "\t" + str(value))
            init = False
            row += 1
            col = 0

    def run(self):

        for dir in glob.glob(self.path+"/bundles/bundle*"):

            projectRaw = json.load(open (dir+"/project.json"))
            sampleRaw = json.load(open (dir+"/sample.json"))
            assayRaw = json.load(open (dir+"/assay.json"))
            assayRaw["sample_id"]= sampleRaw["id"]

            if "id" not in assayRaw and "ena_run" in assayRaw:
                assayRaw["id"] = assayRaw["ena_run"]

            tp = self.flatten(projectRaw)
            if not self.project:
                if not cmp(tp, self.project):
                    print ("Warn, projects aren't equal!")
                else:
                    self.project = tp
            self.assays.append(self.flatten(assayRaw))
            self.samples.append(self.flatten(sampleRaw))
        workbook = xlsxwriter.Workbook(self.project["id"] + ".xlsx")

        self.writeSingleEntityWorksheet(workbook, "project", self.project)
        self.writeSingleEntityWorksheet(workbook, "contact", self.contact)
        self.writeMultiEntityWorksheet(workbook, "sample", self.samples)
        self.writeMultiEntityWorksheet(workbook, "donor", list(self.donors.values()))
        self.writeMultiEntityWorksheet(workbook, "protocols", self.protocols)
        self.writeMultiEntityWorksheet(workbook, "assay", self.assays)
        self.writeMultiEntityWorksheet(workbook, "files", self.files)

        workbook.close()

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path",
                      help="path to HCA example data bundles", metavar="FILE")

    (options, args) = parser.parse_args()
    if not options.path:
        print ("You must supply path to the HCA bundles directory")
        exit(2)
    Bundle2Xls(options.path)