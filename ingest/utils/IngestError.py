class IngestError:
    """
    An error that can be reported to ingest.
    """

    def __init__(self):
        self.type = "http://ingest.data.humancellatlas.org/Error"
        self.title = ""
        self.detail = ""

    def getJSON(self):
        return self.__dict__


class ImporterError(IngestError):
    def __init__(self, detail=""):
        self.type = "http://importer.ingest.data.humancellatlas.org/Error"
        self.title = "An error occurred importing the file."
        self.detail = detail


class ParserError(ImporterError):
    def __init__(self, location="", type="", detail=""):
        self.type = "http://parser.importer.ingest.data.humancellatlas.org/Error"
        self.title = f'An error occurred parsing the file: {type}.'
        self.detail = f'{location}, error={detail}'


class SubmissionError(ImporterError):
    def __init__(self, detail=""):
        self.type = "http://submission.importer.ingest.data.humancellatlas.org/Error"
        self.title = "An error occurred submitting the file."
        self.detail = detail


class ExporterError(IngestError):
    def __init__(self, detail=""):
        self.type = "http://exporter.ingest.data.humancellatlas.org/Error"
        self.title = "Error occurred while attempting to export the submission."
        self.detail = detail
