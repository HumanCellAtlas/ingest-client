class IngestError:
    """
    An error that can be reported to ingest.
    """

    def __init__(self):
        self.type = "http://ingest.data.humancellatlas.org/Error"
        self.title = ""
        self.detail = ""

    def getJSON(self):
        return {
            'type': self.type,
            'title': self.title,
            'detail': self.detail
        }


class ImporterError(IngestError):
    def __init__(self, detail=""):
        self.type = "http://importer.ingest.data.humancellatlas.org/Error"
        self.title = "An error occurred parsing the file."
        self.detail = detail


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
