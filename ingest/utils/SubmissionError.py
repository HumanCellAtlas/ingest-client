class SubmissionError:
    """
    A SubmissionError that can be reported to ingest.
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


class ImporterError(SubmissionError):
    """
    An error importing the Submission, to be reported to ingest.
    """

    def __init__(self, detail=""):
        self.type = "http://importer.ingest.data.humancellatlas.org/Error"
        self.title = "An error occurred importing the submission."
        self.detail = detail


class ExporterError(SubmissionError):
    """
    An error exporting the Submission, to be reported to ingest.
    """

    def __init__(self, detail=""):
        self.type = "http://exporter.ingest.data.humancellatlas.org/Error"
        self.title = "Error occurred while attempting to export the submission."
        self.detail = detail
