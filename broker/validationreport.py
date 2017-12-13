class ValidationReport:
    def __init__(self, validation_state="", error_reports=list()):
        self.validation_state = validation_state
        self.error_reports = error_reports  # list of ErrorReport

    def errors_to_dict(self):
        return [error.to_dict() for error in self.error_reports]

    @staticmethod
    def validation_report_ok():
        report = ValidationReport()
        report.validation_state = "VALID"
        return report
