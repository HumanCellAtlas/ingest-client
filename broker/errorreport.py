class ErrorReport:
    """
    A user friendly error message, along with corresponding ValidationError
    """
    def __init__(self, message="", validation_error=None, error_type=None):
        self.message = message
        self.validation_error = validation_error
        self.error_type = error_type

    def to_dict(self):
        error_report_dict = dict()

        error_report_dict["user_friendly_message"] = self.message

        error_report_dict["validation_error"] = dict()

        if self.error_type == "schema validation":
            error_report_dict["validation_error"]["absolute_path"] = list(self.validation_error.absolute_path)
            error_report_dict["validation_error"]["path"] = list(self.validation_error.path)
            error_report_dict["validation_error"]["message"] = self.validation_error.message
            error_report_dict["validation_error"]["instance"] = self.validation_error.instance
            error_report_dict["validation_error"]["schema_path"] = list(self.validation_error.schema_path)
            error_report_dict["validation_error"]["absolute_schema_path"] = list(self.validation_error.absolute_schema_path)
            error_report_dict["validation_error"]["validator"] = self.validation_error.validator
            error_report_dict["validation_error"]["validator_value"] = self.validation_error.validator_value
        else:
            error_report_dict["validation_error"]["absolute_path"] = self.validation_error.absolute_path
            error_report_dict["validation_error"]["path"] = self.validation_error.path
            error_report_dict["validation_error"]["message"] = self.validation_error.message
            error_report_dict["validation_error"]["instance"] = self.validation_error.instance

        return error_report_dict
