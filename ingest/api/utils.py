from datetime import datetime

INGEST_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DSS_VERSION_FORMAT = "%Y-%m-%dT%H%M%S.%fZ"

_expected_formats = [INGEST_DATE_FORMAT, DSS_VERSION_FORMAT]


def to_dss_version(date_str: str):
    date = parse_date_string(date_str)
    return date.strftime(DSS_VERSION_FORMAT)


def parse_date_string(date_str: str):
    for date_format in _expected_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    raise ValueError(f'unknown date format for [{date_str}]')
