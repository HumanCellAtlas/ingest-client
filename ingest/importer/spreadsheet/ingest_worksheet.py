import re

from xlsxwriter.worksheet import Worksheet

MODULE_TITLE_PATTERN = re.compile(r'^(?P<main_label>\w+( \w+)*)( - (?P<field_name>\w+([ -]\w+)*))?')

HEADER_ROW_IDX = 4
START_DATA_ROW = 6


class IngestWorksheet(object):

    def __init__(self, worksheet: Worksheet, header_row_idx=HEADER_ROW_IDX):
        self._worksheet = worksheet
        self._header_row_idx = header_row_idx

    @staticmethod
    def is_empty(row):
        return all(cell.value is None for cell in row)

    @property
    def title(self):
        return self._worksheet.title

    def get_column_headers(self):
        rows = self._worksheet.iter_rows(min_row=self._header_row_idx, max_row=self._header_row_idx)
        header_row = next(rows)

        headers = []
        for cell in header_row:
            if cell.value is None:
                continue

            cell_value = cell.value.strip()
            headers.append(cell_value)

        return headers

    def get_data_rows(self, start_row=START_DATA_ROW, end_row=None):
        headers = self.get_column_headers()
        max_row = end_row or self.compute_max_row()
        rows = self._worksheet.iter_rows(min_row=start_row, max_row=max_row)
        rows = [row[:len(headers)] for row in rows if not self.is_empty(row)]
        rows = [IngestRow(self._worksheet.title, START_DATA_ROW + index, row) for index, row in enumerate(rows)]
        return rows

    # NOTE: there are no tests around this because it's too complicated to
    # setup the scenario where the worksheet returns an erroneous max_row value
    def compute_max_row(self):
        max_row = self._worksheet.max_row
        if max_row is None:
            self._worksheet.calculate_dimension(force=True)
            max_row = self._worksheet.max_row
        return max_row

    def is_module_tab(self):
        match = MODULE_TITLE_PATTERN.match(self.title)
        return bool(match and match.group('field_name'))

    def get_module_field_name(self):
        match = MODULE_TITLE_PATTERN.match(self.title)
        field_name = match.group('field_name')
        if field_name:
            field_name = re.sub('[\s-]', '_', field_name.lower())
        return field_name

    def insert_column_with_header(self, header, col_idx):
        self._worksheet.insert_cols(col_idx)
        self._worksheet.cell(row=self._header_row_idx, column=col_idx).value = header

    def cell(self, row, column):
        return self._worksheet.cell(row=row, column=column)


class IngestRow(object):
    def __init__(self, worksheet_title, index, values):
        self.values = values or []
        self.index = index or None  # starts at 1
        self.worksheet_title = worksheet_title or None
