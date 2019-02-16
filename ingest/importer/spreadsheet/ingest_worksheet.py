from xlsxwriter.worksheet import Worksheet

HEADER_ROW_IDX = 4


class IngestWorksheet(object):
    def __init__(self, worksheet: Worksheet, header_row_idx=1):
        self.worksheet = worksheet
        self.header_row_idx = header_row_idx
        self.title = worksheet.title

    def get_column_headers(self):
        rows = self.worksheet.iter_rows(min_row=self.header_row_idx, max_row=self.header_row_idx)
        header_row = next(rows)

        headers = []
        for cell in header_row:
            if cell.value is None:
                continue

            cell_value = cell.value.strip()
            headers.append(cell_value)

        return headers

    def get_data_row_cells(self, start_row=1, end_row=None):
        header_row = self.get_column_headers()
        max_row = end_row or self.compute_max_row()
        rows = self.worksheet.iter_rows(min_row=start_row, max_row=max_row)
        return [row[:len(header_row)] for row in rows if not self._is_empty_row(row)]

    # NOTE: there are no tests around this because it's too complicated to setup the
    # scenario where the worksheet returns an erroneous max_row value.
    def compute_max_row(self):
        max_row = self.worksheet.max_row
        if max_row is None:
            self.worksheet.calculate_dimension(force=True)
            max_row = self.worksheet.max_row
        return max_row

    @staticmethod
    def _is_empty_row(row):
        return all(cell.value is None for cell in row)
