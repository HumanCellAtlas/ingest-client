from unittest import TestCase

from ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet

import ingest.utils.spreadsheet as spreadsheet_utils


class IngestWorksheetTest(TestCase):

    def test_get_column_headers(self):
        # given:
        header_row = 4
        rows = [['name', 'address', 'mobile', 'email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', 'mobile', 'email'])

    def test_get_column_headers_trim_whitespace(self):
        # given:
        header_row = 4
        rows = [[' name ', 'address        ', ' ', ' email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', '', 'email'])

    def test_get_column_headers_includes_blank_cells(self):
        # given:
        header_row = 4
        rows = [['name', 'address', '', 'email']]
        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', '', 'email'])

    def test_get_column_headers_skip_none_cells(self):
        # given:
        header_row = 4
        rows = [['name', 'address', None, 'email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 3)
        self.assertEqual(column_headers, ['name', 'address', 'email'])

    def test_get_data_row_cells(self):
        # given:
        start_row_idx = 6
        header_row_idx = 4

        header_row = ['name', 'address', 'mobile', 'email']
        expected_data_row = ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']

        rows = [[], [], [], [], [], []]  # initialise 6 rows
        rows[header_row_idx-1] = header_row
        rows[start_row_idx-1] = expected_data_row

        worksheet = spreadsheet_utils.create_worksheet('person', rows)

        # when
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row_idx)
        data_rows = ingest_worksheet.get_data_row_cells(start_row=start_row_idx)

        data_row_values = []
        for row in data_rows:
            cell_values = [cell.value for cell in row]
            data_row_values.append(cell_values)

        # then:
        self.assertEqual(len(data_row_values), 1)
        self.assertEqual(data_row_values, [expected_data_row])

    def test_get_data_row_cells_skip_blank_rows(self):
        # given:
        start_row_idx = 6
        header_row_idx = 4

        header_row = ['name', 'address', 'mobile', 'email']
        expected_data_row = ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        blank_row = [None, None, None, None]

        rows = [[], [], [], [], [], [], []]  # initialise 7 rows
        rows[header_row_idx - 1] = header_row
        rows[start_row_idx - 1] = expected_data_row
        rows[start_row_idx] = blank_row

        worksheet = spreadsheet_utils.create_worksheet('person', rows)

        # when
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row_idx)
        data_rows = ingest_worksheet.get_data_row_cells(start_row=start_row_idx)

        data_row_values = []
        for row in data_rows:
            cell_values = [cell.value for cell in row]
            data_row_values.append(cell_values)

        # then:
        self.assertEqual(len(data_row_values), 1)
        self.assertEqual(data_row_values, [expected_data_row])
