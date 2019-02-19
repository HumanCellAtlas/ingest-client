from unittest import TestCase

import ingest.utils.spreadsheet as spreadsheet_utils


class SpreadsheetTest(TestCase):
    def test_create_worksheet(self):
        # given:
        rows = [
            ['harry', 'potter', 'gryffindor', 'lily', 'james'],
            ['ron', 'weasley', 'gryffindor', 'molly', 'arthur'],
            ['draco', 'malfoy', 'slytherin', 'narcissa', 'lucius'],
            ['dudley', 'dursely', None, 'petunia', 'vernon']
        ]
        # when:
        worksheet = spreadsheet_utils.create_worksheet('ws', rows)

        # then:
        ws_rows = list(worksheet.iter_rows(min_row=1, max_row=worksheet.max_row))

        self.assertEqual(4, len(ws_rows))
        self.assertEqual(rows[0], self.get_row_values(ws_rows[0]))
        self.assertEqual(rows[1], self.get_row_values(ws_rows[1]))
        self.assertEqual(rows[2], self.get_row_values(ws_rows[2]))
        self.assertEqual(rows[3], self.get_row_values(ws_rows[3]))

    @staticmethod
    def get_row_values(row):
        values = []
        for cell in row:
            values.append(cell.value)

        return values
