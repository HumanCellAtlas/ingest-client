import string

from openpyxl import Workbook

HEADER = list(string.ascii_uppercase)


def create_worksheet(name, rows, start_row=1):
    rows = rows or []
    start_row = start_row or 1

    workbook = Workbook()
    worksheet = workbook.create_sheet(name)

    for row_idx, row in enumerate(rows):
        for col_idx, cell_value in enumerate(row):
            header = HEADER[col_idx]
            worksheet[f'{header}{start_row + row_idx}'] = cell_value

    return worksheet
