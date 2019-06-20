from openpyxl import Workbook


def create_test_workbook(*worksheet_titles, include_default_sheet=False):
    workbook = Workbook()
    for title in worksheet_titles:
        workbook.create_sheet(title)

    if not include_default_sheet:
        default_sheet = workbook.get_sheet_by_name('Sheet')
        workbook.remove(default_sheet)

    return workbook
