import re

from openpyxl import Workbook

# TODO clean this up #module-tab
SCHEMAS_WORKSHEET = 'Schemas'
PROJECT_WORKSHEET = 'Project'
CONTACT_WORKSHEET = 'Contact'
FUNDER_WORKSHEET = 'Funder'
PUBLICATION_WORKSHEET = 'Publications'

# TODO think of a better name
SPECIAL_TABS = [SCHEMAS_WORKSHEET]

# TODO remove this #module-tab
MODULE_TABS = {
    'Contact': {
        'field': 'contributors',
        'parent_entity': 'project'
    },
    'Funder': {
        'field': 'funders',
        'parent_entity': 'project'
    },
    'Publications': {
        'field': 'publications',
        'parent_entity': 'project'
    },
}

class IngestWorkbook:

    def __init__(self, workbook: Workbook):
        self.workbook = workbook

    # TODO deprecate and remove #module-tab
    def get_project_worksheet(self):
        return self.get_worksheet(PROJECT_WORKSHEET)

    def get_worksheet(self, worksheet_title):
        if worksheet_title in self.workbook.get_sheet_names():
            return self.workbook[worksheet_title]

        if worksheet_title.lower() in self.workbook.get_sheet_names():
            return self.workbook[worksheet_title.lower()]

        return None

    def get_schemas(self):
        schemas = []

        worksheet = None

        if SCHEMAS_WORKSHEET in self.workbook.get_sheet_names():
            worksheet = self.workbook.get_sheet_by_name(SCHEMAS_WORKSHEET)

        if SCHEMAS_WORKSHEET.lower() in self.workbook.get_sheet_names():
            worksheet = self.workbook.get_sheet_by_name(SCHEMAS_WORKSHEET.lower())

        if not worksheet:
            return schemas

        for row in worksheet.iter_rows(row_offset=1, max_row=(worksheet.max_row - 1)):
            schema_cell = row[0]
            schemas.append(schema_cell.value)
        return schemas

    def importable_worksheets(self):
        return [IngestWorksheet(worksheet) for worksheet in self.workbook.worksheets
                if worksheet.title not in SPECIAL_TABS]

    def module_worksheets(self):
        return [self.get_worksheet(name) for name in list(MODULE_TABS.keys())]

    def get_module_field(self, module_tab_name):
        return MODULE_TABS[module_tab_name]['field'] if MODULE_TABS.get(module_tab_name) and \
                                                        MODULE_TABS[module_tab_name].get('field')\
                                                        else None


MODULE_TITLE_PATTERN = re.compile(r'^(?P<main_label>\w+( \w+)*)( - (?P<field_name>\w+))?')


class IngestWorksheet:

    def __init__(self, worksheet):
        self._worksheet = worksheet

    @property
    def title(self):
        return self._worksheet.title

    def source(self):
        """
        This method was created to retrofit this new IngestWorksheet framework with the original
        WorksheetImporter. Avoid using this method in new code as much as possible.

        Moving forward, the intention is for IngestWorksheet to meld with
        WorksheetImporter in a way that IngestWorksheet takes care of importing itself.

        :return: the internal Openpyxl Worksheet.
        """
        return self._worksheet

    def is_module_tab(self):
        match = MODULE_TITLE_PATTERN.match(self._worksheet.title)
        return bool(match and match.group('field_name'))
