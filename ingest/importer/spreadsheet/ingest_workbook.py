from openpyxl import Workbook

SCHEMAS_WORKSHEET = 'Schemas'
PROJECT_WORKSHEET = 'Project'
CONTACT_WORKSHEET = 'Contact'
FUNDER_WORKSHEET = 'Funder'
PUBLICATION_WORKSHEET = 'Publications'

# TODO think of a better name
SPECIAL_TABS = [SCHEMAS_WORKSHEET, PROJECT_WORKSHEET]

# TODO figure out how to identify that a tab is a module tab to generate this config automatically
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
        importable_names = [name for name in self.workbook.get_sheet_names() if
                            (not name in SPECIAL_TABS and not name in list(MODULE_TABS.keys()))]
        return [self.workbook[name] for name in importable_names]

    def module_worksheets(self):
        return [self.get_worksheet(name) for name in list(MODULE_TABS.keys())]

    def get_module_field(self, module_tab_name):
        return MODULE_TABS[module_tab_name]['field'] if MODULE_TABS.get(module_tab_name) and \
                                                        MODULE_TABS[module_tab_name].get('field') else None

