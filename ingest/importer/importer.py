class MetadataMapping:

    def __init__(self):
        pass

class TabImporter:

    def __init__(self, metadata_mapping, columns):
        self.metadata_mapping = metadata_mapping
        self.columns = columns

    def do_import(self, worksheet):
        data = {}
        for column in self.columns:
            column_name, display_name = self.metadata_mapping.get_column_mapping(column)
            for cell in list(worksheet.iter_rows())[0]:
                if cell.value == display_name:
                    coordinate = "%s4" % (cell.column)
                    data[column_name] = worksheet[coordinate].value
        return { 'project_core': data }
