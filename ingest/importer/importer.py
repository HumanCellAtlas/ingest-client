class MetadataMapping:

    def __init__(self):
        pass

class TabImporter:

    def __init__(self, metadata_mapping, columns):
        self.metadata_mapping = metadata_mapping
        self.columns = columns

    def do_import(self, worksheet):
        data = {}
        for row in worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3)):
            for cell in row:
                header_coordinate = "%s1" % (cell.column)
                display_name = worksheet[header_coordinate].value
                property_name = self.metadata_mapping.get_column_mapping(display_name)
                data[property_name] = cell.value
        return { 'project_core': data }
