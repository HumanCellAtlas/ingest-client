from ingest.importer.data_node import DataNode


class WorksheetImporter:

    def __init__(self):
        pass

    def do_import(self, worksheet):
        node = DataNode()
        for row in self._get_data_rows(worksheet):
            for cell in row:
                header_name = self._get_header_name(cell, worksheet)
                field_chain = self._get_field_chain(header_name)
                node[field_chain] = cell.value
        return node.as_dict()

    def _get_data_rows(self, worksheet):
        return worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3))

    def _get_header_name(self, cell, worksheet):
        header_coordinate = '%s1' % (cell.column)
        header_name = worksheet[header_coordinate].value
        return header_name

    def _get_field_chain(self, header_name):
        properties = header_name.split('.')
        return '.'.join(properties[2:])
