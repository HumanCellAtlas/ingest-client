class WorksheetImporter:

    def __init__(self):
        pass

    def do_import(self, worksheet):
        node = {}
        for row in worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3)):
            for cell in row: self._add_to_node(node, worksheet, cell)
        return node

    def _add_to_node(self, node, worksheet, cell):
        header_coordinate = '%s1' % (cell.column)
        header_name = worksheet[header_coordinate].value
        field_chain = self._extract_field_chain(header_name)
        target_node = self._determine_node(node, field_chain)
        target_node[field_chain[-1]] = cell.value

    def _extract_field_chain(self, header_name):
        properties = header_name.split('.')
        return properties[2:]

    def _determine_node(self, node, field_chain):
        current_node = node
        for field in field_chain[:len(field_chain) - 1]:
            if field not in current_node:
                current_node[field] = {}
            current_node = current_node[field]
        return current_node