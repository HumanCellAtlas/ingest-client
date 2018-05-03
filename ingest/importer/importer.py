class MetadataMapping:

    def __init__(self):
        pass

class TabImporter:

    def __init__(self):
        pass

    def do_import(self, worksheet):
        data = {}
        for row in worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3)):
            for cell in row:
                header_coordinate = "%s1" % (cell.column)
                header_name = worksheet[header_coordinate].value

                properties = header_name.split('.')
                field_chain = properties[2:]
                current_node = data
                for field in field_chain[:len(field_chain) - 1]:
                    if field not in current_node:
                        current_node[field] = {}
                    current_node = current_node[field]

                current_node[field_chain[-1]] = cell.value
        return data