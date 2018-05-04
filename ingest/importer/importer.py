import re

from ingest.importer.data_node import DataNode


class WorksheetImporter:

    def __init__(self):
        pass

    def do_import(self, worksheet, template_manager):
        node = DataNode()
        for row in self._get_data_rows(worksheet):
            for cell in row:
                header_name = self._get_header_name(cell, worksheet)
                field_chain = self._get_field_chain(header_name)

                converter = template_manager.get_converter(header_name)

                data = converter.convert(cell.value)

                node[field_chain] = data

        return node.as_dict()

    def _get_data_rows(self, worksheet):
        return worksheet.iter_rows(row_offset=3, max_row=(worksheet.max_row - 3))

    def _get_header_name(self, cell, worksheet):
        header_coordinate = '%s1' % (cell.column)
        return worksheet[header_coordinate].value

    def _get_field_chain(self, header_name):
        match = re.search('(\w+\.){2}(?P<field_chain>.*)', header_name)
        return match.group('field_chain')


class TemplateManager(object):
    pass
