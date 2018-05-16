from abc import abstractmethod

from ingest.importer.data_node import DataNode


class CellConversion(object):

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...


class DirectCellConversion(CellConversion):

    def __init__(self, field, converter):
        pass

    def apply(self, data_node:DataNode, cell_data):
        data_node['user'] = {}
