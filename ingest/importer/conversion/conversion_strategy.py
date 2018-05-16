from abc import abstractmethod

from ingest.importer.data_node import DataNode


class CellConversion(object):

    @abstractmethod
    def apply(self, data_node:DataNode, cell_data): ...
