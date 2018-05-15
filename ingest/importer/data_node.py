import copy


class DataNode:

    def __init__(self):
        self.node = {}

    def __setitem__(self, key, value):
        field_chain = key.split('.')
        target_node = self._determine_node(field_chain)
        target_node[field_chain[-1]] = value
        pass

    def _determine_node(self, field_chain):
        current_node = self.node
        for field in field_chain[:len(field_chain) - 1]:
            if field not in current_node:
                current_node[field] = {}
            current_node = current_node[field]
        return current_node

    def as_dict(self):
        return copy.deepcopy(self.node)