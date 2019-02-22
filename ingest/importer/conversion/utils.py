import re

SPLIT_FIELD_PATTERN = re.compile(r'^(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)$')

FIELD_SEPARATOR = '.'


def split_field_chain(field):
    parent_path = ''
    target_field = field

    match = SPLIT_FIELD_PATTERN.search(field)
    if match:
        parent_path = match.group('parent')
        target_field = match.group('target')

    return parent_path, target_field


def slice_field_chain(field, anchor=0):
    field_nodes = field.split(FIELD_SEPARATOR)
    head = [str(node) for node in field_nodes[:anchor]]
    tail = [str(node) for node in field_nodes[anchor:len(field_nodes)]]
    return FIELD_SEPARATOR.join(head), FIELD_SEPARATOR.join(tail)


def extract_root_field(field_chain):
    root_field = None
    if field_chain is not None:
        split = field_chain.split('.')
        root_field = split[0]
    return root_field
