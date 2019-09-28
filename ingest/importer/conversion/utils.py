import re

SPLIT_FIELD_PATTERN = re.compile(r'^(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)$')


def split_field_chain(field):
    match = SPLIT_FIELD_PATTERN.search(field)

    parent_path = match.group('parent') if match else ''
    target_field = match.group('target') if match else field

    return parent_path, target_field


def extract_root_field(field_chain):
    root_field = None
    if field_chain is not None:
        split = field_chain.split('.')
        root_field = split[0]
    return root_field
