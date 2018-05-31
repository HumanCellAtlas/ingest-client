import re

SPLIT_FIELD_REGEX = '(?P<parent>\w*(\.\w*)*)\.(?P<target>\w*)'


def split_field_chain(field):
    parent_path = ''
    target_field = field

    match = re.search(SPLIT_FIELD_REGEX, field)
    if match:
        parent_path = match.group('parent')
        target_field = match.group('target')

    return parent_path, target_field


def extract_root_field(field_chain):
    root_field = None
    if field_chain is not None:
        split = field_chain.split('.')
        root_field = split[0]
    return root_field
