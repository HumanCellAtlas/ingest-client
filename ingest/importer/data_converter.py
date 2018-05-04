VALUE_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class BooleanConverter(object):

    def convert(self, value):
        return VALUE_TABLE.get(value.lower())
