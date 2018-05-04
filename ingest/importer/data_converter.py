class BooleanConverter(object):

    def convert(self, value):
        return True if value.lower() == 'true' else False
