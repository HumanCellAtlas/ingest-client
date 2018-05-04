class BooleanConverter(object):

    def convert(self, value):
        truth_values = ['true', 'yes']
        return True if value.lower() in truth_values else False
