class RootSchemaException(Exception):
    """ This Exception will be used with respect to errors that occur when generating a template based on a root JSON
    object.
    """
    pass


class UnknownKeySchemaException(Exception):
    """ This Exception will be used when a key representing a field in a metadata schema cannot be mapped to a known
    property.
    """
    pass
