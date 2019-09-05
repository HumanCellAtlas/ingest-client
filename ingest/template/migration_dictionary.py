class MigrationDictionary():
    def __init__(self):
        self.migration_dictionary = {}

    def to_dict(self):
        return self.migration_dictionary

    def put(self, fully_qualified_key, migration_value):
        divided_path_of_key = fully_qualified_key.split(".")
        self.put_into_dictionary(divided_path_of_key, migration_value, self.migration_dictionary)

    def get(self, fully_qualified_key):
        # TODO(maniarathi): Implement this
        pass

    def put_into_dictionary(self, key_list, value, dictionary):
        if not len(key_list):
            raise Exception(f"No key provided when attempting to place value {value} into dictionary!")

        if len(key_list) == 1:
            key = key_list[0]
            if key in dictionary.keys():
                raise Exception(f"""Unexpectedly got duplicate values for key {key}. Existing value is {dictionary[
                    key]} and new value is {value}.""")
            else:
                dictionary[key] = value
            return dictionary

        primary_key = key_list[0]
        if primary_key in dictionary.keys():
            self.put_into_dictionary(key_list[1:], value, dictionary[primary_key])
        else:
            dictionary[key_list[0]] = self.generate_new_dictionary_from_nested_keys_and_value(key_list, value)
        return dictionary

    def generate_new_dictionary_from_nested_keys_and_value(self, nested_keys_list, value):
        """ Generate a new nested dictionary using all the keys in the list except the first (the first key will be
        used to reference the generated dictionary from this function in whatever top level dictionary exists before
        this function was called.

        Example input: nested_keys_list = ['cell_suspension', 'biomaterial_core', 'biosd_biomaterial'],
                       value = {
                           'replaced_by': 'cell_suspension.biomaterial_core.biosamples_accession',
                           'version': '12.0.0'
                       }
        Output: {'biomaterial_core' :
                    { 'biosd_biomaterial' :
                        {
                            'replaced_by': 'cell_suspension.biomaterial_core.biosamples_accession',
                            'version': '12.0.0'
                        }
                    }
                }
        """
        new_nested_dictionary = {nested_keys_list[-1]: value}
        for key in reversed(nested_keys_list[1:-1]):
            new_nested_dictionary = {key: new_nested_dictionary}
        return new_nested_dictionary

    def get_from_dictionary(self, key_list, dictionary):
        if not key_list:
            raise Exception(f"No key provided to lookup from dictionary.")

        if len(key_list) == 1:
            key = key_list[0]
            if key not in dictionary.keys():
                return None
            return dictionary[key]

        else:
            primary_key = key_list[0]
            if primary_key in dictionary.keys():
                return self.get_from_dictionary(key_list[1:], dictionary[primary_key])
            else:
                return None
