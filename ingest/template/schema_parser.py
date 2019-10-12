import json

import jsonref

from .descriptor import ComplexPropertyDescriptor


class SchemaParser():
    def __init__(self, json_schema,
                 ignored_properties=["required_properties", "describedBy", "schema_version", "schema_type",
                                     "provenance"]):
        self.ignored_properties = ignored_properties
        self.schema_descriptor = json_schema
        self.schema_dictionary = self.schema_descriptor

    @property
    def schema_descriptor(self):
        return self._schema_descriptor

    @schema_descriptor.setter
    def schema_descriptor(self, json_schema):
        """
        Given a json-formatted metadata schema, loads it into a Descriptor class which captures the structure as a
        dictionary and stores it as a private variable.

        :param json_schema: A raw metadata schema JSON object.
        """

        # Use jsonref to resolve all $refs in JSON
        metadata_schema_data = jsonref.loads(json.dumps(json_schema))

        self._schema_descriptor = ComplexPropertyDescriptor(metadata_schema_data)

    @property
    def schema_dictionary(self):
        return self._schema_dictionary

    @schema_dictionary.setter
    def schema_dictionary(self, descriptor):
        """
        Given a Descriptor object, computes a dictionary representation describing the metadata schema with
        post-processing to removed
        ignored properties.

        :param descriptor: A Descriptor Object derived from a metadata schema JSON object.
        """
        self._schema_dictionary = self._get_schema_dictionary_with_ignored_fields_removed(
            descriptor.get_dictionary_representation_of_descriptor())

    def _get_schema_dictionary_with_ignored_fields_removed(self, dictionary_descriptor):
        """ Recursively removes all ignored properties in the given dictionary representation of a Descriptor which
        describes a metadata schema or a field within the metadata schema.

        :param dictionary_descriptor: A Descriptor in dictionary format from which to remove ignored properties.
        :return: The same dictionary representation of a Descriptor with all the properties listed in
                 self.ignored_properties removed.
        """

        in_post_processing_dictionary_descriptor = dictionary_descriptor

        for ignored_property in self.ignored_properties:
            if ignored_property in in_post_processing_dictionary_descriptor.keys():
                del in_post_processing_dictionary_descriptor[ignored_property]

        for key, value in in_post_processing_dictionary_descriptor.items():
            if isinstance(value, dict):
                post_processed_sub_dictionary_descriptor = \
                    self._get_schema_dictionary_with_ignored_fields_removed(
                        value)
                in_post_processing_dictionary_descriptor[key] = post_processed_sub_dictionary_descriptor

        return in_post_processing_dictionary_descriptor

    def get_map_of_paths_by_property_label(self, dictionary_descriptor):
        """
        Given a dictionary of Descriptor dictionaries by the module name for which they represent, returns a
        dictionary that maps the path via schema modules to each property that exists in the schemas. Each property
        is represented up to two times: once where the key is the user friendly name of the property and once as the
        path itself.

        :param dictionary_descriptor: A dictionary where each key is a module name and the value is a dictionary
        representation of its respective Descriptor onject.
        """

        label_map = {}

        for metadata_schema, metadata_schema_properties in dictionary_descriptor.items():
            self._add_paths_to_map(metadata_schema_properties, label_map, metadata_schema)
        return label_map

    def _add_paths_to_map(self, metadata_property_dictionary, current_label_map, path_so_far):
        for property_key, property_value in metadata_property_dictionary.items():
            # Only put values into the map that are not metadata about the schema itself and not about the uuid.
            if isinstance(property_value, dict) and property_key != "schema" and property_key != "uuid":
                fully_qualified_property_label = path_so_far + "." + property_key
                current_label_map = self._put_into_map(fully_qualified_property_label, fully_qualified_property_label,
                                                       current_label_map)
                if "user_friendly" in property_value.keys():
                    user_friendly_property_label = property_value["user_friendly"]
                    current_label_map = self._put_into_map(user_friendly_property_label, fully_qualified_property_label,
                                                           current_label_map)

                self._add_paths_to_map(property_value, current_label_map, fully_qualified_property_label)
        return current_label_map

    def get_tab_representation_of_schema(self):
        """
        Returns a dictionary representing the way the schema would look as part of a tab in a spreadsheet where each
        of its properties (including embedded properties) are all flattened to be column names.
        """

        tab_key = self.schema_descriptor.get_schema_module_name()
        tab_display_name = tab_key[0].upper() + tab_key[1:].replace("_", " ")
        return {tab_key: {"display_name": tab_display_name,
                          "columns": self._get_columns_names_for_metadata_schema(tab_key, self.schema_dictionary)}}

    def _get_columns_names_for_metadata_schema(self, root_schema_name, root_schema_dictionary):
        list_of_column_names = []
        for key, value in root_schema_dictionary.items():
            if isinstance(value, dict) and key != "schema":
                next_root_schema_name = root_schema_name + "." + key
                children_column_names = self._get_columns_names_for_metadata_schema(next_root_schema_name, value)
                if children_column_names:
                    list_of_column_names += children_column_names
                else:
                    list_of_column_names.append(next_root_schema_name)

        return list_of_column_names

    @staticmethod
    def _put_into_map(key, value, current_label_map):
        key = key.lower()
        value = value.lower()

        if key in current_label_map.keys():
            current_values = current_label_map[key]
            current_values.append(value)
            current_label_map[key] = current_values
        else:
            current_label_map[key] = [value]

        return current_label_map
