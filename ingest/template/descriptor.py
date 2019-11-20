#!/usr/bin/env python
"""
Class encapsulating implementation details on the Descriptor classes. Descriptors represent a portion of a metadata
schema.
"""

import re

IDENTIFIABLE_PROPERTIES = ["biomaterial_id", "process_id", "protocol_id", "file_name"]


class Descriptor():
    """ Parent class type. A Descriptor type encapsulate a small isolated amount of information about a portion of a
    metadata schema.
    """

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a dict representing the Descriptor object. """
        raise NotImplementedError("Subclasses of Descriptor are required to override this method.")


class SchemaTypeDescriptor(Descriptor):
    """ Descriptor encapsulating "metadata" information about a single metadata schema file. """

    def __init__(self, metadata_schema_url):
        url_validation_regex = re.compile(
            r'^http[s]?://(?P<location>([^/]+/)*[^/]+)/' +
            r'(?P<high_level_entity>(type)|(module)|(core)|(system))/' +
            r'((?P<domain_entity>([^/]+/)*[^/]+)/)?' +
            r'(?P<version>(?P<version_number>(?P<major>\d+)(\.(?P<minor>\d+))?(\.(?P<rev>\d+))?)|(?P<latest>latest))/' +
            r'(?P<module>.*)$'
        )
        if not url_validation_regex.match(metadata_schema_url):
            raise Exception(
                f"ERROR: The metadata schema URL passed in for parsing {metadata_schema_url} does not conform to "
                f"expected format.")

        self.high_level_entity = url_validation_regex.match(metadata_schema_url).group("high_level_entity")
        self.domain_entity = url_validation_regex.match(metadata_schema_url).group("domain_entity")
        self.module = url_validation_regex.match(metadata_schema_url).group("module")
        self.version = url_validation_regex.match(metadata_schema_url).group("version")
        self.url = metadata_schema_url

    def get_module(self):
        return self.module

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a dictionary representation of the current schema descriptor object. """
        return self.__dict__


class SimplePropertyDescriptor(Descriptor):
    """ A Descriptor encapsulating information about a simple property of a metadata schema. A simple property is
    designated as having no children properties which arises when the property is associated with its own metadata
    schema.
    """

    def __init__(self, json_data):
        """ Initialize the simply property descriptor using the top level fields in given json data. """
        self.value_type = json_data.get("type")
        self.multivalue = False

        if self.value_type == "array":
            self.multivalue = True
            # Get the type of elements in the array which is nested inside the "items" key.
            self.value_type = json_data["items"]["type"]

        self.format = json_data.get("format")
        self.user_friendly = json_data.get("user_friendly")
        self.description = json_data.get("description")
        self.example = json_data.get("example")
        self.guidelines = json_data.get("guidelines")

        # For now, required, external_reference and identifiable are set to false because the value of these properties
        # exist in the parent metadata schema and not in the property description itself. They will be back-populated
        # later.
        self.required = False
        self.identifiable = False
        self.external_reference = False

    def get_dictionary_representation_of_descriptor(self):
        """ Only include information in the class where the value is not None or empty OR if the value is a boolean
        since in that case, both True and False are valid values."""

        return dict((key, value) for (key, value) in self.__dict__.items() if value or isinstance(value, bool))


class ComplexPropertyDescriptor(SimplePropertyDescriptor, Descriptor):
    """ A Descriptor encapsulating information about a complex property of a metadata schema. A complex property
    means that there exists an entire metadata schema to describe the property itself and usually contains children
    properties."""

    def __init__(self, json_data):
        super().__init__(json_data)

        # Populate metadata/information about the schema itself, derived from the URL
        if "$id" in json_data.keys():
            self.schema = SchemaTypeDescriptor(json_data["$id"])
        elif "id" in json_data.keys():
            self.schema = SchemaTypeDescriptor(json_data["id"])
        else:
            self.schema = None

        # Add required fields
        self.required_properties = json_data.get("required")

        # Add children properties
        self.children_properties = {}
        if "properties" in json_data.keys():
            for property_name, property_values in json_data["properties"].items():
                if "$schema" in property_values or "schema" in property_values:
                    child_property_descriptor = ComplexPropertyDescriptor(property_values)
                elif "items" in property_values and ("$schema" in property_values["items"] or "schema" in property_values["items"]):
                    child_property_descriptor = ComplexPropertyDescriptor(property_values["items"])
                    child_property_descriptor.multivalue = True
                else:
                    child_property_descriptor = SimplePropertyDescriptor(property_values)

                # Make it required if the child property name is in the list of required properties
                if self.required_properties and property_name in self.required_properties:
                    child_property_descriptor.required = True

                # Make the property identifiable if the child property name is one of the listed hardcoded
                # identifiable properties
                if property_name in IDENTIFIABLE_PROPERTIES:
                    child_property_descriptor.identifiable = True

                self.children_properties[property_name] = child_property_descriptor

    def get_schema_module_name(self):
        return self.schema.get_module()

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a representation of the class as a dictionary with the following caveats:
        1) If the value of a key is None or empty but is NOT a boolean, then the attribute it omitted from the
        dictionary.
        2) If the value is of a SchemaTypeDescriptor type, convert it to a dictionary.
        3) Any child descriptors are flattened from being a list to simply added attributes where the key is the
        metadata schema name and the dictionary is the corresponding descriptor.
        """

        dictionary_representation = {}

        for (key, value) in self.__dict__.items():
            if key == "children_properties":
                for child_key, child_value in value.items():
                    self.add_key_value_to_dictionary_if_valid(child_key, child_value, dictionary_representation)
            else:
                self.add_key_value_to_dictionary_if_valid(key, value, dictionary_representation)

        return dictionary_representation

    @staticmethod
    def add_key_value_to_dictionary_if_valid(key, value, dictionary):
        if not value and not isinstance(value, bool):
            return
        if issubclass(type(value), Descriptor):
            dictionary[key] = value.get_dictionary_representation_of_descriptor()
        else:
            dictionary[key] = value
