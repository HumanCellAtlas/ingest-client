import unittest
from collections import OrderedDict

from ingest.template.migration_parser import MigrationParser


class TestMigrationParser(unittest.TestCase):
    """ Unit tests for MigrationParser class. """

    def test__simple_migrations_to_dictionary__success(self):
        self.maxDiff = None
        property_migration_for_cell_suspension = {
            "source_schema": "cell_suspension",
            "property": "total_estimated_cells",
            "target_schema": "cell_suspension",
            "replaced_by": "estimated_cell_count",
            "effective_from": "9.0.0",
            "reason": "Field clarification",
            "type": "renamed property"
        }
        property_migration_for_library_preparation = {
            "source_schema": "library_preparation_protocol",
            "property": "library_construction_approach",
            "target_schema": "library_preparation_protocol",
            "replaced_by": "library_construction_method",
            "effective_from": "5.0.0",
            "reason": "Schema consistency update",
            "type": "renamed property"
        }
        sample_property_migration = [property_migration_for_cell_suspension, property_migration_for_library_preparation]

        migration_parser = MigrationParser(sample_property_migration)

        expected_migrations_parsed_dictionary = {
            "cell_suspension.total_estimated_cells": OrderedDict({"replaced_by": "cell_suspension.estimated_cell_count",
                                                                  "version": "9.0.0", "target_version": None}),
            "library_preparation_protocol.library_construction_approach": OrderedDict({
                "replaced_by": "library_preparation_protocol.library_construction_method", "version": "5.0.0",
                "target_version": None})}
        self.assertEqual(MigrationParser.get_dictionary_representation(migration_parser.migrations),
                         expected_migrations_parsed_dictionary)

    def test__chained_migrations_to_dictionary__success(self):
        property_migration_for_cell_suspension = {
            "source_schema": "cell_suspension",
            "property": "total_estimated_cells",
            "target_schema": "cell_suspension",
            "replaced_by": "estimated_cell_count",
            "effective_from": "9.0.0",
            "reason": "Field clarification",
            "type": "renamed property"
        }
        property_migration_for_cell_suspension_2 = {
            "source_schema": "cell_suspension",
            "property": "estimated_cell_count",
            "target_schema": "cell_suspension",
            "replaced_by": "estimated_cells_count",
            "effective_from": "10.0.0",
            "reason": "For fun",
            "type": "renamed property"
        }
        sample_property_migration = [property_migration_for_cell_suspension, property_migration_for_cell_suspension_2]

        migration_parser = MigrationParser(sample_property_migration)

        expected_migrations_parsed_dictionary = {
            "cell_suspension.total_estimated_cells": OrderedDict(
                {"replaced_by": "cell_suspension.estimated_cell_count", "version": "9.0.0", "target_version": None}),
            "cell_suspension.estimated_cell_count": OrderedDict(
                {"replaced_by": "cell_suspension.estimated_cells_count", "version": "10.0.0", "target_version": None})}
        self.assertEqual(MigrationParser.get_dictionary_representation(migration_parser.migrations),
                         expected_migrations_parsed_dictionary)
