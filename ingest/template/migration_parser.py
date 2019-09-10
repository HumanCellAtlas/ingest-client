from collections import namedtuple


class MigrationParser():
    """
    MigrationParser reads a json formatted list of migrations from a property_migrations file (parsed by the
    SchemaTemplate class) and captures the information in a dictionary structure.
    """

    def __init__(self, json_migrations):
        self.migrations = json_migrations

    @property
    def migrations(self):
        return self._migrations

    @migrations.setter
    def migrations(self, json_migrations):
        """
        Iterate through each of the dictionaries in json_migrations which represent a single change for a single
        property that occurred between two versions of a schema and capture the information in a dictionary structure.

        :param json_migrations: A list of dictionaries where each dictionary is a single change made to a single
                                property in a single schema version bump.
        """
        migrations = {}
        for property_migration in json_migrations:
            source_migrated_property = property_migration["source_schema"] + "." + property_migration["property"]

            if source_migrated_property in migrations:
                raise Exception(
                    f"ERROR: Property {source_migrated_property} already has migration information. Duplicates not "
                    f"expected!")

            migrations[source_migrated_property] = self._get_migration_info_of_migrated_property(property_migration)
        self._migrations = migrations

    @staticmethod
    def get_dictionary_representation(migrations):
        """
        Returns a nested dictionary representation of the given migration structure (that contains a nested
        MigrationInfo object).
        """

        return {migrated_property_name: migrated_property_info._asdict() for
                migrated_property_name, migrated_property_info in migrations.items()}

    @staticmethod
    def _get_migration_info_of_migrated_property(property_migration):
        """
        Derive the necessary migration information from the property migration JSON object (formatted as a dictionary)
        and return it as a namedtuple MigrationInfo.
        """

        if "target_schema" in property_migration and "replaced_by" in property_migration:
            _replaced_by = property_migration["target_schema"] + "." + property_migration[
                "replaced_by"]
        else:
            _replaced_by = None
        if "effective_from" in property_migration:
            _version = property_migration["effective_from"]
            _target_version = None
        elif "effective_from_source" in property_migration:
            _version = property_migration["effective_from_source"]
            _target_version = property_migration["effective_from_target"]

        MigrationInfo = namedtuple("MigrationInfo", "replaced_by version target_version")
        return MigrationInfo(replaced_by=_replaced_by, version=_version, target_version=_target_version)
