#!/usr/bin/env python
"""
Provides mock schemas for building instances of schema_template.py
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "25/05/2018"

import json
import os
from unittest.mock import patch, MagicMock

from ingest.template.schema_template import SchemaTemplate


@patch('urllib.request.urlopen')
def get_template_for_json(mock_urlopen, data="{}"):
    cm = MagicMock()
    cm.getcode.return_value = 200
    cm.read.return_value = data.encode()
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm

    dn = os.path.dirname(os.path.realpath(__file__))
    with open(dn + '/property_migrations.json') as json_file:
        migrations = json.load(json_file)

    return SchemaTemplate(metadata_schema_urls=['test_url'], property_migrations=migrations["migrations"])
