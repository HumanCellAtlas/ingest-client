#!/usr/bin/env python
"""
Provides mock schemas for building intsances of schema_template.py
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "25/05/2018"

from ingest.template.schema_template import SchemaTemplate
from unittest.mock import patch, MagicMock


@patch('urllib.request.urlopen')
def get_template_for_json(mock_urlopen, data="{}"):
    cm = MagicMock()
    cm.getcode.return_value = 200
    cm.read.return_value = data.encode()
    cm.__enter__.return_value = cm
    mock_urlopen.return_value = cm

    return SchemaTemplate(list_of_schema_urls=['test_url'])