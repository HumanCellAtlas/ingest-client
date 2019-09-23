import time
from unittest import TestCase

from mock import patch

from ingest.api.dssapi import DssApi


class DSSApiTest(TestCase):
    @patch('ingest.api.dssapi.hca.dss.DSSClient')
    def test_init_dss_client_not_expired(self, mock_dss_client):
        dss = DssApi()
        self.assertFalse(dss.token.is_expired())

    @patch('ingest.api.dssapi.hca.dss.DSSClient')
    def test_init_dss_client_expired(self, mock_dss_client):
        dss = DssApi()
        dss.token = None
        dss.init_dss_client(duration=100, refresh_period=0)
        time.sleep(0.1)
        self.assertTrue(dss.token.is_expired())

    @patch('ingest.api.dssapi.hca.dss.DSSClient')
    def test_init_dss_client_expired_and_outside_refresh_period(self, mock_dss_client):
        dss = DssApi()
        dss.token = None
        dss.init_dss_client(duration=500, refresh_period=100)
        time.sleep(0.4)
        self.assertTrue(dss.token.is_expired())
