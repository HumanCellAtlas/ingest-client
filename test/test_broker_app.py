from unittest import TestCase

from broker.broker_app import app

class BrokerAppTest(TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_authorization_failed(self):
        response = self.client.post('/upload')
        self.assertEqual(401, response.status_code)