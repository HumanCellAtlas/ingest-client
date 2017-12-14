from unittest import TestCase

from mock import patch

from broker.broker_app import app


class BrokerAppTest(TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_authorization_failed(self):
        # when:
        response = self.client.post('/upload')

        # then:
        self.assertEqual(401, response.status_code)

    @patch('broker.broker_app._save_file')
    def test_failed_save(self, save_file):
        # given:
        save_file.side_effect = Exception("I/O error")

        # when:
        response = self.client.post('/upload', headers={'Authorization': 'authorization'})

        # then:
        self.assertEqual(500, response.status_code)