import time
from unittest import TestCase
from mock import MagicMock

from ingest.utils.token_manager import TokenManager, Token


class TestTokenManager(TestCase):
    def test_get_token(self):
        token_client = MagicMock()
        token_client.retrieve_token = MagicMock(return_value='token')
        token_manager = TokenManager(token_client=token_client)
        token = token_manager.get_token()
        self.assertTrue(token)
        self.assertEqual(token, 'token')

    def test_get_token_when_expired(self):
        token_client = MagicMock()
        token_client.retrieve_token = MagicMock(return_value='token_1')
        token_manager = TokenManager(token_client=token_client)
        expired_token = token_manager.get_token()
        token_manager.token.is_expired = MagicMock(return_value=True)
        token_client.retrieve_token = MagicMock(return_value='token_2')
        new_token = token_manager.get_token()
        self.assertEqual(new_token, 'token_2')

    def test_valid_token(self):
        token = Token(value='token',
                      token_duration=3600 * 1000,
                      refresh_period=60 * 20 * 1000)
        self.assertFalse(token.is_expired())

    def test_expired_token(self):
        token = Token(value='token',
                      token_duration=1000,
                      refresh_period=0)
        time.sleep(1)
        self.assertTrue(token.is_expired())

    def test_expired_token_outside_refresh_period(self):
        token = Token(value='token',
                      token_duration=4000,
                      refresh_period=1000)
        time.sleep(3.5)
        self.assertTrue(token.is_expired())

    def test_valid_token_within_refresh_period(self):
        token = Token(value='token',
                      token_duration=4000,
                      refresh_period=1000)
        time.sleep(2)
        self.assertFalse(token.is_expired())
