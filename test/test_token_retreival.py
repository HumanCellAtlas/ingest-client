from unittest import TestCase

import broker.token_util as token_util


class TestTokenReteival(TestCase):

    def test_retreives_token(self):
        token = token_util.get_token()
        print(token)
