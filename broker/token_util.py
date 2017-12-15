import json

import requests


def get_token():
    response = requests.post('https://danielvaughan.eu.auth0.com/oauth/token',
                      data='{"client_id":"Zdsog4nDAnhQ99yiKwMQWAPc2qUDlR99","client_secret":"t-OAE-GQk_nZZtWn-QQezJxDsLXmU7VSzlAh9cKW5vb87i90qlXGTvVNAjfT9weF","audience":"http://localhost:8080","grant_type":"client_credentials"}',
                      headers={'Content-type': 'application/json'})
    data = json.loads(response.text)
    return data['access_token']
