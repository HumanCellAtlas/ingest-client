import os

import requests
from requests.adapters import HTTPAdapter

env_max_retries = os.environ.get('REQUESTS_MAX_RETRIES')
DEFAULT_MAX_RETRIES = int(env_max_retries) if env_max_retries else 3


def optimistic_session(url_prefix, max_retries=DEFAULT_MAX_RETRIES):
    session = requests.Session()
    retry_adapter = HTTPAdapter(max_retries=max_retries)
    session.mount(url_prefix, retry_adapter)
    return session
