import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import retry


def create_session_with_retry(retry_policy) -> Session:
    retry_policy = retry_policy or retry.Retry(
        total=100,
        # seems that this has a default value of 10,
        # setting this to a very high number so that it'll respect the status retry count

        status=17,
        # status is the no. of retries if response is in status_forcelist,
        # this count will retry for ~20 mins with back off timeout within

        read=10,
        status_forcelist=[500, 502, 503, 504, 409],
        backoff_factor=0.6,
        method_whitelist=frozenset(
            ['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE'])
    )
    session = Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_policy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
