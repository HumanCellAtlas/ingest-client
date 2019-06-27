from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import retry


def create_session_with_retry(retry_policy=None) -> Session:
    retry_policy = retry_policy or retry.Retry(
        total=50,
        # seems that this has a default value of 10,
        # setting this to a very high number so that it'll respect the status retry count

        status=10,
        # status is the no. of retries if response is in status_forcelist

        read=10,
        status_forcelist=[409],
        backoff_factor=0.6,
        method_whitelist=frozenset(
            ['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE'])
    )
    session = Session()
    adapter = HTTPAdapter(max_retries=retry_policy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
