import requests
from requests.adapters import HTTPAdapter, Retry


def init_session(max_retries, backoff_factor):
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount("https://api.openfigi.com", HTTPAdapter(max_retries=retries))
    return session
