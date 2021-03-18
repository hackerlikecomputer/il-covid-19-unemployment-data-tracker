import time
import requests
import warnings


def get_with_retry(url, headers, max_tries=5):
    """send a GET request to a url, retrying if failed

    Args:
        url (str): url to get
        headers (dict): headers to pass to request
        max_tries (int, optional): Max times to retry before failing. Defaults to 5.

    Raises:
        requests.RequestException: If request failed

    Returns:
        requests.Response: http response object
    """

    tries = 0
    while tries < max_tries:
        try:
            resp = requests.get(url, headers=headers)
            assert resp.status_code == 200
        except Exception:
            warnings.warn(
                f"request to url {url} failed. retrying [{tries + 1}/max_tries"
            )
            time.sleep(300)
        else:
            return resp
    else:
        raise requests.RequestException(f"request to {url} failed")