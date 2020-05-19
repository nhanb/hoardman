import os

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
}

PASSWORD = os.environ["GCFPROXY_PASSWORD"]


def proxied_get(url, timeout=10):
    return requests.post(
        "https://asia-east2-nhansproxy.cloudfunctions.net/nhansproxy",
        json={
            "url": url,
            "method": "get",
            "body": None,
            "headers": HEADERS,
            "password": PASSWORD,
        },
        timeout=timeout,
    )
