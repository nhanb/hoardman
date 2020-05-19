import os

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
}

PASSWORD = os.environ["GCFPROXY_PASSWORD"]
REGION_INDEX = int(os.environ["GCFPROXY_INDEX"])
REGIONS = [
    "asia-east2",
    "asia-northeast1",
    "europe-west1",
    "europe-west2",
    "us-central1",
    "us-east1",
    "us-east4",
]

PROXY = f"https://{REGIONS[REGION_INDEX]}-nhansproxy.cloudfunctions.net/nhansproxy"
print(PROXY)


def proxied_get(url, timeout=10):
    return requests.post(
        PROXY,
        json={
            "url": url,
            "method": "get",
            "body": None,
            "headers": HEADERS,
            "password": PASSWORD,
        },
        timeout=timeout,
    )
