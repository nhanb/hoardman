import os
import time
from datetime import datetime as dt

import httpclient
import jobqueue
from dbserver import via_dbserver


def _insert_fetch_result(conn, url, status, body):
    with conn:
        conn.execute(
            """
        insert into fetch_result (url, status, body) values (?, ?, ?);
        """,
            (url, status, body),
        )


insert_fetch_result = via_dbserver(_insert_fetch_result)


def fetch(url):
    status_code = -1
    total_start = dt.now()
    while status_code != 200:
        start = dt.now()
        resp = httpclient.proxied_get(url, timeout=100)
        duration = (dt.now() - start).total_seconds()
        print(f"Request time: {duration}s")
        status_code = resp.status_code
    assert resp.status_code == 200, f"{url} failed: {resp.status_code}"
    total_duration = (dt.now() - total_start).total_seconds()
    print(f"Request time (total): {total_duration}s")

    insert_fetch_result(url, resp.status_code, resp.text)
    print(f"Inserted {url}")


def id_to_url(manga_id):
    return f"https://mangadex.org/title/{manga_id}/"


def put_fetch_jobs(min_id, max_id):
    payloads = [{"url": id_to_url(id_)} for id_ in range(min_id, max_id + 1)]
    jobqueue.put_bulk("fetch", payloads)


def fetch_worker():
    pid = os.getpid()
    while True:
        job = jobqueue.get("fetch", pid)
        if job is not None:
            job_id, payload = job
            print("Started job", job_id, payload)
            fetch(payload["url"])
            jobqueue.finish(job_id)
            print(f"Finished job {job_id}")
        else:
            print("No pending job found.")
            time.sleep(5)


if __name__ == "__main__":
    fetch_worker()
