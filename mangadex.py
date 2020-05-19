import time
from datetime import datetime as dt

import httpclient
import jobqueue


def fetch(conn, url):
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

    with conn:
        conn.execute(
            """
        insert into fetch_result (url, status, body) values (?, ?, ?);
        """,
            (url, resp.status_code, resp.text),
        )
    print(f"Inserted {url}")


def id_to_url(manga_id):
    return f"https://mangadex.org/title/{manga_id}/"


def already_fetched(cursor, manga_id: int):
    url = id_to_url(manga_id)
    cursor.execute("select 1 from fetch_result where status = 200 and url=?", (url,))
    result = cursor.fetchone()
    return result is not None


def put_fetch_jobs(conn, min_id, max_id):
    for chunk in chunks(range(min_id, max_id + 1), 10_000):
        payloads = ({"url": id_to_url(id_)} for id_ in chunk)
        jobqueue.put_bulk("fetch", payloads)


def fetch_worker(conn):
    while True:
        job = jobqueue.get("fetch")
        if job is not None:
            job_id, payload = job
            fetch(conn, payload["url"])
            jobqueue.finish(job_id)
        else:
            time.sleep(5)


def chunks(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


if __name__ == "__main__":
    from persistence import get_conn

    conn = get_conn()
    fetch_worker(conn)
