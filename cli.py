#!/usr/bin/env python3
import sys

from jobqueue import release_jobs_from_dead_workers
from mangadex import fetch_worker, put_fetch_jobs
from persistence import get_conn, initdb


def main():
    assert len(sys.argv) == 2, "Please specifiy 1 action"

    conn = get_conn()
    action = sys.argv[1]

    if action == "worker":
        fetch_worker(conn)
    elif action == "initdb":
        initdb(conn)
    elif action == "putjobs":
        put_fetch_jobs(conn, 1, 48970)
    elif action == "janitor":
        release_jobs_from_dead_workers(conn)


main()
