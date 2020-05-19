#!/usr/bin/env python3
import sys

from persistence import get_conn, initdb


def main():
    assert len(sys.argv) == 2, "Please specifiy 1 action"

    conn = get_conn()
    action = sys.argv[1]

    if action == "worker":
        from mangadex import fetch_worker

        fetch_worker(conn)
    elif action == "initdb":
        initdb(conn)
    elif action == "putjobs":
        from mangadex import put_fetch_jobs

        put_fetch_jobs(conn, 1, 48970)
    elif action == "janitor":
        from jobqueue import release_jobs_from_dead_workers

        release_jobs_from_dead_workers()
    elif action == "jobserver":
        from jobqueue import jobserver

        jobserver()


main()
