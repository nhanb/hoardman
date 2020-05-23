#!/usr/bin/env python3
import sys


def main():
    assert len(sys.argv) == 2, "Please specifiy 1 action"

    action = sys.argv[1]

    if action == "worker":
        from mangadex import fetch_worker

        fetch_worker()
    elif action == "migrate":
        from persistence import migrate

        migrate()
    elif action == "putjobs":
        from mangadex import put_fetch_jobs

        put_fetch_jobs(1, 48970)
    elif action == "putapijobs":
        from mangadex import put_fetch_jobs, id_to_api_url

        put_fetch_jobs(1, 48970, id_to_api_url)
    elif action == "janitor":
        from jobqueue import release_jobs_from_dead_workers

        count = release_jobs_from_dead_workers()
        print("Released", count, "jobs")
    elif action == "dbserver":
        from dbserver import runserver

        runserver()
    else:
        print("Invalid option")
        exit(1)


main()
