import json
import os
import sqlite3

from dbserver import via_dbserver


"""
Multiprocess queue backed by sqlite.
Since I don't want to rely on external software, postgres is out and so is its shiny
SKIP LOCKED stuff. This one is way less sophisticated:

- Each worker runs as a separate process:
    while True:
      job_id = get()
      # do stuff
      finish(job_id)

- The get() function "claims" a job by setting `pid` and `created_at` on its row.

- If a worker dies between get() and finish(), its job will eventually be unclaimed
  again by a janitor process that periodically runs release_jobs_from_dead_workers().
"""


def _get(conn: sqlite3.Connection, queue_name: str, pid):
    with conn:
        c = conn.cursor()
        c.execute(
            """
        SELECT id, payload
        FROM queue
        WHERE name = ?
          AND pid IS NULL
        ORDER BY created_at
        LIMIT 1;
        """,
            (queue_name,),
        )
        result = c.fetchone()
        if result is None:
            return None

        job_id, payload = result
        c.execute(
            "UPDATE queue SET pid = ?, started_at = datetime('now') WHERE id = ?;",
            (pid, job_id),
        )

    return job_id, json.loads(payload)


def _finish(conn, job_id):
    with conn:
        c = conn.cursor()
        c.execute("DELETE FROM queue WHERE id = ?;", (job_id,))
        assert c.rowcount == 1, c.rowcount


def _put_bulk(conn, queue_name, payloads):
    with conn:
        conn.executemany(
            """
        INSERT INTO queue (name, payload)
        VALUES (?, ?);
        """,
            ((queue_name, json.dumps(payload)) for payload in payloads),
        )


def _release_jobs_from_dead_workers(conn):
    c = conn.cursor()
    c.execute(
        """
    SELECT id, pid
    FROM queue
    WHERE datetime(started_at) < datetime('now', '-1 minute')
    AND pid IS NOT NULL;
    """
    )

    results = c.fetchall()
    orphaned_job_ids = tuple(job_id for job_id, pid in results if not _is_alive(pid))

    if not orphaned_job_ids:
        return 0

    with conn:
        c = conn.cursor()
        question_marks = ",".join("?" for _ in range(len(orphaned_job_ids)))
        c.execute(
            f"""
            UPDATE queue
            SET pid = NULL,
                started_at = NULL
            WHERE id IN ({question_marks});
            """,
            orphaned_job_ids,
        )
        return c.rowcount


def _is_alive(pid: int):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


get = via_dbserver(_get)
put_bulk = via_dbserver(_put_bulk)
release_jobs_from_dead_workers = via_dbserver(_release_jobs_from_dead_workers)
finish = via_dbserver(_finish)
