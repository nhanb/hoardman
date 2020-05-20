def migrate(cursor):
    cursor.execute(
        """\
CREATE TABLE fetch_result (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    url TEXT,
    status INT,
    body TEXT
);"""
    )

    cursor.execute(
        """\
CREATE TABLE queue (
    id integer primary key autoincrement,
    created_at timestamp default current_timestamp,
    name text,
    payload text, -- but actually should be json object
    started_at timestamp,
    pid int unique -- ensure only 1 task per process
);"""
    )
