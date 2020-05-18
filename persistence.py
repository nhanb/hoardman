import sqlite3

DBNAME = "db.sqlite3"


def get_conn():
    return sqlite3.connect(DBNAME)


def initdb(conn: sqlite3.Connection):
    with open("tables.sql", "r") as sqlfile:
        sql = sqlfile.read()
    with conn:
        conn.executescript(sql)


if __name__ == "__main__":
    initdb(get_conn())
    print("Created", DBNAME)
