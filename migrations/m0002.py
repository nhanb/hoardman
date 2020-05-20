def migrate(cursor):
    cursor.execute("CREATE INDEX idx_fetch_result_url ON fetch_result(url);")
