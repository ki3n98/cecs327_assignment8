import psycopg2


class Database:
    def __init__(self, database_url):
        self._url = database_url
        self._conn = psycopg2.connect(self._url)

    def execute(self, query: str, params=None):
        cur = None
        try:
            cur = self._conn.cursor()
            cur.execute(query, params)
            if cur.description is not None:
                return cur.fetchall()
            self._conn.commit()
            return cur.rowcount
        except psycopg2.Error as e:
            self._conn.rollback()
            print(e)
            return None
        finally:
            if cur is not None:
                cur.close()

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
